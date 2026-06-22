from flask import Flask, render_template, request, jsonify
from analysis.screener import scan_top_stocks, scan_supply_leaders, scan_surge_stocks, scan_ma_bounce_stocks, scan_osc_stocks, scan_buy_candidates, scan_surge_buy_candidates, scan_pre_surge, get_scan_progress
from analysis.export_growth import load_cache as load_export_cache, scan_export_growth, is_new_update
from analysis.data_fetcher import get_ticker, get_ohlcv, get_investor_detail, get_supply_zone, is_main_stock, get_today_price, append_today
from analysis.cache_manager import load_stock_cache, build_all_cache, is_build_needed, get_build_status
from analysis.indicators import calc_indicators, get_ma_arrangement, get_latest_signals
from analysis.fundamental import get_fundamental
from analysis.news import search_naver_news, analyze_news, get_research_reports
from analysis.signal import calc_score, get_recommendation, get_ai_analysis, get_business_description, summarize_research
from analysis.charts import make_main_chart, make_supply_zone_chart, make_investor_chart, make_ma_chart
from analysis.patterns import detect_patterns, simplify_for_template as simplify_patterns
from analysis.dart import get_disclosures, get_company_info
from analysis.fundamental import get_market_profile
from dotenv import load_dotenv
import os
import json
import threading
import pandas as pd
from datetime import datetime, timedelta

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=True)
app = Flask(__name__)


def _get_report_target_prices(reports):
    """개별 리포트 페이지에서 목표주가 병렬 수집 — '목표가300,000|투자의견Buy' 패턴"""
    import requests as _req
    import re as _re
    from bs4 import BeautifulSoup
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_one(url):
        try:
            res = _req.get(url, headers={'User-Agent': 'Mozilla/5.0',
                           'Referer': 'https://finance.naver.com/'}, timeout=6)
            soup = BeautifulSoup(res.text, 'html.parser')
            for td in soup.find_all('td'):
                text = td.get_text(strip=True)
                m = _re.search(r'목표가([\d,]+)', text)
                if m:
                    return int(m.group(1).replace(',', ''))
        except Exception:
            pass
        return None

    prices = []
    urls = [r['url'] for r in reports if r.get('url')][:6]
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(_fetch_one, u) for u in urls]
        for f in as_completed(futures):
            v = f.result()
            if v and v > 1000:
                prices.append(v)
    return prices

_TICKER_PATH      = os.path.join(os.path.dirname(__file__), 'data', 'krx_tickers.json')
_LAST_UPDATE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'ticker_last_update.txt')


def _auto_update_tickers():
    """매주 토요일 새벽 2시(KST) 종목 DB 자동 갱신 (스캔용 800 + 전종목 4000+)"""
    import time as _time
    while True:
        # KST = UTC+9
        now_kst = datetime.utcnow() + timedelta(hours=9)
        # 토요일(weekday==5), 02:00~02:59 사이
        if now_kst.weekday() == 5 and now_kst.hour == 2:
            this_week = now_kst.strftime('%Y-W%W')
            last = ''
            if os.path.exists(_LAST_UPDATE_PATH):
                with open(_LAST_UPDATE_PATH) as f:
                    last = f.read().strip()
            if last != this_week:
                try:
                    from update_tickers import update
                    update()
                    with open(_LAST_UPDATE_PATH, 'w') as f:
                        f.write(this_week)
                    print(f'[{this_week}] 종목 DB 자동 갱신 완료')
                except Exception as e:
                    print(f'종목 DB 갱신 오류: {e}')
            # 이번 시간대 처리 완료 — 70분 대기 후 다음 루프
            _time.sleep(4200)
        else:
            # 다음 토요일 02시까지 남은 초 계산
            days_until_sat = (5 - now_kst.weekday()) % 7
            if days_until_sat == 0 and now_kst.hour >= 3:
                days_until_sat = 7
            next_sat = (now_kst + timedelta(days=days_until_sat)).replace(
                hour=2, minute=0, second=0, microsecond=0)
            sleep_sec = max(60, (next_sat - now_kst).total_seconds())
            _time.sleep(min(sleep_sec, 3600))  # 최대 1시간 단위로 재확인


threading.Thread(target=_auto_update_tickers, daemon=True).start()


_DATA_DIR = '/data' if os.path.isdir('/data') else os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(_DATA_DIR, exist_ok=True)

_SURGE_CACHE_PATH          = os.path.join(_DATA_DIR, 'surge_cache.json')
_OSC_CACHE_PATH            = os.path.join(_DATA_DIR, 'osc_cache.json')
_RECOMMEND_CACHE_PATH      = os.path.join(_DATA_DIR, 'recommend_cache.json')
_SUPPLY_CACHE_PATH         = os.path.join(_DATA_DIR, 'supply_cache.json')
_BUY_CANDIDATE_CACHE_PATH  = os.path.join(_DATA_DIR, 'buy_candidate_cache.json')
_SURGE_BUY_CACHE_PATH      = os.path.join(_DATA_DIR, 'surge_buy_cache.json')
_PRE_SURGE_CACHE_PATH      = os.path.join(_DATA_DIR, 'pre_surge_cache.json')

# 종목 DB — 볼륨 경로로 재정의 (line 56의 로컬 경로 override)
_TICKER_PATH      = os.path.join(_DATA_DIR, 'krx_tickers.json')
_LAST_UPDATE_PATH = os.path.join(_DATA_DIR, 'ticker_last_update.txt')
_ALL_TICKER_PATH  = os.path.join(_DATA_DIR, 'krx_all_tickers.json')

# 최초 배포 시: 번들 파일이 볼륨에 없으면 복사
_LOCAL_DATA = os.path.join(os.path.dirname(__file__), 'data')
for _fname in ('krx_tickers.json', 'krx_all_tickers.json'):
    _vol = os.path.join(_DATA_DIR, _fname)
    _loc = os.path.join(_LOCAL_DATA, _fname)
    if not os.path.exists(_vol) and os.path.exists(_loc):
        import shutil as _shutil
        _shutil.copy2(_loc, _vol)

def _load_surge_cache():
    if not os.path.exists(_SURGE_CACHE_PATH):
        return None
    try:
        with open(_SURGE_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def _load_pre_surge_cache():
    if not os.path.exists(_PRE_SURGE_CACHE_PATH):
        return None
    try:
        with open(_PRE_SURGE_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def _run_surge_scan():
    """MA 반등 종목 + 급등 종목 스캔 후 surge_cache 저장 (pick_rec/sup는 각자 스케줄러 캐시 활용)"""
    global _surge_scanning
    _surge_scanning = True
    _status_set('surge', 'running')
    today = datetime.today().strftime('%Y-%m-%d')
    scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
    try:
        bounce = scan_ma_bounce_stocks(top_n=20)
        try:
            surge = scan_surge_stocks(top_n=10)
        except Exception:
            surge = []

        # pick_rec / pick_sup 는 별도 스케줄러 캐시에서 읽기 (중복 스캔 방지)
        try:
            rec_cache = _load_recommend_cache()
            rec_list = rec_cache.get('results', []) if rec_cache else []
            pick_rec = rec_list[0] if rec_list else None
        except Exception:
            pick_rec = None
        try:
            exp_cache = load_export_cache()
            exp_list = exp_cache.get('results', []) if exp_cache else []
            pick_exp = {'name': exp_list[0]['name'], 'ticker': exp_list[0]['ticker']} if exp_list else None
        except Exception:
            pick_exp = None

        with open(_SURGE_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({
                'date':       today,
                'scanned_at': scanned_at,
                'bounce':     bounce,
                'results':    surge,
                'pick_rec':   pick_rec,
                'pick_sup':   None,
                'pick_exp':   pick_exp,
            }, f, ensure_ascii=False)
        print(f'[{scanned_at}] 반등/급등 스캔 완료 — 반등:{len(bounce)}건, 급등:{len(surge)}건')
        _status_set('surge', 'done')
    except Exception as e:
        print(f'반등/급등 스캔 오류: {e}')
        _status_set('surge', 'done')
    finally:
        _surge_scanning = False


def _evening_scheduler():
    """매일 19:00 UTC(=04:00 KST) 반등/급등 스캔 — 캐시 없으면 시작 시 1회 즉시 스캔"""
    import time as _time
    _sc = _load_surge_cache()
    if (not _sc or (not _sc.get('bounce') and not _sc.get('results'))) and not is_build_needed():
        threading.Thread(target=_run_surge_scan, daemon=True).start()

    while True:
        now = datetime.today()
        next_run = now.replace(hour=19, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        _time.sleep((next_run - now).total_seconds())
        _run_surge_scan()


def _market_osc_scheduler():
    """평일 11:00 / 13:00 과매도·과매수 스캔
    - 11:00 결과 → 13:00까지 표시
    - 13:00 결과 → 다음날 11:00까지 표시
    - 앱 시작 시 자동 스캔 없음 (캐시 그대로 표시)
    """
    import time as _time
    if _load_osc_cache() is None:
        threading.Thread(target=_run_osc_scan, daemon=True).start()

    while True:
        now = datetime.today()
        # 다음 실행 시각 계산 (11:00 또는 13:00)
        candidates = []
        for h in (11, 13):
            t = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if t > now:
                candidates.append(t)
        if candidates:
            next_run = min(candidates)
        else:
            # 오늘 13:00 이후면 다음날 11:00
            next_run = (now + timedelta(days=1)).replace(
                hour=11, minute=0, second=0, microsecond=0)
        # 주말이면 다음 월요일 11:00으로
        while next_run.weekday() >= 5:
            next_run += timedelta(days=1)
            next_run = next_run.replace(hour=11, minute=0, second=0, microsecond=0)
        _time.sleep((next_run - now).total_seconds())
        if datetime.today().weekday() < 5:
            _run_osc_scan()


def _load_osc_cache():
    if not os.path.exists(_OSC_CACHE_PATH):
        return None
    try:
        with open(_OSC_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


_osc_scanning             = False
_surge_scanning           = False
_buy_candidate_scanning   = False
_surge_buy_scanning       = False

# ── 전체 스캔 진행 상태 ──────────────────────────────────────────
_SCAN_LABELS = {
    'recommend':  '추천 종목 TOP20',
    'supply':     '수급 주도 종목',
    'osc':        '과매도/과매수',
    'surge':      'MA반등/급등',
    'buy':        '매수후보(단기)',
    'surge_buy':  '급등주 매수후보',
    'pre_surge':  '급등 선취 후보',
}
_scan_status: dict = {}
_scan_status_lock = threading.Lock()

def _status_set(key: str, state: str):
    with _scan_status_lock:
        _scan_status[key] = state

def _build_progress_response():
    prog = get_scan_progress()
    with _scan_status_lock:
        status = dict(_scan_status)
    items = []
    for key, label in _SCAN_LABELS.items():
        st  = status.get(key, 'idle')
        p   = prog.get(key, {})
        cur = p.get('current', 0)
        tot = p.get('total', 0)
        nm  = p.get('name', '')
        pct = round(cur / tot * 100) if tot > 0 else (100 if st == 'done' else 0)
        items.append({'key': key, 'label': label, 'status': st,
                      'current': cur, 'total': tot, 'name': nm, 'pct': pct})
    any_running = any(s == 'running' for s in status.values())
    all_done    = bool(status) and all(s == 'done' for s in status.values())
    return {'items': items, 'running': any_running, 'done': all_done}

def _run_osc_scan():
    """과매도/과매수 스캔 실행 후 캐시 저장"""
    global _osc_scanning
    _osc_scanning = True
    _status_set('osc', 'running')
    try:
        result = scan_osc_stocks(top_n=30)
        now = datetime.today().strftime('%Y-%m-%d %H:%M')
        with open(_OSC_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'updated_at': now, **result}, f, ensure_ascii=False)
        print(f'[{now}] 과매도/과매수 스캔 완료 — 과매도:{len(result["oversold"])} 과매수:{len(result["overbought"])}')
        _status_set('osc', 'done')
    except Exception as e:
        print(f'과매도/과매수 스캔 오류: {e}')
        _status_set('osc', 'done')
    finally:
        _osc_scanning = False


threading.Thread(target=_evening_scheduler, daemon=True).start()
threading.Thread(target=_market_osc_scheduler, daemon=True).start()

_EXPORT_SCAN_DATE_PATH = os.path.join(_DATA_DIR, 'export_scan_date.txt')

def _run_export_scan():
    """수출주 스캔 실행 후 완료 날짜 기록"""
    today_str = datetime.today().strftime('%Y-%m-%d')
    try:
        scan_export_growth(growth_threshold=10)
        with open(_EXPORT_SCAN_DATE_PATH, 'w') as f:
            f.write(today_str)
        print(f'[{today_str}] 수출주 자동 스캔 완료')
    except Exception as e:
        print(f'수출주 스캔 오류: {e}')

def _export_scan_scheduler():
    """3일마다 자정(00:00) 수출주 자동 스캔"""
    import time as _time
    # 캐시 없으면 시작 시 즉시 실행
    if load_export_cache() is None:
        threading.Thread(target=_run_export_scan, daemon=True).start()

    while True:
        now = datetime.today()
        # 오늘 자정 00:00 이후면 내일 자정, 아니면 오늘 자정
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now >= midnight:
            midnight += timedelta(days=1)
        _time.sleep((midnight - now).total_seconds())
        # 자정 도달 — 3일 주기 체크
        last_date = None
        if os.path.exists(_EXPORT_SCAN_DATE_PATH):
            with open(_EXPORT_SCAN_DATE_PATH) as f:
                last_date = f.read().strip()
        today_str = datetime.today().strftime('%Y-%m-%d')
        if last_date is None or (datetime.today() - datetime.strptime(last_date, '%Y-%m-%d')).days >= 3:
            _run_export_scan()

threading.Thread(target=_export_scan_scheduler, daemon=True).start()


def _auto_build_cache():
    """매일 캐시가 없으면 350종목 전체 데이터 사전 수집.
    빌드 여부와 무관하게, 스캔 결과가 비어 있으면 스캔을 트리거한다 —
    재빌드 직후 컨테이너가 재시작되면 '빌드 완료'로 보고 스캔이 누락되는 문제 방지."""
    import time as _t
    _t.sleep(8)  # 모듈 전체 로드 완료 보장 (_run_*_scan 정의 이후 실행)
    if is_build_needed():
        today = datetime.today().strftime('%Y-%m-%d')
        print(f'[{today}] 전체 종목 캐시 빌드 시작...')
        try:
            count, errors = build_all_cache(max_workers=6)
            print(f'[{today}] 캐시 완료: {count}건 성공, {errors}건 실패')
        except Exception as e:
            print(f'캐시 빌드 오류: {e}')
            return
    # 결과 캐시가 비었으면(또는 0건) 스캔 트리거 — 빌드를 새로 했든 안 했든 항상 점검
    try:
        surge = _load_surge_cache()
        if not surge or (not surge.get('bounce') and not surge.get('results')):
            threading.Thread(target=_run_surge_scan, daemon=True).start()
        buy = _load_buy_candidate_cache()
        if not buy or not buy.get('results'):
            threading.Thread(target=_run_buy_candidate_scan, daemon=True).start()
        surge_buy = _load_surge_buy_cache()
        if not surge_buy or not surge_buy.get('results'):
            threading.Thread(target=_run_surge_buy_scan, daemon=True).start()
        pre_surge = _load_pre_surge_cache()
        if not pre_surge or not pre_surge.get('results'):
            threading.Thread(target=_run_pre_surge_scan, daemon=True).start()
    except Exception as e:
        print(f'스캔 트리거 오류: {e}')

threading.Thread(target=_auto_build_cache, daemon=True).start()


# ── 캐시 클린 리셋 공통 함수 ──────────────────────────────────────
def _clean_cache_files():
    """캐시 pkl·빌드플래그·스캔결과 JSON을 전부 삭제 (클린 리셋의 공통 동작).
    일요일 정기 리셋, 헬스체크 자동복구, 수동 rebuild API가 모두 이 함수를 사용."""
    import glob as _glob
    # 빌드 플래그 삭제
    _flag = os.path.join(_DATA_DIR, 'cache_built.txt')
    try:
        if os.path.exists(_flag):
            os.remove(_flag)
    except Exception:
        pass
    # pkl 캐시 삭제
    for _f in _glob.glob(os.path.join(_DATA_DIR, 'cache', '*.pkl')):
        try:
            os.remove(_f)
        except Exception:
            pass
    # 스캔 결과 JSON 삭제
    for _json in ['surge_cache.json', 'buy_candidate_cache.json',
                  'surge_buy_cache.json', 'pre_surge_cache.json',
                  'recommend_cache.json']:
        _jp = os.path.join(_DATA_DIR, _json)
        try:
            if os.path.exists(_jp):
                os.remove(_jp)
        except Exception:
            pass


# ── 매주 일요일 새벽 전체 캐시 클린 리셋 (안전장치) ─────────────────
_SUNDAY_RESET_PATH = os.path.join(_DATA_DIR, 'sunday_reset.txt')


def _sunday_cache_reset():
    """매주 일요일 새벽 3시(KST) 캐시(pkl·플래그·스캔결과)를 전부 비우고 재빌드"""
    import time as _time
    while True:
        now_kst = datetime.utcnow() + timedelta(hours=9)
        # 일요일(weekday==6), 03:00~03:59 사이
        if now_kst.weekday() == 6 and now_kst.hour == 3:
            this_week = now_kst.strftime('%Y-W%W')
            last = ''
            if os.path.exists(_SUNDAY_RESET_PATH):
                with open(_SUNDAY_RESET_PATH) as f:
                    last = f.read().strip()
            if last != this_week:
                try:
                    _clean_cache_files()
                    with open(_SUNDAY_RESET_PATH, 'w') as f:
                        f.write(this_week)
                    print(f'[{this_week}] 일요일 캐시 클린 리셋 — 재빌드 시작')
                    _auto_build_cache()  # 플래그가 없어졌으므로 전체 재빌드 + 스캔 트리거
                except Exception as e:
                    print(f'일요일 캐시 리셋 오류: {e}')
            # 이번 시간대 처리 완료 — 70분 대기
            _time.sleep(4200)
        else:
            # 다음 일요일 03시까지 남은 초 계산
            days_until_sun = (6 - now_kst.weekday()) % 7
            if days_until_sun == 0 and now_kst.hour >= 4:
                days_until_sun = 7
            next_sun = (now_kst + timedelta(days=days_until_sun)).replace(
                hour=3, minute=0, second=0, microsecond=0)
            sleep_sec = max(60, (next_sun - now_kst).total_seconds())
            _time.sleep(min(sleep_sec, 3600))  # 최대 1시간 단위로 재확인


threading.Thread(target=_sunday_cache_reset, daemon=True).start()


# ── 매일 데이터 건강검진 + 자동 복구 (헬스체크) ────────────────────
_HEALTH_RETRY_PATH = os.path.join(_DATA_DIR, 'health_retry.txt')


def _cache_health_check():
    """매일 평일 오전 11시(KST), 수급 의존 스캔 3종(선취·급등주매수·매수후보)이
    모두 0건이면 '데이터 불량' 신호로 보고 그날 1회만 자동 클린 리셋+재빌드한다.
    정상인 날에는 아무 작업도 하지 않는다 (불필요한 리셋·빈 화면 방지)."""
    import time as _time
    while True:
        now_kst = datetime.utcnow() + timedelta(hours=9)
        today = now_kst.strftime('%Y-%m-%d')
        # 평일(월~금) 오전 11시 — 아침 빌드+스캔이 끝났을 시간
        if now_kst.weekday() < 5 and now_kst.hour == 11:
            last = ''
            if os.path.exists(_HEALTH_RETRY_PATH):
                with open(_HEALTH_RETRY_PATH) as f:
                    last = f.read().strip()
            # 오늘 아직 점검 안 했고, 빌드는 끝난 상태일 때만
            if last != today and not is_build_needed():
                try:
                    def _empty(c):
                        return not c or not c.get('results')
                    pre = _load_pre_surge_cache()
                    sb  = _load_surge_buy_cache()
                    bc  = _load_buy_candidate_cache()
                    if _empty(pre) and _empty(sb) and _empty(bc):
                        # 수급 스캔 3종 모두 0건 → 데이터 불량 의심 → 자동 클린 리셋
                        print(f'[{today}] 헬스체크: 수급스캔 3종 모두 0건 → 자동 클린 리셋+재빌드')
                        _clean_cache_files()
                        _auto_build_cache()
                    else:
                        print(f'[{today}] 헬스체크: 정상 (수급스캔 결과 있음)')
                    # 결과와 무관하게 오늘은 점검 완료로 기록 (하루 1회만)
                    with open(_HEALTH_RETRY_PATH, 'w') as f:
                        f.write(today)
                except Exception as e:
                    print(f'헬스체크 오류: {e}')
            _time.sleep(4200)  # 70분 대기 후 다음 루프
        else:
            _time.sleep(1800)  # 30분마다 시간 확인


threading.Thread(target=_cache_health_check, daemon=True).start()


# ── 추천 종목 TOP 20 일일 캐시 ───────────────────────────────────
def _load_recommend_cache():
    if not os.path.exists(_RECOMMEND_CACHE_PATH):
        return None
    try:
        with open(_RECOMMEND_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


_RECOMMEND_ERROR_PATH = os.path.join(_DATA_DIR, 'recommend_error.txt')

def _run_recommend_scan():
    """추천 종목 20선 스캔 후 캐시 저장"""
    _status_set('recommend', 'running')
    try:
        results = scan_top_stocks(top_n=20, months=6)
        today = datetime.today().strftime('%Y-%m-%d')
        scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
        with open(_RECOMMEND_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'date': today, 'scanned_at': scanned_at, 'results': results}, f,
                      ensure_ascii=False)
        print(f'[{scanned_at}] 추천 종목 스캔 완료 — {len(results)}건')
        _status_set('recommend', 'done')
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        print(f'추천 종목 스캔 오류: {e}')
        _status_set('recommend', 'done')
        with open(_RECOMMEND_ERROR_PATH, 'w') as f:
            f.write(err)


@app.route('/api/recommend-error')
def recommend_error():
    if not os.path.exists(_RECOMMEND_ERROR_PATH):
        return jsonify({'error': None})
    with open(_RECOMMEND_ERROR_PATH) as f:
        return jsonify({'error': f.read()})


def _recommend_scheduler():
    """매일 20:00 UTC(=05:00 KST) 추천 종목 자동 스캔
    - 앱 시작 시 캐시 없으면 1회 백그라운드 스캔 (재배포 후 빈 화면 방지)
    - 페이지 방문 시 스캔 없음 — 캐시만 표시
    """
    import time as _time
    if _load_recommend_cache() is None:
        threading.Thread(target=_run_recommend_scan, daemon=True).start()

    while True:
        now = datetime.today()
        next_run = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        _time.sleep((next_run - now).total_seconds())
        _run_recommend_scan()


threading.Thread(target=_recommend_scheduler, daemon=True).start()


# ── 수급주도 종목 캐시 ────────────────────────────────────────────
def _load_supply_cache():
    if not os.path.exists(_SUPPLY_CACHE_PATH):
        return None
    try:
        with open(_SUPPLY_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _run_supply_scan():
    _status_set('supply', 'running')
    try:
        results = scan_supply_leaders(months=3)
        scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
        with open(_SUPPLY_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'scanned_at': scanned_at, 'results': results}, f, ensure_ascii=False)
        print(f'[{scanned_at}] 수급주도 스캔 완료 — {len(results)}건')
        _status_set('supply', 'done')
    except Exception as e:
        print(f'수급주도 스캔 오류: {e}')
        _status_set('supply', 'done')


def _supply_scheduler():
    """평일 02:30 / 05:30 UTC(=11:30 / 14:30 KST) 수급주도 종목 스캔
    - 캐시 없으면 앱 시작 시 1회 백그라운드 스캔
    - 페이지 방문 시 스캔 없음
    """
    import time as _time
    if _load_supply_cache() is None:
        threading.Thread(target=_run_supply_scan, daemon=True).start()

    while True:
        now = datetime.today()
        candidates = []
        for h, m in ((2, 30), (5, 30)):
            t = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if t > now:
                candidates.append(t)
        if candidates:
            next_run = min(candidates)
        else:
            next_run = (now + timedelta(days=1)).replace(
                hour=9, minute=30, second=0, microsecond=0)
        while next_run.weekday() >= 5:
            next_run += timedelta(days=1)
            next_run = next_run.replace(hour=9, minute=30, second=0, microsecond=0)
        _time.sleep((next_run - now).total_seconds())
        if datetime.today().weekday() < 5:
            _run_supply_scan()


threading.Thread(target=_supply_scheduler, daemon=True).start()


# ── 매수후보(단기) 캐시 ───────────────────────────────────────
def _load_buy_candidate_cache():
    if not os.path.exists(_BUY_CANDIDATE_CACHE_PATH):
        return None
    try:
        with open(_BUY_CANDIDATE_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _run_buy_candidate_scan():
    global _buy_candidate_scanning
    _buy_candidate_scanning = True
    _status_set('buy', 'running')
    try:
        results = scan_buy_candidates(top_n=10)
        scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
        with open(_BUY_CANDIDATE_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'scanned_at': scanned_at, 'results': results}, f, ensure_ascii=False)
        print(f'[{scanned_at}] 매수후보 스캔 완료 — {len(results)}건')
        _status_set('buy', 'done')
    except Exception as e:
        print(f'매수후보 스캔 오류: {e}')
        _status_set('buy', 'done')
    finally:
        _buy_candidate_scanning = False


def _buy_candidate_scheduler():
    """매일 16:00 UTC(=01:00 KST) 매수후보(단기) 자동 스캔 — 캐시 없으면 시작 시 1회 즉시 스캔"""
    import time as _time
    _bc = _load_buy_candidate_cache()
    if (not _bc or not _bc.get('results')) and not is_build_needed():
        threading.Thread(target=_run_buy_candidate_scan, daemon=True).start()

    while True:
        now = datetime.today()
        next_run = now.replace(hour=16, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        _time.sleep((next_run - now).total_seconds())
        _run_buy_candidate_scan()


threading.Thread(target=_buy_candidate_scheduler, daemon=True).start()


# ── 급등주 매수후보 캐시 ──────────────────────────────────────
def _load_surge_buy_cache():
    if not os.path.exists(_SURGE_BUY_CACHE_PATH):
        return None
    try:
        with open(_SURGE_BUY_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _run_surge_buy_scan():
    global _surge_buy_scanning
    _surge_buy_scanning = True
    _status_set('surge_buy', 'running')
    try:
        results = scan_surge_buy_candidates(top_n=10)
        scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
        with open(_SURGE_BUY_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'scanned_at': scanned_at, 'results': results}, f, ensure_ascii=False)
        print(f'[{scanned_at}] 급등주 매수후보 스캔 완료 — {len(results)}건')
        _status_set('surge_buy', 'done')
    except Exception as e:
        print(f'급등주 매수후보 스캔 오류: {e}')
        _status_set('surge_buy', 'done')
    finally:
        _surge_buy_scanning = False


def _surge_buy_scheduler():
    """매일 18:00 UTC(=03:00 KST) 급등주 매수후보 자동 스캔 — 캐시 없으면 시작 시 1회 즉시 스캔"""
    import time as _time
    _sb = _load_surge_buy_cache()
    if (not _sb or not _sb.get('results')) and not is_build_needed():
        threading.Thread(target=_run_surge_buy_scan, daemon=True).start()
    while True:
        now = datetime.today()
        next_run = now.replace(hour=18, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        _time.sleep((next_run - now).total_seconds())
        _run_surge_buy_scan()


threading.Thread(target=_surge_buy_scheduler, daemon=True).start()


# ── 교차 종목 탐지 + 텔레그램 알림 ─────────────────────────────
import requests as _req_tg

_TG_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
_TG_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
_alerted_today: set = set()   # 당일 이미 알림 보낸 ticker 집합
_alerted_date: str  = ''      # 날짜 바뀌면 초기화용


def _find_cross_picks():
    """스캔별 가중 점수 합산으로 복수 스캔 종목 선정.
    과매도 → 양수 기여 / 과매수 → 완전 제외. 점수 내림차순 반환."""

    sources: dict[str, list] = {}
    streak_map: dict = {}  # ticker → {'fs': max, 'is_': max}

    def _add(key, ticker, name, reasons, score: int = 0):
        sources.setdefault(key, []).append((ticker, name, reasons, score))

    def _investor_bonus(r) -> tuple:
        """점수 계산 + 신호 이유 반환. streak는 streak_map에서 별도 관리."""
        bonus = 0
        signal_reasons = []
        fs  = r.get('foreign_streak', 0) or 0
        is_ = r.get('inst_streak',   0) or 0

        if fs >= 5:   bonus += 15
        elif fs >= 3: bonus += 10
        elif fs >= 1: bonus += 5
        if is_ >= 3:  bonus += 8
        elif is_ >= 1: bonus += 4

        if r.get('joint_star'):
            jd = r.get('joint_days', '')
            bonus += 15; signal_reasons.append(f"외인+기관 동시매수 {jd}일")
        if r.get('buying_surge_star'):
            bonus += 12; signal_reasons.append("매수세 2배↑ 급증")
        if r.get('volume_surge'):
            bonus += 15; signal_reasons.append("거래량 급증")

        return bonus, signal_reasons, fs, is_

    def _track(ticker, fs, is_, ps=0):
        prev = streak_map.get(ticker, {'fs': 0, 'is_': 0, 'ps': 0})
        streak_map[ticker] = {'fs': max(prev['fs'], fs), 'is_': max(prev['is_'], is_), 'ps': max(prev['ps'], ps)}

    def _base_reasons(r):
        """스캔 원본 이유에서 외인·기관·연속매수 중복 항목 제거 (streak_map으로 통합)"""
        skip_kw = ('외인', '기관', '연속 매수', '동시매수', '수급량')
        return [x for x in r.get('reasons', []) if not any(k in x for k in skip_kw)]

    # 추천종목 +30 + 수급·거래량 보너스
    try:
        c = _load_recommend_cache()
        for r in (c.get('results', []) if c else []):
            bonus, sig, fs, is_ = _investor_bonus(r)
            _track(r['ticker'], fs, is_, r.get('pe_streak', 0) or 0)
            _add('추천종목', r['ticker'], r['name'], _base_reasons(r) + sig, 30 + bonus)
    except Exception:
        pass

    # 수급주도 +25 + 수급·거래량 보너스
    try:
        c = _load_supply_cache()
        for r in (c.get('results', []) if c else []):
            bonus, sig, fs, is_ = _investor_bonus(r)
            _track(r['ticker'], fs, is_, r.get('pe_streak', 0) or 0)
            _add('수급주도', r['ticker'], r['name'], sig, 25 + bonus)
    except Exception:
        pass

    # 과매도 → 강도 비례 양수 (+8 ~ +25)
    try:
        c = _load_osc_cache()
        for r in (c.get('oversold', []) if c else []):
            s100 = r.get('score100', 50)
            _add('과매도', r['ticker'], r['name'], [], max(8, round(s100 * 0.25)))
    except Exception:
        pass

    # 과매수 → 강도 비례 음수 패널티
    try:
        c = _load_osc_cache()
        for r in (c.get('overbought', []) if c else []):
            s100 = r.get('score100', 50)
            _add('과매수⚠', r['ticker'], r['name'], [], -max(8, round(s100 * 0.25)))
    except Exception:
        pass

    # MA반등 +20 + 수급 보너스
    try:
        c = _load_surge_cache()
        for r in (c.get('bounce', []) if c else []):
            bonus, sig, fs, is_ = _investor_bonus(r)
            _track(r['ticker'], fs, is_, r.get('pe_streak', 0) or 0)
            mas = r.get('touched_mas', [])
            label = '/'.join(mas) if mas else r.get('ma_label', '')
            _add('MA반등', r['ticker'], r['name'],
                 [f"{label} 반등 (눌림 {r.get('pullback_pct', '')}%)"] + sig, 20 + bonus)
    except Exception:
        pass

    # 매수후보단기 +25 + 수급·거래량 보너스
    try:
        c = _load_buy_candidate_cache()
        for r in (c.get('results', []) if c else []):
            bonus, sig, fs, is_ = _investor_bonus(r)
            _track(r['ticker'], fs, is_, r.get('pe_streak', 0) or 0)
            _add('매수후보단기', r['ticker'], r['name'], _base_reasons(r) + sig, 25 + bonus)
    except Exception:
        pass

    # 급등주매수후보 +35 + 수급·거래량 보너스
    try:
        c = _load_surge_buy_cache()
        for r in (c.get('results', []) if c else []):
            bonus, sig, fs, is_ = _investor_bonus(r)
            _track(r['ticker'], fs, is_, r.get('pe_streak', 0) or 0)
            _add('급등주매수후보', r['ticker'], r['name'], _base_reasons(r) + sig, 35 + bonus)
    except Exception:
        pass

    # 선취후보 +30 + 수급·거래량 보너스
    try:
        c = _load_pre_surge_cache()
        for r in (c.get('results', []) if c else []):
            bonus, sig, fs, is_ = _investor_bonus(r)
            _track(r['ticker'], fs, is_, r.get('pe_streak', 0) or 0)
            sig_reasons = r.get('signals', [])[:4]
            _add('선취후보', r['ticker'], r['name'], sig_reasons + sig, 30 + bonus)
    except Exception:
        pass

    ticker_map: dict = {}
    for scan_key, entries in sources.items():
        for ticker, name, reasons, score in entries:
            if ticker not in ticker_map:
                ticker_map[ticker] = {
                    'ticker': ticker, 'name': name,
                    'scans': [], 'reasons': [], 'total_score': 0
                }
            ticker_map[ticker]['scans'].append(scan_key)
            ticker_map[ticker]['total_score'] += score
            for r in reasons:
                if r and r not in ticker_map[ticker]['reasons']:
                    ticker_map[ticker]['reasons'].append(r)

    # streak 병합
    for ticker, st in streak_map.items():
        if ticker in ticker_map:
            ticker_map[ticker]['foreign_streak'] = st['fs']
            ticker_map[ticker]['inst_streak']    = st['is_']
            ticker_map[ticker]['pe_streak']      = st['ps']

    # 과매수 제외 세트 + 과매도 상세 점수 수집
    overbought_tickers = set()
    osc_score_map      = {}
    try:
        osc = _load_osc_cache()
        for r in (osc.get('overbought', []) if osc else []):
            overbought_tickers.add(r['ticker'])
        for r in (osc.get('oversold', []) if osc else []):
            osc_score_map[r['ticker']] = r.get('score100', 0)
    except Exception:
        pass

    # 반등/상승 신호 수집 — rebound_pct 50% 이하만 유효 진입 신호로 인정
    bounce_map = {}
    try:
        sc = _load_surge_cache()
        for r in (sc.get('bounce', []) if sc else []):
            rb = r.get('rebound_pct', 0) or 0
            if rb > 50:
                continue  # 이미 너무 많이 올라온 종목 제외
            mas = r.get('touched_mas', [])
            bounce_map[r['ticker']] = {
                'label': '/'.join(mas) if mas else '',
                'pct':   round(rb, 1),
                'type':  r.get('type', 'bounce'),
            }
    except Exception:
        pass

    # 2개 이상 스캔 등장 + 합산 점수 양수 + 수급 최소 조건 + 과매수 완전 제외
    picks = [v for v in ticker_map.values()
             if len(v['scans']) >= 2
             and v['total_score'] > 0
             and (v.get('foreign_streak', 0) > 0 or v.get('inst_streak', 0) > 0)
             and v['ticker'] not in overbought_tickers]

    for p in picks:
        p['osc_score']   = osc_score_map.get(p['ticker'])
        p['bounce_info'] = bounce_map.get(p['ticker'])

    picks.sort(key=lambda x: x['total_score'], reverse=True)
    return picks


def _send_telegram(text: str) -> dict:
    """텔레그램 메시지 발송. 결과 dict 반환 {'ok': bool, 'error': str|None}"""
    if not _TG_TOKEN or not _TG_CHAT_ID:
        print('[TG] TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 미설정 — 스킵')
        return {'ok': False, 'error': 'env 미설정'}
    try:
        url  = f'https://api.telegram.org/bot{_TG_TOKEN}/sendMessage'
        resp = _req_tg.post(url, json={'chat_id': _TG_CHAT_ID, 'text': text,
                                       'parse_mode': 'HTML'}, timeout=10)
        if resp.ok:
            print(f'[TG] 발송 완료')
            return {'ok': True, 'error': None}
        else:
            err = resp.text
            print(f'[TG] 발송 실패: {err}')
            return {'ok': False, 'error': err}
    except Exception as e:
        print(f'[TG] 발송 오류: {e}')
        return {'ok': False, 'error': str(e)}


def _calc_osc_based_prices(ticker):
    """복합 오실레이터 + 볼린저밴드 기반 매수가·익절가·손절가 계산.
    실시간 가격을 우선 사용하고, BB밴드는 캐시 기반으로 계산한다.
    현재가가 이미 BB 상단을 넘어선 경우 익절가를 현재가 기준으로 재설정.
    """
    try:
        from analysis.indicators import calc_indicators
        cached = load_stock_cache(ticker)
        if not cached or cached['ohlcv'].empty:
            return None

        df = calc_indicators(cached['ohlcv'].tail(90))
        req = ['close', 'rsi', 'stoch_k', 'macd_hist', 'mfi',
               'bb_upper', 'bb_lower', 'bb_mid']
        if any(c not in df.columns for c in req):
            return None

        # MACD 히스토그램 → 60일 rolling 범위로 0~100 정규화
        h      = df['macd_hist']
        h_min  = h.rolling(60, min_periods=1).min()
        h_max  = h.rolling(60, min_periods=1).max()
        h_rng  = (h_max - h_min).clip(lower=1e-9)
        macd_norm = ((h - h_min) / h_rng * 100).clip(0, 100)

        # 거래량 방향성 점수
        vol_ratio = (df['volume'] / df['volume'].rolling(20).mean()).clip(0, 3)
        price_up  = (df['close'] > df['close'].shift(1)).astype(float).fillna(0.5)
        vol_score = price_up * (vol_ratio / 3 * 100) + (1 - price_up) * ((1 - vol_ratio / 3) * 100)

        # 복합 오실레이터 (5개 지표 평균)
        composite = (df['rsi'].fillna(50) + df['stoch_k'].fillna(50) +
                     macd_norm.fillna(50) + df['mfi'].fillna(50) +
                     vol_score.fillna(50)) / 5

        last     = df.iloc[-1]
        bb_upper = float(last['bb_upper'])
        bb_lower = float(last['bb_lower'])
        bb_mid   = float(last['bb_mid'])
        cur_osc  = float(composite.iloc[-1]) if not pd.isna(composite.iloc[-1]) else 50

        # ── 실시간 현재가 우선 사용 ─────────────────────────────
        cur = float(last['close'])  # 기본값: 캐시 종가
        try:
            today = get_today_price(ticker)
            if today and today.get('close') and float(today['close']) > 0:
                cur = float(today['close'])
        except Exception:
            pass

        if cur <= 0 or bb_upper <= bb_lower:
            return None

        # ── 현재가 위치에 따라 매수가·익절가·손절가 결정 ─────────
        already_above_bb = cur >= bb_upper  # 이미 BB 상단 돌파

        buy_price = cur  # 매수가 = 현재가

        if already_above_bb:
            # 이미 BB 상단 위 — BB 상단이 지지선, 익절가는 BB 상단 위 +7%
            sell_price = cur * 1.07
            stop_loss  = max(bb_upper * 0.97, cur * 0.95)
        elif bb_upper > cur * 1.03:
            # 일반적인 경우 — BB 상단까지 여유 있음
            sell_price = bb_upper
            stop_loss  = max(bb_lower * 0.97, cur * 0.95)
        else:
            # BB 상단이 현재가와 너무 가까움 — +8% 목표
            sell_price = cur * 1.08
            stop_loss  = max(bb_lower * 0.97, cur * 0.95)

        profit_pct = round((sell_price - buy_price) / buy_price * 100, 1)

        return {
            'buy_price':       int(buy_price),
            'sell_price':      int(sell_price),
            'stop_loss':       int(stop_loss),
            'profit_pct':      profit_pct,
            'cur_osc':         round(cur_osc, 1),
            'above_bb':        already_above_bb,   # BB 상단 돌파 여부
        }
    except Exception:
        return None


def _send_cross_alert(picks: list):
    """공통 종목 텔레그램 알림 — 상위 5종목, 수급·오실레이터·차트 신호 포함"""
    import re as _re
    if not picks:
        return
    top5 = picks[:5]
    now_kst = (datetime.utcnow() + timedelta(hours=9)).strftime('%m/%d %H:%M')
    lines = [f'📊 <b>복수 스캔 공통 종목</b> ({now_kst} KST)\n']
    for p in top5:
        scans_str = ' · '.join(p['scans'])
        score_str = f" [{p.get('total_score', 0)}점]"

        # 외인·기관 연속 순매수일
        fs  = p.get('foreign_streak', 0) or 0
        is_ = p.get('inst_streak',   0) or 0
        investor_parts = []
        if fs  >= 1: investor_parts.append(f"외인 {fs}일 연속")
        if is_ >= 1: investor_parts.append(f"기관 {is_}일 연속")
        investor_str = ('  💰 ' + ' | '.join(investor_parts) + ' 순매수\n') if investor_parts else ''

        # 오실레이터 상태 (score100: 높을수록 강한 과매도, max≈100)
        osc_score = p.get('osc_score')
        if osc_score is not None:
            if osc_score >= 80:
                osc_label = f"과매도 강도 {osc_score}점 — 극단적 과매도, 반등 가능성 高"
            elif osc_score >= 60:
                osc_label = f"과매도 강도 {osc_score}점 — 강한 과매도 하단"
            else:
                osc_label = f"과매도 강도 {osc_score}점 — 과매도 진입"
            osc_str = f'  📉 {osc_label}\n'
        else:
            osc_str = ''

        # 차트 반등·상승 신호
        bounce = p.get('bounce_info')
        if bounce:
            label = bounce.get('label', '')
            pct   = bounce.get('pct', '')
            if bounce.get('type') == 'riding':
                bounce_str = f"  📈 {label} 타고 상승 중\n"
            else:
                bounce_str = f"  📈 {label} 반등 신호 (저점 대비 +{pct}%)\n"
        else:
            bounce_str = ''

        # 매수가·익절가·손절가 (실시간가 + BB 기반)
        price_info = _calc_osc_based_prices(p['ticker'])
        if price_info:
            osc_pos = price_info['cur_osc']
            if price_info.get('above_bb'):
                osc_tag = '🔴 BB 상단 돌파 — 단기 과열, 추격 주의'
            elif osc_pos < 30:
                osc_tag = '🟢 현재 과매도 — 매수 적기'
            elif osc_pos > 70:
                osc_tag = '🟡 오실레이터 과매수 — 진입 신중'
            else:
                osc_tag = f'🟡 오실레이터 {osc_pos:.0f} — 중립'
            buy_str = (
                f"  {osc_tag}\n"
                f"  💵 매수  {price_info['buy_price']:,}원"
                f"  →  익절  {price_info['sell_price']:,}원"
                f"  (+{price_info['profit_pct']}%)\n"
                f"  🔴 손절  {price_info['stop_loss']:,}원\n"
            )
        else:
            buy_str = ''

        # 선정 이유 — 점수 표기 제거, 빈 항목 정리
        cleaned = []
        for r in p.get('reasons', []):
            r2 = _re.sub(r'\s*\([+-]?\d+점?\)', '', r).strip()
            if r2 and r2 not in cleaned:
                cleaned.append(r2)
        reasons_str = '\n'.join(f'  · {r}' for r in cleaned[:4]) if cleaned else ''

        lines.append(
            f"<b>{p['name']}</b> ({p['ticker']}){score_str}\n"
            f"  📌 {scans_str}\n"
            f"{investor_str}"
            f"{osc_str}"
            f"{bounce_str}"
            f"{buy_str}"
            f"{reasons_str}"
        )
    lines.append('\n⚠️ 투자 판단은 본인 책임입니다.')
    return _send_telegram('\n\n'.join(lines))


def _cross_alert_scheduler():
    """평일 09:00, 13:30 KST — 100점 이상 복수 스캔 공통 종목 텔레그램 발송"""
    import time as _time
    global _alerted_today, _alerted_date

    _time.sleep(120)   # 앱 시작 직후 실행 방지

    while True:
        now_kst   = datetime.utcnow() + timedelta(hours=9)
        today_str = now_kst.strftime('%Y-%m-%d')

        if today_str != _alerted_date:
            _alerted_today = set()
            _alerted_date  = today_str

        is_morning = (now_kst.hour == 9  and 0 <= now_kst.minute < 59)
        is_lunch   = (now_kst.hour == 13 and 30 <= now_kst.minute < 59)
        slot = 'morning' if is_morning else ('lunch' if is_lunch else None)

        if slot and slot not in _alerted_today and now_kst.weekday() < 5:
            try:
                picks = _find_cross_picks()
                # 100점 이상만 발송
                strong = [p for p in picks if p.get('total_score', 0) >= 100]
                if strong:
                    _send_cross_alert(strong)
                    _alerted_today.add(slot)
                    print(f'[CrossAlert] {slot} 발송 완료 — {len(strong)}종목 (100점↑)')
                elif picks:
                    # picks는 있지만 100점 미만 → 오늘 조용한 날, 재시도 불필요
                    _alerted_today.add(slot)
                    print(f'[CrossAlert] {slot} — 100점 이상 종목 없음 ({len(picks)}종목 중)')
                else:
                    # picks == 0 → 캐시 아직 미빌드, 슬롯 완료 처리 안 함 (10분 후 재시도)
                    print(f'[CrossAlert] {slot} — 캐시 없음, 10분 후 재시도')
            except Exception as e:
                print(f'[CrossAlert] 오류: {e}')

        _time.sleep(600)   # 10분마다 체크 (시간대 놓치지 않도록)


threading.Thread(target=_cross_alert_scheduler, daemon=True).start()


_pre_surge_scanning = False
_pre_surge_alerted_today: set = set()
_pre_surge_alerted_date: str = ''


def _run_pre_surge_scan():
    """선취 후보 스캔 → pre_surge_cache.json 저장"""
    global _pre_surge_scanning
    _pre_surge_scanning = True
    _status_set('pre_surge', 'running')
    try:
        results = scan_pre_surge(top_n=10)
        now_kst = (datetime.utcnow() + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')
        payload = {'updated_at': now_kst, 'results': results}
        with open(_PRE_SURGE_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
        print(f'[선취스캔] 완료 {len(results)}개 → 캐시 저장')
    except Exception as e:
        print(f'[선취스캔] 오류: {e}')
    finally:
        _pre_surge_scanning = False
        _status_set('pre_surge', 'done')


def _send_pre_surge_alert(results: list):
    """선취 후보 텔레그램 발송 — 상위 5종목"""
    import re as _re
    if not results:
        return {'ok': False, 'error': '결과 없음'}
    top5 = results[:5]
    now_kst = (datetime.utcnow() + timedelta(hours=9)).strftime('%m/%d %H:%M')
    lines = [f'🎯 <b>급등 선취 후보</b> ({now_kst} KST)\n오실레이터 바닥 반전 + 외인/기관 초기 진입\n']

    for p in top5:
        # 신호 목록
        sigs = p.get('signals', [])
        sig_str = '\n'.join(f'  · {s}' for s in sigs[:5]) if sigs else ''

        # 외인/기관
        fs  = p.get('foreign_streak', 0) or 0
        is_ = p.get('inst_streak', 0) or 0
        inv_parts = []
        if fs  >= 1: inv_parts.append(f'외인 {fs}일 연속')
        if is_ >= 1: inv_parts.append(f'기관 {is_}일 연속')
        inv_str = ('  💰 ' + ' | '.join(inv_parts) + ' 순매수\n') if inv_parts else ''

        # 오실레이터 현재 위치
        osc = p.get('cur_osc', 50)
        if osc <= 30:
            osc_label = f'🟢 오실레이터 {osc:.0f} — 과매도 바닥권 (매수 적기)'
        elif osc <= 45:
            osc_label = f'🟡 오실레이터 {osc:.0f} — 바닥 탈출 중'
        else:
            osc_label = f'🟡 오실레이터 {osc:.0f}'

        # 매수가/익절가
        price_info = _calc_osc_based_prices(p['ticker'])
        if price_info:
            buy_str = (
                f'  💵 매수  {price_info["buy_price"]:,}원'
                f'  →  익절  {price_info["sell_price"]:,}원'
                f'  (+{price_info["profit_pct"]}%)\n'
                f'  🔴 손절  {price_info["stop_loss"]:,}원\n'
            )
        else:
            buy_str = ''

        lines.append(
            f"<b>{p['name']}</b> ({p['ticker']})\n"
            f"  현재가 {p['price']}원\n"
            f"{sig_str}\n"
            f"{inv_str}"
            f"  {osc_label}\n"
            f"{buy_str}"
        )

    lines.append('\n⚠️ 투자 판단은 본인 책임입니다.')
    return _send_telegram('\n\n'.join(lines))


def _pre_surge_scheduler():
    """오전 9:30, 오후 1:00 KST에 선취 스캔 후 텔레그램 발송"""
    import time as _time
    global _pre_surge_alerted_today, _pre_surge_alerted_date
    _time.sleep(180)  # 앱 시작 직후 방지

    while True:
        now_kst   = datetime.utcnow() + timedelta(hours=9)
        today_str = now_kst.strftime('%Y-%m-%d')

        if today_str != _pre_surge_alerted_date:
            _pre_surge_alerted_today = set()
            _pre_surge_alerted_date  = today_str

        # 평일 9:30~10:30 또는 13:00~14:00 에 한 번씩 발송 (캐시 빌드 대기 여유 포함)
        is_morning = (now_kst.hour == 9  and 30 <= now_kst.minute < 60) or \
                     (now_kst.hour == 10 and 0  <= now_kst.minute < 30)
        is_lunch   = (now_kst.hour == 13 and 0  <= now_kst.minute < 59)
        slot = 'morning' if is_morning else ('lunch' if is_lunch else None)

        if slot and slot not in _pre_surge_alerted_today and now_kst.weekday() < 5:
            try:
                # pkl 캐시가 없으면 스캔 결과 0건 → 빌드 완료 후 자동 재시도됨
                _run_pre_surge_scan()
                cache = _load_pre_surge_cache()
                if cache and cache.get('results'):
                    _send_pre_surge_alert(cache['results'])
                    _pre_surge_alerted_today.add(slot)
                else:
                    print(f'[선취스케줄러] {slot} — 결과 없음 (캐시 빌드 중?), 재시도 대기')
            except Exception as e:
                print(f'[선취스케줄러] 오류: {e}')

        _time.sleep(900)  # 15분마다 체크


threading.Thread(target=_pre_surge_scheduler, daemon=True).start()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/analyze', methods=['GET', 'POST'])
def analyze():
    if request.method == 'GET':
        query = request.args.get('query', '').strip()
        months = int(request.args.get('months', 6))
        if not query:
            return render_template('index.html')
    else:
        query = request.form.get('query', '').strip()
        months = int(request.form.get('months', 6))

    if not query:
        return render_template('index.html', error="종목명 또는 코드를 입력하세요.")

    ticker, name = get_ticker(query)
    if not ticker:
        return render_template('index.html', error=f"'{query}' 종목을 찾을 수 없습니다.")

    # KOSPI200·KOSDAQ150 외 종목은 기업소개 페이지로
    if not is_main_stock(ticker):
        try:
            company_info = get_company_info(ticker)
        except Exception:
            company_info = {}
        try:
            market_profile = get_market_profile(ticker)
        except Exception:
            market_profile = {'market_cap': 'N/A', 'w52_high': 'N/A', 'w52_low': 'N/A', 'market_type': 'N/A'}
        return render_template('company_only.html',
                               name=name, ticker=ticker,
                               company_info=company_info,
                               market_profile=market_profile)

    # ── 데이터 수집 (캐시 우선 → 미스 시 병렬 fetch) ────────────
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor

    _empty_fundamental = {'per': 'N/A', 'forward_per': 'N/A', 'pbr': 'N/A', 'operating_profit': [],
                          'roe': 'N/A', 'op_margin': 'N/A', 'debt_ratio': 'N/A', 'revenue': []}
    _empty_news = {'total': 0, 'positive': 0, 'negative': 0, 'neutral': 0,
                   'sentiment_score': 0, 'top_keywords': [], 'press_counts': {},
                   'exclusive_count': 0, 'articles': []}

    cached = load_stock_cache(ticker)

    if cached:
        ohlcv_full  = cached['ohlcv']
        investor_df = cached['investor_df']
        supply_df   = cached['supply_df']
        fundamental = cached['fundamental']

        # 모든 네트워크 호출 병렬 실행 (today_price 포함)
        def _today():
            try: return get_today_price(ticker)
            except: return None
        def _company():
            try: return get_company_info(ticker)
            except: return {}
        def _profile():
            try:
                p = get_market_profile(ticker)
                # 52주 고저가 없으면 캐시에서 계산 (pykrx 호출 회피)
                if p.get('w52_high') == 'N/A' and cached:
                    yr = cached['ohlcv'].tail(260)
                    if not yr.empty:
                        p['w52_high'] = f"{int(yr['high'].max()):,}"
                        p['w52_low']  = f"{int(yr['low'].min()):,}"
                return p
            except: return {'market_cap': 'N/A', 'w52_high': 'N/A', 'w52_low': 'N/A', 'market_type': 'N/A'}
        def _news():
            try: return analyze_news(search_naver_news(name, days=30))
            except: return _empty_news
        def _research():
            try: return get_research_reports(ticker)
            except: return []
        def _disclosures():
            try: return get_disclosures(ticker, days=60)
            except: return []

        with ThreadPoolExecutor(max_workers=6) as ex:
            f_td = ex.submit(_today)
            f_co = ex.submit(_company)
            f_pr = ex.submit(_profile)
            f_nw = ex.submit(_news)
            f_rr = ex.submit(_research)
            f_dc = ex.submit(_disclosures)
            today_info     = f_td.result()
            company_info   = f_co.result()
            market_profile = f_pr.result()
            news_result    = f_nw.result()
            research_reports = f_rr.result()
            disclosures    = f_dc.result()
        research_summary = ""

        ohlcv_full = append_today(ohlcv_full, today_info)
        ohlcv      = ohlcv_full.tail(max(months * 22, 60))
    else:
        # 캐시 미스: 모든 항목 병렬 fetch
        def _ohlcv():
            try: return get_ohlcv(ticker, months)
            except: return pd.DataFrame()
        def _investor():
            try: return get_investor_detail(ticker, months)
            except: return pd.DataFrame()
        def _supply():
            try: return get_supply_zone(ticker, max(months, 6))
            except: return pd.DataFrame({'price_mid': [], 'volume': []})
        def _fundamental():
            try: return get_fundamental(ticker)
            except: return _empty_fundamental
        def _company():
            try: return get_company_info(ticker)
            except: return {}
        def _profile():
            try: return get_market_profile(ticker)
            except: return {'market_cap': 'N/A', 'w52_high': 'N/A', 'w52_low': 'N/A', 'market_type': 'N/A'}
        def _news():
            try: return analyze_news(search_naver_news(name, days=30))
            except: return _empty_news
        def _research():
            try: return get_research_reports(ticker)
            except: return []
        def _disclosures():
            try: return get_disclosures(ticker, days=60)
            except: return []

        with ThreadPoolExecutor(max_workers=9) as ex:
            f_ohl = ex.submit(_ohlcv)
            f_inv = ex.submit(_investor)
            f_sup = ex.submit(_supply)
            f_fun = ex.submit(_fundamental)
            f_co  = ex.submit(_company)
            f_pr  = ex.submit(_profile)
            f_nw  = ex.submit(_news)
            f_rr  = ex.submit(_research)
            f_dc  = ex.submit(_disclosures)
            ohlcv          = f_ohl.result()
            investor_df    = f_inv.result()
            supply_df      = f_sup.result()
            fundamental    = f_fun.result()
            company_info   = f_co.result()
            market_profile = f_pr.result()
            news_result    = f_nw.result()
            research_reports = f_rr.result()
            disclosures    = f_dc.result()
        research_summary = ""

    # OHLCV 데이터 부족하면 기업소개 페이지로
    if ohlcv is None or ohlcv.empty or len(ohlcv) < 20:
        return render_template('company_only.html',
                               name=name, ticker=ticker,
                               company_info=company_info,
                               market_profile=market_profile)

    # 지표 계산
    df = calc_indicators(ohlcv)
    # MA 차트용: 120일선 표시를 위해 ohlcv_full(6개월) 기준으로 별도 계산
    _ohlcv_full = ohlcv_full if 'ohlcv_full' in dir() and len(ohlcv_full) > len(ohlcv) else ohlcv
    df_ma = calc_indicators(_ohlcv_full) if len(_ohlcv_full) > len(ohlcv) else df
    ma_status = get_ma_arrangement(df)
    signals = get_latest_signals(df)
    current_price = int(df['close'].iloc[-1])

    if supply_df is None:
        supply_df = pd.DataFrame({'price_mid': [], 'volume': []})

    # 신호 계산
    score, reasons = calc_score(ma_status, signals, investor_df, news_result, df)
    recommendation, rec_color = get_recommendation(score)
    score_pct = max(0, min(100, round((score + 14) / 34 * 100)))

    # AI 분석 — 별도 API로 지연 로딩 (페이지 속도 개선)
    ai_comment = None

    # 차트 패턴 탐지
    try:
        detected_patterns = detect_patterns(df)
    except Exception:
        detected_patterns = []

    # 차트
    main_chart = make_main_chart(df, name, patterns=detected_patterns)
    ma_chart   = make_ma_chart(df_ma, name)
    # 캔들차트와 동일한 y축 범위를 매물대에 전달 (최근 40거래일 기준, make_main_chart와 동일)
    _recent     = df.tail(40)
    _c_low      = float(_recent['low'].min())
    _c_high     = float(_recent['high'].max())
    _pad        = (_c_high - _c_low) * 0.05
    chart_y_min = _c_low  - _pad
    chart_y_max = _c_high + _pad
    try:
        supply_chart = (make_supply_zone_chart(supply_df, current_price, chart_y_min, chart_y_max)
                        if not supply_df.empty else None)
    except Exception:
        supply_chart = None
    try:
        investor_chart = make_investor_chart(investor_df)
    except Exception:
        investor_chart = None

    # 증권사 목표가 min/max — 개별 리포트 페이지에서 병렬 수집
    target_prices = _get_report_target_prices(research_reports)
    target_min = f"{min(target_prices):,}" if target_prices else None
    target_max = f"{max(target_prices):,}" if target_prices else None

    return render_template('result.html',
        patterns=simplify_patterns(detected_patterns),
        disclosures=disclosures,
        company_info=company_info,
        market_profile=market_profile,
        name=name, ticker=ticker, current_price=f"{current_price:,}",
        ma_chart=ma_chart, score_pct=score_pct,
        months=months,
        ma_label=ma_status[0], ma_type=ma_status[1],
        signals=signals,
        fundamental=fundamental,
        score=score, recommendation=recommendation, rec_color=rec_color,
        reasons=reasons,
        news=news_result,
        research_reports=research_reports,
        research_summary=research_summary,
        target_min=target_min, target_max=target_max,
        ai_comment=ai_comment,
        main_chart=main_chart,
        supply_chart=supply_chart,
        investor_chart=investor_chart
    )


@app.route('/recommend')
def recommend():
    cache = _load_recommend_cache()
    results    = cache.get('results', []) if cache else []
    scanned_at = cache.get('scanned_at', '') if cache else ''
    for r in results:
        r.setdefault('foreign_streak', 0)
        r.setdefault('inst_streak', 0)
        r.setdefault('joint_star', False)
        r.setdefault('joint_days', 0)
        r.setdefault('buying_surge_star', False)
        r.setdefault('volume_surge', False)
        r.setdefault('score_pct', max(0, min(100, round((r.get('score', 0) + 14) / 50 * 100))))
    return render_template('recommend.html', results=results, scanned_at=scanned_at)


@app.route('/supply-leaders')
def supply_leaders():
    cache = _load_supply_cache()
    results = cache.get('results', []) if cache else []
    scanned_at = cache.get('scanned_at', '') if cache else ''
    for r in results:
        r.setdefault('foreign_streak', 0)
        r.setdefault('inst_streak', 0)
        r.setdefault('joint_star', False)
        r.setdefault('joint_days', 0)
        r.setdefault('buying_surge_star', False)
        r.setdefault('volume_surge', False)
        r.setdefault('score_pct', max(0, min(100, round((r.get('score', 0) + 14) / 50 * 100))))
    return render_template('supply_leaders.html', results=results, scanned_at=scanned_at)


@app.route('/surge-buy-candidates')
def surge_buy_candidates():
    cache = _load_surge_buy_cache()
    results    = cache.get('results', []) if cache else []
    scanned_at = cache.get('scanned_at', '') if cache else ''
    return render_template('surge_buy_candidates.html',
                           results=results,
                           scanned_at=scanned_at,
                           scanning=_surge_buy_scanning)


@app.route('/buy-candidates')
def buy_candidates():
    cache = _load_buy_candidate_cache()
    results    = cache.get('results', []) if cache else []
    scanned_at = cache.get('scanned_at', '') if cache else ''
    build_status = get_build_status()
    cache_days_old = 0
    if build_status:
        try:
            cache_days_old = (datetime.today() - datetime.strptime(build_status['date'], '%Y-%m-%d')).days
        except Exception:
            pass
    return render_template('buy_candidates.html',
                           results=results,
                           scanned_at=scanned_at,
                           scanning=_buy_candidate_scanning,
                           cache_days_old=cache_days_old)


@app.route('/export-surge')
def export_surge():
    cache = load_export_cache()
    if cache is None:
        return render_template('export_surge.html', high=[], moderate=[], scanning=False,
                               updated_at=None, total=0, high_count=0, moderate_count=0)
    results = cache.get('results', [])
    # 가격이 N/A인 종목은 주가 캐시에서 보완
    for r in results:
        if r.get('price', 'N/A') in ('N/A', '', None):
            try:
                sc = load_stock_cache(r['ticker'])
                if sc and not sc['ohlcv'].empty:
                    r['price'] = f"{int(sc['ohlcv']['close'].iloc[-1]):,}"
            except Exception:
                pass
    high     = [r for r in results if r.get('tier') == 'high']
    moderate = [r for r in results if r.get('tier') == 'moderate']
    return render_template('export_surge.html',
                           high=high,
                           moderate=moderate,
                           scanning=False,
                           updated_at=cache.get('updated_at', ''),
                           total=cache.get('count', 0),
                           high_count=len(high),
                           moderate_count=len(moderate),
                           dart_limit_hit=cache.get('dart_limit_hit', False))


@app.route('/export-surge/refresh', methods=['POST'])
def export_surge_refresh():
    """수동 재스캔 트리거"""
    threading.Thread(target=lambda: scan_export_growth(growth_threshold=10), daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/api/osc-picks')
def osc_picks():
    cache = _load_osc_cache()
    if cache:
        return jsonify({**cache, 'scanning': False})
    return jsonify({'updated_at': '', 'oversold': [], 'overbought': [], 'scanning': _osc_scanning})


@app.route('/api/osc-refresh', methods=['POST'])
def osc_refresh():
    threading.Thread(target=_run_osc_scan, daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/osc-radar')
def osc_radar_page():
    """과매도·과매수 레이더 전용 페이지"""
    cache = _load_osc_cache()
    oversold   = cache.get('oversold',   []) if cache else []
    overbought = cache.get('overbought', []) if cache else []
    updated_at = cache.get('updated_at', '') if cache else ''
    return render_template('osc_radar.html',
                           oversold=oversold,
                           overbought=overbought,
                           updated_at=updated_at,
                           scanning=_osc_scanning)


@app.route('/api/recommend-refresh', methods=['POST'])
def recommend_refresh():
    threading.Thread(target=_run_recommend_scan, daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/api/supply-refresh', methods=['POST'])
def supply_refresh():
    threading.Thread(target=_run_supply_scan, daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/api/surge-buy-picks')
def surge_buy_picks():
    cache = _load_surge_buy_cache()
    if cache:
        return jsonify({**cache, 'scanning': _surge_buy_scanning})
    return jsonify({'scanned_at': '', 'results': [], 'scanning': _surge_buy_scanning})


@app.route('/api/surge-buy-refresh', methods=['POST'])
def surge_buy_refresh():
    threading.Thread(target=_run_surge_buy_scan, daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/api/buy-candidate-picks')
def buy_candidate_picks():
    cache = _load_buy_candidate_cache()
    if cache:
        return jsonify({**cache, 'scanning': _buy_candidate_scanning})
    return jsonify({'scanned_at': '', 'results': [], 'scanning': _buy_candidate_scanning})


@app.route('/api/buy-candidate-refresh', methods=['POST'])
def buy_candidate_refresh():
    threading.Thread(target=_run_buy_candidate_scan, daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/api/scan-all', methods=['POST'])
def scan_all():
    """모든 스캔 한 번에 실행"""
    with _scan_status_lock:
        for key in _SCAN_LABELS:
            _scan_status[key] = 'pending'
    threading.Thread(target=_run_recommend_scan, daemon=True).start()
    threading.Thread(target=_run_supply_scan, daemon=True).start()
    threading.Thread(target=_run_osc_scan, daemon=True).start()
    threading.Thread(target=_run_surge_scan, daemon=True).start()
    threading.Thread(target=_run_export_scan, daemon=True).start()
    threading.Thread(target=_run_buy_candidate_scan, daemon=True).start()
    threading.Thread(target=_run_surge_buy_scan, daemon=True).start()
    threading.Thread(target=_run_pre_surge_scan, daemon=True).start()
    return jsonify({'status': 'scanning', 'message': '모든 스캔 시작됨 — 완료까지 30~60분 소요'})


@app.route('/api/krx-test')
def krx_test():
    """KRX 인증 + 사모 컬럼 반환 여부 테스트 (세션 주입 포함)"""
    from pykrx import stock as _stk
    from pykrx.website.comm import webio as _webio
    from pykrx.website.comm.auth import build_krx_session
    from datetime import datetime, timedelta
    krx_id = os.getenv('KRX_ID', '')
    krx_pw = os.getenv('KRX_PW', '')
    # 인증 후 pykrx 내부 세션에 주입
    auth_ok = False
    auth_msg = ''
    try:
        sess = build_krx_session(krx_id, krx_pw)
        _webio._session = sess
        auth_ok = sess is not None
        auth_msg = 'success' if auth_ok else 'failed'
    except Exception as e:
        auth_msg = str(e)
    end = datetime.today().strftime('%Y%m%d')
    start = (datetime.today() - timedelta(days=20)).strftime('%Y%m%d')
    cols, has_samo, rows = [], False, 0
    err_msg = ''
    try:
        from pykrx.website.krx.market.wrap import get_market_trading_value_and_volume_on_ticker_by_date as _detail_fn
        df = _detail_fn(start, end, '005930', '거래량', '순매수', True)
        cols = list(df.columns)
        has_samo = any('사모' in c for c in cols)
        rows = len(df)
    except Exception as e:
        err_msg = str(e)
    return jsonify({
        'krx_id_set': bool(krx_id),
        'auth': auth_msg,
        'auth_ok': auth_ok,
        'columns': cols,
        'has_samo': has_samo,
        'rows': rows,
        'error': err_msg,
    })


@app.route('/api/investor-table/<ticker>')
def investor_table_api(ticker):
    """외인·기관 순매수 상세 표 — 최근 20거래일, 연기금·사모·보험·투신 등 세분화"""
    try:
        cached = load_stock_cache(ticker)
        inv_df = cached['investor_df'] if cached else None
        if inv_df is None or inv_df.empty:
            from analysis.data_fetcher import get_investor_detail
            inv_df = get_investor_detail(ticker, months=2)
        if inv_df is None or inv_df.empty:
            return jsonify({'ok': False, 'rows': [], 'columns': []})

        df = inv_df.tail(20).copy()

        COL_ORDER = [
            ('외국인합계', '외국인', '외인'),
            ('기타외국인',),
            ('연기금',),
            ('사모',),
            ('보험',),
            ('투신',),
            ('금융투자',),
            ('은행',),
            ('기타금융',),
            ('기타법인',),
            ('개인',),
        ]
        def _find(keywords):
            for kw in keywords:
                for c in df.columns:
                    if kw in str(c):
                        return c
            return None

        col_map = []   # (label, col_name)
        for keywords in COL_ORDER:
            found = _find(keywords)
            if found:
                col_map.append((keywords[0], found))

        # 기관합계 = 세부 합산 or 직접 컬럼
        inst_cols = [c for lbl, c in col_map if lbl not in ('외국인합계', '외국인', '외인', '개인')]
        if inst_cols:
            df['__기관합계__'] = sum(df[c].fillna(0) for c in inst_cols)
        else:
            inst_col = _find(('기관합계', '기관'))
            df['__기관합계__'] = df[inst_col].fillna(0) if inst_col else 0

        col_map_final = []
        # 외국인 먼저
        for lbl, c in col_map:
            if lbl in ('외국인합계', '외국인', '외인'):
                col_map_final.append(('외국인', c))
                break
        # 기관합계
        col_map_final.append(('기관합계', '__기관합계__'))
        # 기관 세부
        for lbl, c in col_map:
            if lbl not in ('외국인합계', '외국인', '외인', '개인'):
                col_map_final.append((lbl, c))
        # 개인
        for lbl, c in col_map:
            if lbl == '개인':
                col_map_final.append((lbl, c))

        rows = []
        for idx, row in df.iterrows():
            date_str = str(idx)[:10]
            cells = {'날짜': date_str}
            for lbl, c in col_map_final:
                v = float(row[c]) if c in df.columns else 0.0
                cells[lbl] = int(v)
            rows.append(cells)

        rows.reverse()  # 최신 날짜가 위로
        columns = ['날짜'] + [lbl for lbl, _ in col_map_final]
        return jsonify({'ok': True, 'rows': rows, 'columns': columns})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'rows': [], 'columns': []})


@app.route('/api/investor-debug/<ticker>')
def investor_debug(ticker):
    """특정 종목 investor_df 컬럼·최근 5행 반환 — 사모 수집 여부 확인용"""
    from analysis.data_fetcher import get_investor_detail
    df = get_investor_detail(ticker, months=1)
    if df.empty:
        return jsonify({'ok': False, 'error': 'empty', 'columns': []})
    samo_col = next((c for c in df.columns if '사모' in c), None)
    last5 = {}
    if samo_col:
        last5 = {str(k): int(v) for k, v in df[samo_col].tail(5).items()}
    return jsonify({
        'ok': True,
        'columns': list(df.columns),
        'has_samo': samo_col is not None,
        'samo_col': samo_col,
        'samo_last5': last5,
        'rows': len(df),
    })


@app.route('/api/rebuild-cache', methods=['POST'])
def rebuild_cache_api():
    """캐시 강제 재빌드 — 플래그·pkl·스캔결과 전부 삭제 후 재수집 (약 5분 소요)"""
    _clean_cache_files()  # 플래그·pkl·스캔결과 JSON 삭제 (공통 함수)
    threading.Thread(target=_auto_build_cache, daemon=True).start()
    return jsonify({'status': 'building', 'message': '캐시 재빌드 시작 — 약 5분 소요 후 스캔 자동 실행됩니다'})


@app.route('/api/scan-progress')
def scan_progress():
    """전체 스캔 진행률 폴링 엔드포인트"""
    return jsonify(_build_progress_response())


@app.route('/api/cross-picks')
def api_cross_picks():
    """공통 선별 종목 조회"""
    try:
        picks = _find_cross_picks()
        now_kst = (datetime.utcnow() + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')
        return jsonify({'ok': True, 'picks': picks, 'updated_at': now_kst})
    except Exception as e:
        return jsonify({'ok': False, 'picks': [], 'error': str(e)})


@app.route('/api/telegram-test')
def telegram_test():
    """텔레그램 봇 연결 테스트"""
    if not _TG_TOKEN or not _TG_CHAT_ID:
        return jsonify({'ok': False, 'error': 'TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 환경변수가 설정되지 않았습니다.'})
    now_kst = (datetime.utcnow() + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')
    _send_telegram(f'✅ 주식봇 연결 테스트 성공!\n\n{now_kst} KST\n공통 종목 발견 시 이 채팅으로 알림이 옵니다.')
    return jsonify({'ok': True, 'message': '텔레그램으로 테스트 메시지를 발송했습니다.'})


@app.route('/api/cross-alert-test')
def cross_alert_test():
    """현재 캐시 기준 공통 종목을 즉시 텔레그램으로 발송 (테스트용)"""
    if not _TG_TOKEN or not _TG_CHAT_ID:
        return jsonify({'ok': False, 'error': 'TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 환경변수가 설정되지 않았습니다.'})
    try:
        picks = _find_cross_picks()
        if not picks:
            return jsonify({'ok': True, 'message': '공통 종목이 없습니다 (캐시 확인 필요)', 'count': 0})
        tg_result = _send_cross_alert(picks)
        names = [f"{p['name']}({p['ticker']})" for p in picks[:5]]
        if tg_result and not tg_result.get('ok'):
            return jsonify({'ok': False, 'message': '종목 선정 완료, 텔레그램 발송 실패',
                            'picks': names, 'tg_error': tg_result.get('error')})
        return jsonify({'ok': True, 'message': f'{len(picks)}건 발견, 상위 5종목 발송 완료', 'picks': names})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()})


@app.route('/api/pre-surge-picks')
def pre_surge_picks_api():
    """선취 후보 캐시 조회"""
    cache = _load_pre_surge_cache()
    if not cache:
        return jsonify({'results': [], 'updated_at': None, 'scanning': _pre_surge_scanning})
    return jsonify({
        'results':    cache.get('results', []),
        'updated_at': cache.get('updated_at'),
        'scanning':   _pre_surge_scanning,
    })


@app.route('/api/pre-surge-refresh', methods=['POST'])
def pre_surge_refresh_api():
    """선취 후보 스캔 즉시 실행 (비동기)"""
    global _pre_surge_scanning
    if _pre_surge_scanning:
        return jsonify({'ok': True, 'message': '스캔 진행 중'})
    threading.Thread(target=_run_pre_surge_scan, daemon=True).start()
    return jsonify({'ok': True, 'message': '선취 후보 스캔 시작'})


@app.route('/api/pre-surge-alert')
def pre_surge_alert_api():
    """선취 후보 즉시 텔레그램 발송 (테스트용)"""
    if not _TG_TOKEN or not _TG_CHAT_ID:
        return jsonify({'ok': False, 'error': '텔레그램 환경변수 미설정'})
    try:
        cache = _load_pre_surge_cache()
        if not cache or not cache.get('results'):
            # 캐시 없으면 즉시 스캔
            _run_pre_surge_scan()
            cache = _load_pre_surge_cache()
        if not cache or not cache.get('results'):
            return jsonify({'ok': False, 'error': '선취 후보 없음 — 캐시 빌드 후 재시도'})
        results = cache['results']
        tg_result = _send_pre_surge_alert(results)
        names = [f"{r['name']}({r['ticker']})" for r in results[:5]]
        if tg_result and not tg_result.get('ok'):
            return jsonify({'ok': False, 'error': tg_result.get('error'), 'picks': names})
        return jsonify({'ok': True, 'message': f'{len(results)}건 발견, 상위 5종목 발송', 'picks': names})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()})


@app.route('/api/ai-comment')
def ai_comment_api():
    """AI 분석을 별도로 요청 (result 페이지에서 비동기 호출)"""
    ticker = request.args.get('ticker', '').strip()
    name   = request.args.get('name', '').strip()
    if not ticker or not name:
        return jsonify({'comment': 'ticker/name 파라미터가 필요합니다.'})
    try:
        import pandas as pd
        cached = load_stock_cache(ticker)
        if cached:
            ohlcv       = cached['ohlcv'].tail(66)
            investor_df = cached['investor_df']
            fundamental = cached['fundamental']
        else:
            ohlcv = get_ohlcv(ticker, months=3)
            if ohlcv.empty:
                return jsonify({'comment': '데이터를 불러올 수 없습니다.'})
            try: investor_df = get_investor_detail(ticker, months=1)
            except: investor_df = pd.DataFrame()
            try: fundamental = get_fundamental(ticker)
            except: fundamental = {'per':'N/A','forward_per':'N/A','pbr':'N/A','operating_profit':[],'roe':'N/A','op_margin':'N/A','debt_ratio':'N/A','revenue':[]}
        df = calc_indicators(ohlcv)
        ma_status = get_ma_arrangement(df)
        signals   = get_latest_signals(df)
        try:
            articles = search_naver_news(name, days=30)
            news_result = analyze_news(articles)
        except: news_result = {'total':0,'positive':0,'negative':0,'neutral':0,'sentiment_score':0,'top_keywords':[],'press_counts':{},'exclusive_count':0,'articles':[]}
        score, reasons = calc_score(ma_status, signals, investor_df, news_result, df)
        comment = get_ai_analysis(name, score, reasons, signals, fundamental, news_result)
        return jsonify({'comment': comment})
    except Exception as e:
        return jsonify({'comment': f'AI 분석 오류: {str(e)}'})


@app.route('/api/research-summary')
def research_summary_api():
    """증권사 리포트 요약 (result 페이지에서 비동기 호출)"""
    ticker = request.args.get('ticker', '').strip()
    name   = request.args.get('name', '').strip()
    if not ticker or not name:
        return jsonify({'summary': ''})
    try:
        rr = get_research_reports(ticker)
        summary = summarize_research(name, rr)
        return jsonify({'summary': summary})
    except Exception:
        return jsonify({'summary': ''})


@app.route('/api/cache-status')
def cache_status():
    status = get_build_status()
    return jsonify(status or {'date': None, 'count': 0, 'errors': 0})


@app.route('/api/cache-refresh', methods=['POST'])
def cache_refresh():
    threading.Thread(target=_auto_build_cache, daemon=True).start()
    return jsonify({'status': 'building'})


@app.route('/api/surge-picks')
def surge_picks():
    cache = _load_surge_cache()
    rec_cache = _load_recommend_cache()
    recommend_date = rec_cache.get('scanned_at', '') if rec_cache else ''
    if cache:
        return jsonify({**cache, 'scanning': _surge_scanning, 'recommend_date': recommend_date})
    return jsonify({'date': '', 'bounce': [], 'results': [], 'pick_rec': None,
                    'pick_sup': None, 'pick_exp': None,
                    'scanning': _surge_scanning, 'recommend_date': recommend_date})


@app.route('/api/surge-refresh', methods=['POST'])
def surge_refresh():
    threading.Thread(target=_run_surge_scan, daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/ma-bounce')
def ma_bounce_page():
    """MA 반등 분석 전용 페이지 — surge_cache 볼륨에서 즉시 로드"""
    cache = _load_surge_cache()
    bounce = cache.get('bounce', []) if cache else []
    scanned_at = cache.get('scanned_at', '') if cache else ''
    bounce_list  = [r for r in bounce if r.get('type', 'bounce') != 'riding']
    riding_list  = [r for r in bounce if r.get('type') == 'riding']
    build_status = get_build_status()
    cache_days_old = 0
    if build_status:
        try:
            cache_days_old = (datetime.today() - datetime.strptime(build_status['date'], '%Y-%m-%d')).days
        except Exception:
            pass
    return render_template('ma_bounce.html',
                           bounce_list=bounce_list,
                           riding_list=riding_list,
                           scanned_at=scanned_at,
                           total=len(bounce),
                           scanning=_surge_scanning,
                           cache_days_old=cache_days_old)


@app.route('/api/ma-bounce-refresh', methods=['POST'])
def ma_bounce_refresh():
    threading.Thread(target=_run_surge_scan, daemon=True).start()
    return jsonify({'status': 'scanning'})


@app.route('/pre-surge')
def pre_surge_page():
    """급등 선취 후보 전용 페이지"""
    cache = _load_pre_surge_cache()
    results    = cache.get('results', [])   if cache else []
    scanned_at = cache.get('updated_at', '') if cache else ''
    return render_template('pre_surge.html',
                           results=results,
                           scanned_at=scanned_at,
                           scanning=_pre_surge_scanning)


@app.route('/api/search-suggest')
def search_suggest():
    q = request.args.get('q', '').strip()
    if len(q) < 1:
        return jsonify([])
    # 전종목 DB 우선, 없으면 800종목 DB
    path = _ALL_TICKER_PATH if os.path.exists(_ALL_TICKER_PATH) else _TICKER_PATH
    try:
        with open(path, encoding='utf-8') as f:
            db = json.load(f)
        q_lower = q.lower()
        matches = [
            {'name': name, 'ticker': ticker}
            for name, ticker in db.items()
            if q_lower in name.lower() or q_lower in ticker.lower()
        ][:10]
    except Exception:
        matches = []
    return jsonify(matches)


@app.route('/api/debug/dart/<ticker>')
def debug_dart(ticker):
    import os, requests as _req
    from analysis.dart import DART_API_KEY, _CORP_CACHE_PATH, get_corp_code, get_disclosures
    # 잘못된 캐시(빈 파일) 삭제
    if os.path.exists(_CORP_CACHE_PATH):
        try:
            with open(_CORP_CACHE_PATH, encoding='utf-8') as f:
                data = json.load(f)
            cache_valid = bool(data)
        except Exception:
            cache_valid = False
        if not cache_valid:
            os.remove(_CORP_CACHE_PATH)
    # DART API 원시 응답 확인
    raw_resp = None
    if DART_API_KEY:
        try:
            r = _req.get(f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}",
                         timeout=5)
            raw_resp = {'status': r.status_code, 'len': len(r.content),
                        'text': r.content.decode('utf-8', errors='replace')[:300]}
        except Exception as e:
            raw_resp = {'error': str(e)}
    corp_code = get_corp_code(ticker)
    disclosures = get_disclosures(ticker, days=30) if corp_code else []
    return jsonify({
        'key_set': bool(DART_API_KEY),
        'corp_code': corp_code,
        'disclosure_count': len(disclosures),
        'raw_api_response': raw_resp,
    })


@app.route('/api/debug/investor/<ticker>')
def debug_investor(ticker):
    """수급 데이터 디버그 v3 — frgn TD값 + pykrx 직접 테스트"""
    import requests as _req
    from bs4 import BeautifulSoup
    from pykrx import stock as _stock
    from datetime import datetime, timedelta
    out = {}

    # 1) Naver frgn 페이지 첫 데이터 행 TD 텍스트
    try:
        HEADERS = {'User-Agent': 'Mozilla/5.0'}
        res = _req.get(f"https://finance.naver.com/item/frgn.naver?code={ticker}&page=1",
                       headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.content.decode('euc-kr', errors='replace'), 'html.parser')
        for tbl in soup.find_all('table'):
            for tr in tbl.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 5:
                    first = tds[0].get_text(strip=True)
                    if len(first) == 10 and first.count('.') == 2:
                        out['frgn_td_count'] = len(tds)
                        out['frgn_td_values'] = [td.get_text(strip=True) for td in tds]
                        break
            if 'frgn_td_values' in out:
                break
    except Exception as e:
        out['frgn_error'] = str(e)

    # 2) pykrx get_market_trading_volume_by_date 직접 테스트
    try:
        end = datetime.today().strftime('%Y%m%d')
        start = (datetime.today() - timedelta(days=10)).strftime('%Y%m%d')
        df = _stock.get_market_trading_volume_by_date(start, end, ticker, on='순매수')
        out['pykrx_cols'] = list(df.columns)
        out['pykrx_rows'] = len(df)
        if not df.empty:
            out['pykrx_sample'] = df.tail(2).fillna(0).astype(int).to_dict(orient='records')
    except Exception as e:
        out['pykrx_error'] = str(e)

    return jsonify(out)


@app.route('/api/company-desc')
def company_desc():
    ticker = request.args.get('ticker', '').strip()
    name = request.args.get('name', '').strip()
    industry = request.args.get('industry', '')
    if not ticker or not name:
        return jsonify({'error': '종목 정보가 없습니다.'}), 400
    desc = get_business_description(name, industry, ticker)
    return jsonify({'desc': desc})


@app.route('/api/debug/fundamental/<ticker>')
def debug_fundamental(ticker):
    """펀더멘털 스크래핑 원시 결과 확인"""
    import requests as _req
    from bs4 import BeautifulSoup
    from analysis.fundamental import HEADERS, _decode
    out = {}

    # 1) FnGuide SVD_Main 투자지표 테이블
    try:
        main_url = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
                    f"?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=&NewMenuID=11&stkGb=701")
        res = _req.get(main_url, headers={**HEADERS, 'Referer': 'https://comp.fnguide.com/'}, timeout=8)
        soup = BeautifulSoup(res.content.decode('utf-8', errors='replace'), 'html.parser')
        rows = []
        for tbl in soup.find_all('table'):
            for tr in tbl.find_all('tr'):
                th = tr.find('th')
                tds = tr.find_all('td')
                if th and tds:
                    key = th.get_text(strip=True)
                    if any(k in key for k in ('ROE', '부채비율', '영업이익률', 'PER', 'PBR')):
                        rows.append({'key': key, 'vals': [td.get_text(strip=True) for td in tds[:5]]})
        out['fnguide_main_rows'] = rows if rows else None
    except Exception as e:
        out['fnguide_main_error'] = str(e)

    # 3) 실제 get_fundamental 결과
    try:
        out['fundamental'] = get_fundamental(ticker)
    except Exception as e:
        out['fundamental_error'] = str(e)

    return jsonify(out)


@app.route('/api/debug/reports/<ticker>')
def debug_reports(ticker):
    """증권사 리포트 목표주가 수집 확인"""
    from analysis.news import get_research_reports
    reports = get_research_reports(ticker, max_items=6)
    prices = _get_report_target_prices(reports)
    return jsonify({
        'reports': [{'firm': r['firm'], 'title': r['title'], 'url': r['url']} for r in reports],
        'target_prices': prices,
        'target_min': f"{min(prices):,}" if prices else None,
        'target_max': f"{max(prices):,}" if prices else None,
    })


@app.route('/api/debug/dart-raw/<ticker>')
def debug_dart_raw(ticker):
    """DART API 원시 응답 직접 확인 — 예: /api/debug/dart-raw/005930"""
    import requests as _req
    import time as _time
    from analysis.dart import DART_API_KEY, get_corp_code
    corp_code = get_corp_code(ticker)
    if not corp_code:
        return jsonify({'error': 'no corp_code', 'ticker': ticker, 'dart_key_set': bool(DART_API_KEY)})
    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcnt.json'
    results = {}
    for year, code, label in [('2025', '11014', 'Q3-2025'), ('2025', '11012', 'H1-2025'), ('2025', '11013', 'Q1-2025'), ('2026', '11013', 'Q1-2026')]:
        params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code, 'bsns_year': year, 'reprt_code': code}
        try:
            t0 = _time.time()
            res = _req.get(url, params=params, timeout=20)
            elapsed = round(_time.time() - t0, 2)
            data = res.json()
            items = data.get('list', [])
            revenue_items = [it for it in items if '매출' in it.get('account_nm', '') and '증감' not in it.get('account_nm', '')]
            results[label] = {
                'status_code': res.status_code,
                'elapsed_sec': elapsed,
                'dart_status': data.get('status'),
                'dart_message': data.get('message'),
                'total_items': len(items),
                'revenue_items': revenue_items[:3],
            }
        except Exception as e:
            results[label] = {'error': str(e)}
    return jsonify({'ticker': ticker, 'corp_code': corp_code, 'dart_key_set': bool(DART_API_KEY), 'results': results})


@app.route('/api/debug/export/<ticker>')
def debug_export(ticker):
    """수출주 분기 매출 데이터 fetch 테스트 — 예: /api/debug/export/005930"""
    import traceback as _tb
    from analysis.export_growth import _fetch_quarterly_revenue
    from analysis.dart import DART_API_KEY, get_corp_code
    try:
        corp_code = get_corp_code(ticker)
        if not corp_code:
            return jsonify({
                'ticker': ticker, 'corp_code': None,
                'dart_key_set': bool(DART_API_KEY),
                'error': 'corp_code를 찾을 수 없음', 'status': 'no_corp_code',
            })
        latest, yoy_rev, qoq_rev = _fetch_quarterly_revenue(corp_code, DART_API_KEY)
        yoy_growth = round((latest - yoy_rev) / yoy_rev * 100, 1) if latest and yoy_rev and yoy_rev > 0 else None
        qoq_growth = round((latest - qoq_rev) / qoq_rev * 100, 1) if latest and qoq_rev and qoq_rev > 0 else None
        return jsonify({
            'ticker': ticker,
            'corp_code': corp_code,
            'dart_key_set': bool(DART_API_KEY),
            'latest_revenue': latest,
            'yoy_revenue': yoy_rev,
            'qoq_revenue': qoq_rev,
            'yoy_growth': yoy_growth,
            'qoq_growth': qoq_growth,
            'status': 'ok' if latest else 'no_data',
        })
    except Exception as e:
        return jsonify({'ticker': ticker, 'error': str(e),
                        'traceback': _tb.format_exc(), 'status': 'error'})


@app.route('/api/chart-data/<ticker>')
def api_chart_data(ticker):
    """캔들 + MA20/60 + 자동 추세선 + 추세선 해석"""
    cached = load_stock_cache(ticker)
    ohlcv  = cached.get('ohlcv') if cached else None
    name   = cached.get('name', ticker) if cached else ticker

    if ohlcv is None or ohlcv.empty:
        try:
            ohlcv = get_ohlcv(ticker, months=6)
        except Exception:
            return jsonify({'error': 'no data'}), 404

    if ohlcv is None or len(ohlcv) < 20:
        return jsonify({'error': 'insufficient data'}), 404

    close_all = ohlcv['close']
    ma20_all  = close_all.rolling(20).mean()
    ma60_all  = close_all.rolling(60).mean()

    display = ohlcv.tail(90)
    n       = len(display)

    if hasattr(display.index, 'strftime'):
        dates = display.index.strftime('%Y-%m-%d').tolist()
    else:
        dates = [str(d)[:10] for d in display.index]

    candles, volumes = [], []
    for i, (_, row) in enumerate(display.iterrows()):
        o = int(round(float(row['open'])))
        h = int(round(float(row['high'])))
        l = int(round(float(row['low'])))
        c = int(round(float(row['close'])))
        candles.append({'time': dates[i], 'open': o, 'high': h, 'low': l, 'close': c})
        try:
            vol = int(float(row['volume']))
        except Exception:
            vol = 0
        color = 'rgba(239,68,68,0.35)' if c >= o else 'rgba(59,130,246,0.35)'
        volumes.append({'time': dates[i], 'value': vol, 'color': color})

    def _ma_pts(series, n_disp, date_list):
        vals = series.tail(n_disp)
        pts  = []
        for i, v in enumerate(vals):
            if v == v and i < len(date_list):
                pts.append({'time': date_list[i], 'value': int(round(float(v)))})
        return pts

    ma20 = _ma_pts(ma20_all, n, dates)
    ma60 = _ma_pts(ma60_all, n, dates)

    high_arr = display['high'].values.astype(float)
    low_arr  = display['low'].values.astype(float)
    PW = 2

    pivot_highs, pivot_lows = [], []
    for i in range(PW, n - PW):
        if all(high_arr[i] > high_arr[i - j] for j in range(1, PW + 1)) and \
           all(high_arr[i] > high_arr[i + j] for j in range(1, PW + 1)):
            pivot_highs.append((i, high_arr[i]))
        if all(low_arr[i] < low_arr[i - j] for j in range(1, PW + 1)) and \
           all(low_arr[i] < low_arr[i + j] for j in range(1, PW + 1)):
            pivot_lows.append((i, low_arr[i]))

    def _find_trendlines(pivots, date_list, n_total, is_resistance):
        """피벗 쌍을 모두 시도해 유효한 추세선을 최대 3개 반환"""
        if len(pivots) < 2:
            return []
        TOL = 0.018  # 1.8% 허용 오차
        candidates = []
        for a in range(len(pivots) - 1):
            for b in range(a + 1, len(pivots)):
                xi, yi = pivots[a]
                xj, yj = pivots[b]
                if xj == xi or yi == 0:
                    continue
                slope = (yj - yi) / (xj - xi)
                touches = 2
                violated = False
                for k in range(len(pivots)):
                    if k == a or k == b:
                        continue
                    xk, yk = pivots[k]
                    line_val = yi + slope * (xk - xi)
                    if line_val <= 0:
                        continue
                    diff = (yk - line_val) / line_val
                    if is_resistance:
                        if diff > TOL:
                            violated = True; break
                        elif abs(diff) <= TOL:
                            touches += 1
                    else:
                        if diff < -TOL:
                            violated = True; break
                        elif abs(diff) <= TOL:
                            touches += 1
                if not violated:
                    pts = []
                    for i in range(xi, n_total):
                        val = yi + slope * (i - xi)
                        if val > 0 and i < len(date_list):
                            pts.append({'time': date_list[i], 'value': int(round(val))})
                    if pts:
                        sp = slope / yi * 100 if yi else 0
                        direction = 'up' if sp > 0.1 else ('down' if sp < -0.1 else 'flat')
                        candidates.append({'pts': pts, 'touches': touches,
                                           'direction': direction, 'xi': xi, 'slope': slope})
        # 터치 많고 최신 피벗 우선 정렬
        candidates.sort(key=lambda c: (-c['touches'], -c['xi']))
        # 기울기 유사 중복 제거 후 최대 3개
        result = []
        for c in candidates:
            dup = any(
                abs(c['slope'] - r['slope']) / (abs(r['slope']) + 1e-9) < 0.25
                and abs(c['xi'] - r['xi']) <= 8
                for r in result
            )
            if not dup:
                result.append(c)
            if len(result) >= 3:
                break
        return result

    resist_lines = _find_trendlines(pivot_highs, dates, n, is_resistance=True)
    support_lines = _find_trendlines(pivot_lows,  dates, n, is_resistance=False)

    # 추세선 Y값을 캔들 가격 범위 내로 클리핑 (LightweightCharts Y축 팽창 방지)
    _price_lo = float(display['low'].min())
    _price_hi = float(display['high'].max())
    _margin   = (_price_hi - _price_lo) * 0.15
    def _clip(pts):
        return [p for p in pts if _price_lo - _margin <= p['value'] <= _price_hi + _margin]

    resistance_pts = _clip(resist_lines[0]['pts']) if resist_lines else []
    support_pts    = _clip(support_lines[0]['pts']) if support_lines else []
    r_dir = resist_lines[0]['direction'] if resist_lines else 'none'
    s_dir = support_lines[0]['direction'] if support_lines else 'none'

    has_r = len(resistance_pts) > 0
    has_s = len(support_pts) > 0

    interp = []
    if has_s:
        if s_dir == 'up':
            interp.append('지지선 ↗ 상향 — 저점이 높아지는 상승 흐름')
        elif s_dir == 'down':
            interp.append('지지선 ↘ 하향 — 하락 압력 지속, 지지대 약화')
        else:
            interp.append('지지선 → 수평 — 강한 지지대 형성 중')
    if has_r:
        if r_dir == 'down':
            interp.append('저항선 ↘ 하향 — 매도 압력 강함, 돌파 여부 주시')
        elif r_dir == 'up':
            interp.append('저항선 ↗ 상향 — 점진적 고점 형성, 강한 상승 패턴')
        else:
            interp.append('저항선 → 수평 — 주요 매물대 구간')
    if has_s and has_r:
        if s_dir == 'up' and r_dir == 'down':
            interp.append('★ 수렴형(쐐기) — 조만간 큰 방향 결정 예상')
        elif s_dir == 'up' and r_dir == 'up':
            interp.append('▲ 상승 채널 — 우상향 추세 유지 중')
        elif s_dir == 'down' and r_dir == 'down':
            interp.append('▼ 하락 채널 — 추세 전환 신호 확인 후 진입 권장')
        elif s_dir == 'up' and r_dir == 'flat':
            interp.append('저점 높아지며 수평 저항 근접 — 돌파 시도 주시')
        elif s_dir == 'flat' and r_dir == 'down':
            interp.append('저항선 눌려오는 형태 — 지지 여부 확인 필요')

    return jsonify({
        'name': name, 'ticker': ticker,
        'candles': candles, 'volumes': volumes,
        'ma20': ma20, 'ma60': ma60,
        'support': support_pts, 'resistance': resistance_pts,
        'support_lines':    [_clip(l['pts']) for l in support_lines],
        'resistance_lines': [_clip(l['pts']) for l in resist_lines],
        'interpretation': interp,
    })


@app.route('/api/osc-history/<ticker>')
def api_osc_history(ticker):
    """최근 60일 종합 오실레이터 히스토리 반환
    종합 = (RSI + Stoch%K + (WR+100) + clip(CCI,-200,200)+200)/4) / 4  → 0~100
    """
    try:
        ohlcv = None
        cached = load_stock_cache(ticker)
        if cached:
            ohlcv = cached.get('ohlcv')
        if ohlcv is None or ohlcv.empty or len(ohlcv) < 30:
            ohlcv = get_ohlcv(ticker, months=3)
        if ohlcv is None or ohlcv.empty or len(ohlcv) < 30:
            return jsonify({'error': 'no data'}), 404

        df = calc_indicators(ohlcv)
        df = df.tail(60)

        import numpy as np

        def _col(col, default=None):
            if col not in df.columns:
                return [default] * len(df)
            return [None if pd.isna(v) else round(float(v), 2) for v in df[col]]

        rsi_vals   = _col('rsi', 50.0)
        stoch_vals = _col('stoch_k', 50.0)
        wr_vals    = _col('williams_r', -50.0)   # -100 ~ 0
        cci_vals   = _col('cci', 0.0)             # -200 ~ +200 내외

        # 종합 오실레이터 계산 (행별로)
        composite = []
        for rsi, stoch, wr, cci in zip(rsi_vals, stoch_vals, wr_vals, cci_vals):
            if any(v is None for v in [rsi, stoch, wr, cci]):
                composite.append(None)
                continue
            wr_norm  = wr + 100                              # 0~100
            cci_norm = (max(-200.0, min(200.0, cci)) + 200) / 4  # 0~100
            comp = (rsi + stoch + wr_norm + cci_norm) / 4
            composite.append(round(comp, 1))

        dates = [str(idx)[:10] for idx in df.index]

        vol_vals = [None if pd.isna(v) else int(v) for v in df['volume']]

        data = []
        for i in range(len(df)):
            data.append({
                'composite': composite[i],
                'rsi':       rsi_vals[i],
                'stoch':     stoch_vals[i],
                'wr':        wr_vals[i],
                'cci':       cci_vals[i],
                'volume':    vol_vals[i],
            })

        return jsonify({'dates': dates, 'data': data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.errorhandler(500)
def internal_error(e):
    import traceback
    tb = traceback.format_exc()
    print(f'[500 ERROR] {tb}', flush=True)
    return f'<h1>Internal Server Error</h1><pre>{tb}</pre>', 500


if __name__ == '__main__':
    app.run(debug=True, port=5000, use_reloader=False)
