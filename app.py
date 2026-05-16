from flask import Flask, render_template, request, jsonify
from analysis.screener import scan_top_stocks, scan_supply_leaders, scan_surge_stocks, scan_ma_bounce_stocks, scan_osc_stocks, scan_buy_candidates, scan_surge_buy_candidates, get_scan_progress
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
    """매월 1일 종목 DB 자동 갱신 (스캔용 800 + 전종목 4000+)"""
    today = datetime.today()
    if today.day != 1:
        return
    last = ''
    if os.path.exists(_LAST_UPDATE_PATH):
        with open(_LAST_UPDATE_PATH) as f:
            last = f.read().strip()
    this_month = today.strftime('%Y-%m')
    if last == this_month:
        return
    try:
        from update_tickers import update
        update()
        with open(_LAST_UPDATE_PATH, 'w') as f:
            f.write(this_month)
        print(f'[{this_month}] 종목 DB 자동 갱신 완료')
    except Exception as e:
        print(f'종목 DB 갱신 오류: {e}')


threading.Thread(target=_auto_update_tickers, daemon=True).start()


_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(_DATA_DIR, exist_ok=True)

_SURGE_CACHE_PATH          = os.path.join(_DATA_DIR, 'surge_cache.json')
_OSC_CACHE_PATH            = os.path.join(_DATA_DIR, 'osc_cache.json')
_RECOMMEND_CACHE_PATH      = os.path.join(_DATA_DIR, 'recommend_cache.json')
_SUPPLY_CACHE_PATH         = os.path.join(_DATA_DIR, 'supply_cache.json')
_BUY_CANDIDATE_CACHE_PATH  = os.path.join(_DATA_DIR, 'buy_candidate_cache.json')
_SURGE_BUY_CACHE_PATH      = os.path.join(_DATA_DIR, 'surge_buy_cache.json')

def _load_surge_cache():
    if not os.path.exists(_SURGE_CACHE_PATH):
        return None
    try:
        with open(_SURGE_CACHE_PATH, encoding='utf-8') as f:
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
    """매일 10:00 UTC(=19:00 KST) 반등/급등 스캔 — 캐시 없으면 시작 시 1회 즉시 스캔"""
    import time as _time
    if _load_surge_cache() is None:
        threading.Thread(target=_run_surge_scan, daemon=True).start()

    while True:
        now = datetime.today()
        next_run = now.replace(hour=10, minute=0, second=0, microsecond=0)
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
    """매일 캐시가 없으면 350종목 전체 데이터 사전 수집"""
    if not is_build_needed():
        return
    today = datetime.today().strftime('%Y-%m-%d')
    print(f'[{today}] 전체 종목 캐시 빌드 시작...')
    try:
        count, errors = build_all_cache(max_workers=6)
        print(f'[{today}] 캐시 완료: {count}건 성공, {errors}건 실패')
    except Exception as e:
        print(f'캐시 빌드 오류: {e}')

threading.Thread(target=_auto_build_cache, daemon=True).start()


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
    """매일 07:00 추천 종목 자동 스캔
    - 앱 시작 시 캐시 없으면 1회 백그라운드 스캔 (재배포 후 빈 화면 방지)
    - 페이지 방문 시 스캔 없음 — 캐시만 표시
    """
    import time as _time
    if _load_recommend_cache() is None:
        threading.Thread(target=_run_recommend_scan, daemon=True).start()

    while True:
        now = datetime.today()
        next_run = now.replace(hour=7, minute=0, second=0, microsecond=0)
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
    """평일 09:30 / 14:00 수급주도 종목 스캔
    - 캐시 없으면 앱 시작 시 1회 백그라운드 스캔
    - 페이지 방문 시 스캔 없음
    """
    import time as _time
    if _load_supply_cache() is None:
        threading.Thread(target=_run_supply_scan, daemon=True).start()

    while True:
        now = datetime.today()
        candidates = []
        for h, m in ((9, 30), (14, 0)):
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
    """매일 07:30 매수후보(단기) 자동 스캔 — 캐시 없으면 시작 시 1회 즉시 스캔"""
    import time as _time
    if _load_buy_candidate_cache() is None:
        threading.Thread(target=_run_buy_candidate_scan, daemon=True).start()

    while True:
        now = datetime.today()
        next_run = now.replace(hour=7, minute=30, second=0, microsecond=0)
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
    """평일 07:30 UTC(=16:30 KST) 급등주 매수후보 자동 스캔 (장 마감 후 당일 데이터 반영)"""
    import time as _time
    while True:
        now = datetime.today()
        next_run = now.replace(hour=7, minute=30, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        while next_run.weekday() >= 5:
            next_run += timedelta(days=1)
        _time.sleep((next_run - now).total_seconds())
        if datetime.today().weekday() < 5:
            _run_surge_buy_scan()


threading.Thread(target=_surge_buy_scheduler, daemon=True).start()


# ── 교차 종목 탐지 + 텔레그램 알림 ─────────────────────────────
import requests as _req_tg

_TG_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
_TG_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
_alerted_today: set = set()   # 당일 이미 알림 보낸 ticker 집합
_alerted_date: str  = ''      # 날짜 바뀌면 초기화용


def _find_cross_picks():
    """6개 스캔 결과에서 2개 이상 중복 종목 반환 [{ticker, name, scans, reasons}]"""
    sources: dict = {}

    def _add(key, ticker, name, reasons):
        sources.setdefault(key, []).append((ticker, name, reasons))

    try:
        c = _load_recommend_cache()
        for r in (c.get('results', []) if c else []):
            _add('추천종목', r['ticker'], r['name'], r.get('reasons', []))
    except Exception:
        pass

    try:
        c = _load_supply_cache()
        for r in (c.get('results', []) if c else []):
            rs = []
            if r.get('foreign_streak', 0) >= 3:
                rs.append(f"외인 {r['foreign_streak']}일 연속 매수")
            if r.get('inst_streak', 0) >= 3:
                rs.append(f"기관 {r['inst_streak']}일 연속 매수")
            _add('수급주도', r['ticker'], r['name'], rs)
    except Exception:
        pass

    try:
        c = _load_osc_cache()
        for r in (c.get('oversold', []) if c else []):
            _add('과매도', r['ticker'], r['name'], [f"과매도 점수 {r.get('score', '')}"])
    except Exception:
        pass

    try:
        c = _load_surge_cache()
        for r in (c.get('bounce', []) if c else []):
            _add('MA반등', r['ticker'], r['name'],
                 [f"{r.get('ma_label','')} 반등 (눌림 {r.get('pullback_pct','')}%)"])
    except Exception:
        pass

    try:
        c = _load_buy_candidate_cache()
        for r in (c.get('results', []) if c else []):
            _add('매수후보단기', r['ticker'], r['name'], r.get('reasons', []))
    except Exception:
        pass

    try:
        c = _load_surge_buy_cache()
        for r in (c.get('results', []) if c else []):
            _add('급등주매수후보', r['ticker'], r['name'], r.get('reasons', []))
    except Exception:
        pass

    ticker_map: dict = {}
    for scan_key, entries in sources.items():
        for ticker, name, reasons in entries:
            if ticker not in ticker_map:
                ticker_map[ticker] = {'ticker': ticker, 'name': name, 'scans': [], 'reasons': []}
            ticker_map[ticker]['scans'].append(scan_key)
            for r in reasons:
                if r and r not in ticker_map[ticker]['reasons']:
                    ticker_map[ticker]['reasons'].append(r)

    return [v for v in ticker_map.values() if len(v['scans']) >= 2]


def _send_telegram(text: str):
    """텔레그램 메시지 발송"""
    if not _TG_TOKEN or not _TG_CHAT_ID:
        print('[TG] TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 미설정 — 스킵')
        return
    try:
        url  = f'https://api.telegram.org/bot{_TG_TOKEN}/sendMessage'
        resp = _req_tg.post(url, json={'chat_id': _TG_CHAT_ID, 'text': text,
                                       'parse_mode': 'HTML'}, timeout=10)
        if resp.ok:
            print(f'[TG] 발송 완료')
        else:
            print(f'[TG] 발송 실패: {resp.text}')
    except Exception as e:
        print(f'[TG] 발송 오류: {e}')


def _send_cross_alert(picks: list):
    """공통 종목 텔레그램 알림"""
    if not picks:
        return
    now_kst = (datetime.utcnow() + timedelta(hours=9)).strftime('%m/%d %H:%M')
    lines = [f'📊 <b>복수 스캔 공통 종목</b> ({now_kst} KST)\n']
    for p in picks:
        scans_str   = ' · '.join(p['scans'])
        reasons_str = '\n'.join(f'  · {r}' for r in p['reasons'][:4]) if p['reasons'] else '  · —'
        lines.append(f"<b>{p['name']}</b> ({p['ticker']})\n"
                     f"  📌 {scans_str}\n"
                     f"{reasons_str}")
    lines.append('\n⚠️ 투자 판단은 본인 책임입니다.')
    _send_telegram('\n\n'.join(lines))


def _cross_alert_scheduler():
    """매 1시간 체크 — 07:00~20:00 KST 구간에 공통 종목 발견 시 텔레그램 알림"""
    import time as _time
    global _alerted_today, _alerted_date

    _time.sleep(120)   # 앱 시작 직후 실행 방지

    while True:
        now_kst   = datetime.utcnow() + timedelta(hours=9)
        today_str = now_kst.strftime('%Y-%m-%d')

        if today_str != _alerted_date:
            _alerted_today = set()
            _alerted_date  = today_str

        if 7 <= now_kst.hour < 20:
            try:
                picks     = _find_cross_picks()
                new_picks = [p for p in picks if p['ticker'] not in _alerted_today]
                if new_picks:
                    _send_cross_alert(new_picks)
                    for p in new_picks:
                        _alerted_today.add(p['ticker'])
            except Exception as e:
                print(f'[CrossAlert] 교차 탐지 오류: {e}')

        _time.sleep(3600)


threading.Thread(target=_cross_alert_scheduler, daemon=True).start()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/analyze', methods=['GET', 'POST'])
def analyze():
    if request.method == 'GET':
        query = request.args.get('query', '').strip()
        months = int(request.args.get('months', 3))
        if not query:
            return render_template('index.html')
    else:
        query = request.form.get('query', '').strip()
        months = int(request.form.get('months', 3))

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
    ma_chart   = make_ma_chart(df, name)
    # 캔들차트와 동일한 y축 범위를 매물대에 전달 → 가격대 완전 일치
    chart_y_min = float(df['low'].min())  * 0.97
    chart_y_max = float(df['high'].max()) * 1.03
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
    return render_template('buy_candidates.html',
                           results=results,
                           scanned_at=scanned_at,
                           scanning=_buy_candidate_scanning)


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
                           moderate_count=len(moderate))


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
    return jsonify({'status': 'scanning', 'message': '모든 스캔 시작됨 — 완료까지 30~60분 소요'})


@app.route('/api/scan-progress')
def scan_progress():
    """전체 스캔 진행률 폴링 엔드포인트"""
    return jsonify(_build_progress_response())


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


_ALL_TICKER_PATH = os.path.join(os.path.dirname(__file__), 'data', 'krx_all_tickers.json')

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


@app.route('/api/debug/export/<ticker>')
def debug_export(ticker):
    """수출주 분기 매출 데이터 fetch 테스트 — 예: /api/debug/export/005930"""
    from analysis.export_growth import _fetch_quarterly_revenue
    try:
        rev = _fetch_quarterly_revenue(ticker)
        if len(rev) >= 4:
            latest, prev_q, q2, q3 = rev[0], rev[1], rev[2], rev[3]
            yoy = round((latest - q3) / q3 * 100, 1) if q3 > 0 else None
            qoq = round((latest - prev_q) / prev_q * 100, 1) if prev_q > 0 else None
        else:
            yoy = qoq = None
        return jsonify({
            'ticker': ticker,
            'revenue_quarters': rev,
            'count': len(rev),
            'yoy_growth': yoy,
            'qoq_growth': qoq,
            'status': 'ok' if len(rev) >= 4 else 'data_insufficient',
        })
    except Exception as e:
        return jsonify({'ticker': ticker, 'error': str(e), 'status': 'error'})


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

    def _trendline(pivots, date_list, n_total):
        if len(pivots) < 2:
            return [], 'none'
        x1, y1 = pivots[-2]
        x2, y2 = pivots[-1]
        if x2 == x1 or y1 == 0:
            return [], 'none'
        slope = (y2 - y1) / (x2 - x1)
        slope_pct = (y2 - y1) / y1 * 100 / (x2 - x1)
        direction = 'up' if slope_pct > 0.1 else ('down' if slope_pct < -0.1 else 'flat')
        pts = []
        for i in range(x1, n_total):
            val = y1 + slope * (i - x1)
            if val > 0 and i < len(date_list):
                pts.append({'time': date_list[i], 'value': int(round(val))})
        return pts, direction

    resistance_pts, r_dir = _trendline(pivot_highs, dates, n)
    support_pts,    s_dir = _trendline(pivot_lows,  dates, n)

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
        'interpretation': interp,
    })


@app.route('/api/osc-history/<ticker>')
def api_osc_history(ticker):
    """최근 60일 오실레이터(RSI·Stoch·MFI·BB%) 히스토리 반환"""
    try:
        cached = load_stock_cache(ticker)
        if not cached:
            return jsonify({'error': 'no cache'}), 404
        ohlcv = cached.get('ohlcv')
        if ohlcv is None or ohlcv.empty or len(ohlcv) < 30:
            return jsonify({'error': 'no data'}), 404

        df = calc_indicators(ohlcv)
        df = df.tail(60)

        def _s(col, scale=1.0, default=50.0):
            if col not in df.columns:
                return [default] * len(df)
            return [
                round(float(v) * scale, 1) if not pd.isna(v) else None
                for v in df[col]
            ]

        dates = [str(idx)[:10] for idx in df.index]
        return jsonify({
            'dates': dates,
            'rsi':   _s('rsi'),
            'stoch': _s('stoch_k'),
            'mfi':   _s('mfi'),
            'bb':    _s('bb_pct', scale=100.0, default=50.0),
        })
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
