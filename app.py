from flask import Flask, render_template, request, jsonify
from analysis.screener import scan_top_stocks, scan_supply_leaders, scan_surge_stocks, scan_ma_bounce_stocks, scan_osc_stocks
from analysis.export_growth import load_cache as load_export_cache, scan_export_growth, is_new_update
from analysis.data_fetcher import get_ticker, get_ohlcv, get_investor_detail, get_supply_zone, is_main_stock, get_today_price, append_today
from analysis.cache_manager import load_stock_cache, build_all_cache, is_build_needed, get_build_status
from analysis.indicators import calc_indicators, get_ma_arrangement, get_latest_signals
from analysis.fundamental import get_fundamental
from analysis.news import search_naver_news, analyze_news, get_research_reports
from analysis.signal import calc_score, get_recommendation, get_ai_analysis, get_business_description, summarize_research
from analysis.charts import make_main_chart, make_supply_zone_chart, make_investor_chart, make_ma_chart
from analysis.dart import get_disclosures, get_company_info
from analysis.fundamental import get_market_profile
from dotenv import load_dotenv
import os
import json
import threading
from datetime import datetime, timedelta

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=True)
app = Flask(__name__)

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


_SURGE_CACHE_PATH     = os.path.join(os.path.dirname(__file__), 'data', 'surge_cache.json')
_OSC_CACHE_PATH       = os.path.join(os.path.dirname(__file__), 'data', 'osc_cache.json')
_RECOMMEND_CACHE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'recommend_cache.json')

def _load_surge_cache():
    if not os.path.exists(_SURGE_CACHE_PATH):
        return None
    try:
        with open(_SURGE_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def _run_surge_scan():
    """MA 반등 종목 + 급등 종목 스캔 후 surge_cache 저장"""
    global _surge_scanning
    _surge_scanning = True
    today = datetime.today().strftime('%Y-%m-%d')
    scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
    try:
        bounce = scan_ma_bounce_stocks(top_n=20)
        try:
            surge = scan_surge_stocks(top_n=10)
        except Exception:
            surge = []
        try:
            top_rec = scan_top_stocks(top_n=1, months=3)
            pick_rec = top_rec[0] if top_rec else None
        except Exception:
            pick_rec = None
        try:
            top_sup = scan_supply_leaders(months=2)
            pick_sup = top_sup[0] if top_sup else None
        except Exception:
            pick_sup = None
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
                'pick_sup':   pick_sup,
                'pick_exp':   pick_exp,
            }, f, ensure_ascii=False)
        print(f'[{scanned_at}] 반등/급등 스캔 완료 — 반등:{len(bounce)}건, 급등:{len(surge)}건')
    except Exception as e:
        print(f'반등/급등 스캔 오류: {e}')
    finally:
        _surge_scanning = False


def _evening_scheduler():
    """매일 20:00 반등/급등 스캔 (앱 시작 시 자동 스캔 없음 — 캐시 표시만)"""
    import time as _time
    while True:
        now = datetime.today()
        next_run = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        _time.sleep((next_run - now).total_seconds())
        _run_surge_scan()


def _market_osc_scheduler():
    """장중 11:00, 13:00 과매도/과매수 스캔 (평일만, 앱 시작 시 캐시 없으면 1회 즉시 실행)"""
    import time as _time
    if _load_osc_cache() is None:
        threading.Thread(target=_run_osc_scan, daemon=True).start()

    while True:
        now = datetime.today()
        # 다음 11:00 또는 13:00 계산
        candidates = []
        for h in (11, 13):
            t = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if t > now:
                candidates.append(t)
        next_run = min(candidates) if candidates else (now + timedelta(days=1)).replace(
            hour=11, minute=0, second=0, microsecond=0)
        # 주말이면 다음 월요일로
        while next_run.weekday() >= 5:
            next_run += timedelta(days=1)
        _time.sleep((next_run - now).total_seconds())
        # 평일(월~금)만 실행
        if datetime.today().weekday() < 5:
            _run_osc_scan()


threading.Thread(target=_evening_scheduler, daemon=True).start()
threading.Thread(target=_market_osc_scheduler, daemon=True).start()

_EXPORT_SCAN_PATH = os.path.join(os.path.dirname(__file__), 'data', 'export_scan_month.txt')

def _run_export_scan():
    """수출주 스캔 실행 후 완료 월 기록"""
    this_month = datetime.today().strftime('%Y-%m')
    try:
        scan_export_growth(growth_threshold=10)
        with open(_EXPORT_SCAN_PATH, 'w') as f:
            f.write(this_month)
        print(f'[{this_month}] 수출주 자동 스캔 완료')
    except Exception as e:
        print(f'수출주 스캔 오류: {e}')

def _export_scan_scheduler():
    """매월 15일 06:00 수출주 자동 스캔 (루프 방식)"""
    import time as _time
    # 앱 시작 시 이번 달 15일 스캔이 아직 안 됐으면 즉시 실행
    today = datetime.today()
    this_month = today.strftime('%Y-%m')
    already_done = False
    if os.path.exists(_EXPORT_SCAN_PATH):
        with open(_EXPORT_SCAN_PATH) as f:
            already_done = f.read().strip() == this_month
    if not already_done and today.day >= 15:
        _run_export_scan()

    while True:
        now = datetime.today()
        # 다음 15일 06:00 KST(= UTC-9) 계산
        year, month = now.year, now.month
        if now.day > 15 or (now.day == 15 and now.hour >= 6):
            month += 1
            if month > 12:
                month = 1
                year += 1
        next_run = now.replace(year=year, month=month, day=15, hour=6, minute=0, second=0, microsecond=0)
        _time.sleep((next_run - now).total_seconds())
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


def _load_osc_cache():
    if not os.path.exists(_OSC_CACHE_PATH):
        return None
    try:
        with open(_OSC_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


_osc_scanning = False
_surge_scanning = False

def _run_osc_scan():
    """과매도/과매수 스캔 실행 후 캐시 저장"""
    global _osc_scanning
    _osc_scanning = True
    try:
        result = scan_osc_stocks(top_n=30)
        now = datetime.today().strftime('%Y-%m-%d %H:%M')
        with open(_OSC_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'updated_at': now, **result}, f, ensure_ascii=False)
        print(f'[{now}] 과매도/과매수 스캔 완료 — 과매도:{len(result["oversold"])} 과매수:{len(result["overbought"])}')
    except Exception as e:
        print(f'과매도/과매수 스캔 오류: {e}')
    finally:
        _osc_scanning = False




# ── 추천 종목 TOP 20 일일 캐시 ───────────────────────────────────
def _load_recommend_cache():
    if not os.path.exists(_RECOMMEND_CACHE_PATH):
        return None
    try:
        with open(_RECOMMEND_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _run_recommend_scan():
    """추천 종목 20선 스캔 후 캐시 저장"""
    try:
        results = scan_top_stocks(top_n=20, months=6)
        today = datetime.today().strftime('%Y-%m-%d')
        scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
        with open(_RECOMMEND_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'date': today, 'scanned_at': scanned_at, 'results': results}, f,
                      ensure_ascii=False)
        print(f'[{scanned_at}] 추천 종목 스캔 완료 — {len(results)}건')
    except Exception as e:
        print(f'추천 종목 스캔 오류: {e}')


def _recommend_scheduler():
    """매일 07:00 추천 종목 자동 스캔 (앱 시작 시 당일 캐시 없으면 즉시 실행)"""
    import time as _time
    # 앱 시작 시 오늘 캐시 없으면 즉시 실행
    cache = _load_recommend_cache()
    today = datetime.today().strftime('%Y-%m-%d')
    if not cache or cache.get('date') != today:
        _run_recommend_scan()

    # 이후 매일 07:00 실행
    while True:
        now = datetime.today()
        next_run = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        sleep_sec = (next_run - now).total_seconds()
        _time.sleep(sleep_sec)
        _run_recommend_scan()


threading.Thread(target=_recommend_scheduler, daemon=True).start()


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
        today_info  = get_today_price(ticker)
        ohlcv_full  = append_today(ohlcv_full, today_info)
        ohlcv       = ohlcv_full.tail(max(months * 22, 60))
        investor_df = cached['investor_df']
        supply_df   = cached['supply_df']
        fundamental = cached['fundamental']

        # 나머지는 병렬 fetch
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
            try:
                rr = get_research_reports(ticker)
                return rr, summarize_research(name, rr)
            except: return [], ""
        def _disclosures():
            try: return get_disclosures(ticker, days=60)
            except: return []

        with ThreadPoolExecutor(max_workers=5) as ex:
            f_co = ex.submit(_company)
            f_pr = ex.submit(_profile)
            f_nw = ex.submit(_news)
            f_rr = ex.submit(_research)
            f_dc = ex.submit(_disclosures)
            company_info   = f_co.result()
            market_profile = f_pr.result()
            news_result    = f_nw.result()
            research_reports, research_summary = f_rr.result()
            disclosures    = f_dc.result()
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
            try:
                rr = get_research_reports(ticker)
                return rr, summarize_research(name, rr)
            except: return [], ""
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
            research_reports, research_summary = f_rr.result()
            disclosures    = f_dc.result()

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
    score, reasons = calc_score(ma_status, signals, investor_df, news_result)
    recommendation, rec_color = get_recommendation(score)
    score_pct = max(0, min(100, round((score + 14) / 28 * 100)))

    # AI 분석 — 별도 API로 지연 로딩 (페이지 속도 개선)
    ai_comment = None

    # 차트
    main_chart = make_main_chart(df, name)
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

    return render_template('result.html',
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
        ai_comment=ai_comment,
        main_chart=main_chart,
        supply_chart=supply_chart,
        investor_chart=investor_chart
    )


@app.route('/recommend')
def recommend():
    cache = _load_recommend_cache()
    if cache:
        results    = cache.get('results', [])
        scanned_at = cache.get('scanned_at', '')
    else:
        # 캐시 없으면 실시간 스캔 (첫 접속 시만)
        results    = scan_top_stocks(top_n=20, months=6)
        scanned_at = datetime.today().strftime('%Y-%m-%d %H:%M')
    return render_template('recommend.html', results=results, scanned_at=scanned_at)


@app.route('/supply-leaders')
def supply_leaders():
    results = scan_supply_leaders(months=3)
    return render_template('supply_leaders.html', results=results)


@app.route('/export-surge')
def export_surge():
    cache = load_export_cache()
    if cache is None:
        threading.Thread(target=scan_export_growth, daemon=True).start()
        return render_template('export_surge.html', high=[], moderate=[], scanning=True,
                               updated_at=None, total=0, high_count=0, moderate_count=0)
    results = cache.get('results', [])
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


@app.route('/api/ai-comment')
def ai_comment_api():
    """AI 분석을 별도로 요청 (result 페이지에서 비동기 호출)"""
    ticker = request.args.get('ticker', '').strip()
    name   = request.args.get('name', '').strip()
    if not ticker or not name:
        return jsonify({'comment': 'ticker/name 파라미터가 필요합니다.'})
    try:
        import pandas as pd
        ohlcv = get_ohlcv(ticker, months=3)
        if ohlcv.empty:
            return jsonify({'comment': '데이터를 불러올 수 없습니다.'})
        df = calc_indicators(ohlcv)
        ma_status   = get_ma_arrangement(df)
        signals     = get_latest_signals(df)
        try: investor_df = get_investor_detail(ticker, months=1)
        except: investor_df = pd.DataFrame()
        try:
            articles = search_naver_news(name, days=30)
            news_result = analyze_news(articles)
        except: news_result = {'total':0,'positive':0,'negative':0,'neutral':0,'sentiment_score':0,'top_keywords':[],'press_counts':{},'exclusive_count':0,'articles':[]}
        try: fundamental = get_fundamental(ticker)
        except: fundamental = {'per':'N/A','forward_per':'N/A','pbr':'N/A','operating_profit':[],'roe':'N/A','op_margin':'N/A','debt_ratio':'N/A','revenue':[]}
        score, reasons = calc_score(ma_status, signals, investor_df, news_result)
        comment = get_ai_analysis(name, score, reasons, signals, fundamental, news_result)
        return jsonify({'comment': comment})
    except Exception as e:
        return jsonify({'comment': f'AI 분석 오류: {str(e)}'})


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


if __name__ == '__main__':
    app.run(debug=True, port=5000)
