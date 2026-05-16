"""
매출 고성장 수출주 스캐너
- DART API 분기 재무제표 매출액으로 YoY / QoQ 성장률 계산
- 30%↑ (강한 성장) / 10~30% (양호한 성장) 2단계 분류
- 결과를 data/export_cache.json 에 캐시
"""
import os
import json
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

_DATA_DIR    = '/data' if os.path.isdir('/data') else os.path.join(os.path.dirname(__file__), '..', 'data')
_CACHE_PATH  = os.path.join(_DATA_DIR, 'export_cache.json')
_LOCAL_DATA  = os.path.join(os.path.dirname(__file__), '..', 'data')
# 볼륨에 종목 DB 없으면 번들 파일 fallback
_TICKER_PATH = (os.path.join(_DATA_DIR, 'krx_tickers.json')
                if os.path.exists(os.path.join(_DATA_DIR, 'krx_tickers.json'))
                else os.path.join(_LOCAL_DATA, 'krx_tickers.json'))

# DART 분기 코드 순서 (최신→구형), (연도오프셋, 보고서코드, 분기명)
_QUARTER_ORDER = [
    (0,  '11013', 'Q1'),
    (-1, '11014', 'Q3'),
    (-1, '11012', 'Q2'),
    (-1, '11013', 'Q1'),
    (-2, '11014', 'Q3'),
    (-2, '11012', 'Q2'),
    (-2, '11013', 'Q1'),
]

# 분기 YoY 짝 (현재분기코드 → 전년동기코드)
_YOY_MAP = {
    '11013': '11013',  # Q1 → Q1 작년
    '11012': '11012',  # Q2 → Q2 작년
    '11014': '11014',  # Q3 → Q3 작년
}

# 분기 QoQ 짝 (현재 → 직전, 연도오프셋 차이)
_QOQ_MAP = {
    '11013': (-1, '11014'),  # Q1 → Q3 전년도
    '11012': (0,  '11013'),  # Q2 → Q1 같은해
    '11014': (0,  '11012'),  # Q3 → Q2 같은해
}


def _dart_revenue(corp_code, dart_key, year, reprt_code):
    """DART API로 특정 분기 매출액 반환. 없으면 None."""
    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcnt.json'
    params = {'crtfc_key': dart_key, 'corp_code': corp_code,
              'bsns_year': str(year), 'reprt_code': reprt_code}
    try:
        res = requests.get(url, params=params, timeout=5)
        if res.status_code != 200:
            return None
        items = res.json().get('list', [])
        for it in items:
            nm = it.get('account_nm', '')
            if '매출' in nm and '증감' not in nm and '성장' not in nm:
                raw = it.get('thstrm_amount', '').replace(',', '').strip()
                if raw and raw not in ('-', ''):
                    try:
                        return int(raw)
                    except ValueError:
                        pass
    except Exception:
        pass
    return None


def _fetch_quarterly_revenue(corp_code, dart_key):
    """최근 분기부터 시도해 (최신분기, 전년동기, 직전분기) 3개 값 반환."""
    now = datetime.now()
    base_year = now.year

    latest_rev = None
    latest_year = None
    latest_code = None

    for yr_offset, code, _ in _QUARTER_ORDER:
        yr = base_year + yr_offset
        rev = _dart_revenue(corp_code, dart_key, yr, code)
        if rev and rev > 0:
            latest_rev = rev
            latest_year = yr
            latest_code = code
            break

    if not latest_rev:
        return None, None, None

    # YoY: 전년 동분기
    yoy_rev = None
    if latest_code in _YOY_MAP:
        yoy_rev = _dart_revenue(corp_code, dart_key, latest_year - 1, _YOY_MAP[latest_code])

    # QoQ: 직전 분기
    qoq_rev = None
    if latest_code in _QOQ_MAP:
        yr_delta, prev_code = _QOQ_MAP[latest_code]
        qoq_rev = _dart_revenue(corp_code, dart_key, latest_year + yr_delta, prev_code)

    return latest_rev, yoy_rev, qoq_rev


def _get_current_price(ticker):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(res.text, 'html.parser')
        el = soup.select_one('p.no_today em span.blind')
        if el:
            return el.text.strip().replace(',', '')
    except Exception:
        pass
    return 'N/A'


def _scan_one(name, ticker, corp_code, dart_key, growth_threshold, high_threshold):
    try:
        latest, yoy, qoq = _fetch_quarterly_revenue(corp_code, dart_key)
        if not latest or latest <= 0:
            return None

        yoy_growth = round((latest - yoy) / yoy * 100, 1) if yoy and yoy > 0 else None
        qoq_growth = round((latest - qoq) / qoq * 100, 1) if qoq and qoq > 0 else None

        max_growth = max(
            yoy_growth if yoy_growth is not None else -999,
            qoq_growth if qoq_growth is not None else -999,
        )
        if max_growth < growth_threshold:
            return None

        tier  = 'high' if max_growth >= high_threshold else 'moderate'
        price = _get_current_price(ticker)

        return {
            'ticker':          ticker,
            'name':            name,
            'price':           price,
            'latest_revenue':  latest,
            'yoy_growth':      yoy_growth,
            'qoq_growth':      qoq_growth,
            'max_growth':      max_growth,
            'tier':            tier,
            'yoy_high':  yoy_growth is not None and yoy_growth >= high_threshold,
            'qoq_high':  qoq_growth is not None and qoq_growth >= high_threshold,
            'yoy_mod':   yoy_growth is not None and yoy_growth >= growth_threshold,
            'qoq_mod':   qoq_growth is not None and qoq_growth >= growth_threshold,
            'is_yoy':    yoy_growth is not None and yoy_growth >= high_threshold,
            'is_qoq':    qoq_growth is not None and qoq_growth >= high_threshold,
        }
    except Exception:
        return None


def scan_export_growth(growth_threshold=10, high_threshold=30, max_stocks=800):
    """분기 매출 YoY/QoQ 성장 종목 스캔 (DART API)"""
    from analysis.dart import DART_API_KEY, get_corp_code

    if not DART_API_KEY:
        print('[수출주 스캔] DART_API_KEY 없음 — 스캔 불가')
        _save_cache([], growth_threshold)
        return []

    if not os.path.exists(_TICKER_PATH):
        return []

    with open(_TICKER_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    ticker_list = list(tickers.items())[:max_stocks]

    # corp_code 캐시 로드
    corp_map = {}
    for name, ticker in ticker_list:
        code = get_corp_code(ticker)
        if code:
            corp_map[ticker] = code

    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {
            executor.submit(
                _scan_one, name, ticker, corp_map[ticker],
                DART_API_KEY, growth_threshold, high_threshold
            ): ticker
            for name, ticker in ticker_list
            if ticker in corp_map
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: x['max_growth'], reverse=True)
    _save_cache(results, growth_threshold)
    return results


def _save_cache(results, growth_threshold):
    high_list     = [r for r in results if r['tier'] == 'high']
    moderate_list = [r for r in results if r['tier'] == 'moderate']
    cache = {
        'updated_at':     datetime.now().strftime('%Y-%m-%d %H:%M'),
        'updated_ts':     time.time(),
        'count':          len(results),
        'high_count':     len(high_list),
        'moderate_count': len(moderate_list),
        'growth_threshold': growth_threshold,
        'results':        results,
    }
    os.makedirs(_DATA_DIR, exist_ok=True)
    with open(_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    print(f'[수출주 스캔 완료] high:{len(high_list)} moderate:{len(moderate_list)} / 전체:{len(results)}')


def load_cache():
    if not os.path.exists(_CACHE_PATH):
        return None
    try:
        with open(_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def is_new_update(hours=48):
    cache = load_cache()
    if not cache:
        return False
    age = time.time() - cache.get('updated_ts', 0)
    return age < hours * 3600
