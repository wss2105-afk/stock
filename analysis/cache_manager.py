"""
주식 데이터 일별 캐시 관리자
- 매일 6시 KST에 350종목 OHLCV·수급·펀더멘털·매물대를 미리 수집
- 검색 시 캐시 로드 + 오늘 현재가 1회만 호출 → 빠른 응답
"""
import os
import pickle
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

_BASE_DIR   = '/data' if os.path.isdir('/data') else os.path.join(os.path.dirname(__file__), '..', 'data')
_CACHE_DIR  = os.path.join(_BASE_DIR, 'cache')
_TICKER_DB  = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_tickers.json')
_BUILD_FLAG = os.path.join(_BASE_DIR, 'cache_built.txt')

os.makedirs(_CACHE_DIR, exist_ok=True)

# ── 캐시 유효성 ────────────────────────────────────────────
def _cache_path(ticker):
    return os.path.join(_CACHE_DIR, f'{ticker}.pkl')


def is_cache_valid(ticker):
    path = _cache_path(ticker)
    if not os.path.exists(path):
        return False
    try:
        mtime = os.path.getmtime(path)
        age_hours = (datetime.now() - datetime.fromtimestamp(mtime)).total_seconds() / 3600
        # 주말(토·일)에는 금요일 캐시를 월요일 개장 전까지 유효 처리 (최대 72시간)
        weekday = datetime.now().weekday()  # 0=월 … 5=토 6=일
        max_age = 72 if weekday in (5, 6) else 30
        return age_hours < max_age
    except Exception:
        return False


def load_stock_cache(ticker):
    """캐시 데이터 반환. 오늘 캐시가 없으면 None."""
    if not is_cache_valid(ticker):
        return None
    try:
        with open(_cache_path(ticker), 'rb') as f:
            return pickle.load(f)
    except Exception:
        return None


def save_stock_cache(ticker, data):
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(_cache_path(ticker), 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)


# ── 개별 종목 캐시 빌드 ────────────────────────────────────
def _build_one(name, ticker):
    try:
        import pandas as pd
        from analysis.data_fetcher import get_ohlcv, get_investor_detail, get_supply_zone
        from analysis.fundamental import get_fundamental

        ohlcv = get_ohlcv(ticker, months=6)
        if ohlcv.empty or len(ohlcv) < 20:
            return False

        try:
            investor_df = get_investor_detail(ticker, months=3)
        except Exception:
            investor_df = pd.DataFrame()

        try:
            fundamental = get_fundamental(ticker)
        except Exception:
            fundamental = {
                'per': 'N/A', 'forward_per': 'N/A', 'pbr': 'N/A',
                'roe': 'N/A', 'op_margin': 'N/A', 'debt_ratio': 'N/A',
                'operating_profit': [], 'revenue': [],
            }

        try:
            supply_df = get_supply_zone(ticker, months=6)
        except Exception:
            supply_df = pd.DataFrame({'price_mid': [], 'volume': []})

        save_stock_cache(ticker, {
            'name':        name,
            'ohlcv':       ohlcv,
            'investor_df': investor_df,
            'fundamental': fundamental,
            'supply_df':   supply_df,
        })
        return True
    except Exception:
        return False


# ── 전체 캐시 빌드 ─────────────────────────────────────────
def build_all_cache(max_workers=6):
    """350종목 전체 캐시 빌드. 평균 3-5분 소요."""
    with open(_TICKER_DB, encoding='utf-8') as f:
        tickers = json.load(f)

    count = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_build_one, name, ticker): ticker
            for name, ticker in tickers.items()
        }
        for future in as_completed(futures):
            try:
                if future.result():
                    count += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

    today = datetime.today().strftime('%Y-%m-%d')
    with open(_BUILD_FLAG, 'w') as f:
        f.write(f'{today},{count},{errors}')

    return count, errors


def is_build_needed():
    """오늘 이미 전체 빌드를 했으면 False"""
    if not os.path.exists(_BUILD_FLAG):
        return True
    try:
        with open(_BUILD_FLAG) as f:
            line = f.read().strip()
        built_date = line.split(',')[0]
        return built_date != datetime.today().strftime('%Y-%m-%d')
    except Exception:
        return True


def get_build_status():
    """마지막 캐시 빌드 정보 반환"""
    if not os.path.exists(_BUILD_FLAG):
        return None
    try:
        with open(_BUILD_FLAG) as f:
            parts = f.read().strip().split(',')
        return {
            'date':   parts[0],
            'count':  int(parts[1]) if len(parts) > 1 else 0,
            'errors': int(parts[2]) if len(parts) > 2 else 0,
        }
    except Exception:
        return None
