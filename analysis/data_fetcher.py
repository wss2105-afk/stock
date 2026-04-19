from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import json
import os

_TICKER_DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_tickers.json')
_ticker_db = None


def _load_ticker_db():
    global _ticker_db
    if _ticker_db is None:
        with open(_TICKER_DB_PATH, encoding='utf-8') as f:
            _ticker_db = json.load(f)
    return _ticker_db


def get_date_range(months=3):
    end = datetime.today()
    start = end - timedelta(days=months * 30)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def get_ticker(name_or_ticker):
    """종목명 또는 티커 코드로 (ticker, name) 반환"""
    db = _load_ticker_db()

    # 6자리 숫자 코드로 직접 입력한 경우
    if name_or_ticker.isdigit() and len(name_or_ticker) == 6:
        try:
            name = stock.get_market_ticker_name(name_or_ticker)
            if name:
                return name_or_ticker, name
        except Exception:
            pass

    # 종목명으로 검색 (부분 일치 포함)
    query = name_or_ticker.strip()
    # 완전 일치
    if query in db:
        return db[query], query

    # 부분 일치: query가 DB 종목명 안에 포함된 경우만 허용
    # (name in query는 제외 — "카카오"가 "카카오페이" 쿼리에 매칭되는 오류 방지)
    candidates = [(name, ticker) for name, ticker in db.items() if query in name]
    if candidates:
        # 가장 짧은 이름(가장 유사한 것) 선택
        candidates.sort(key=lambda x: len(x[0]))
        return candidates[0][1], candidates[0][0]

    # DART 전체 상장사 폴백 검색 (비 KOSPI200/KOSDAQ150 종목)
    try:
        from analysis.dart import search_ticker_by_name
        ticker, name = search_ticker_by_name(query)
        if ticker:
            return ticker, name
    except Exception:
        pass

    return None, None


def is_main_stock(ticker):
    """KOSPI200 + KOSDAQ150 로컬 DB 포함 종목 여부"""
    db = _load_ticker_db()
    return ticker in db.values()


def get_ohlcv(ticker, months=3):
    start, end = get_date_range(months)
    df = stock.get_market_ohlcv_by_date(start, end, ticker)
    df.index = pd.to_datetime(df.index)
    df.columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
    return df


def get_investor_detail(ticker, months=3):
    """연기금/금융투자 포함 상세 수급"""
    start, end = get_date_range(months)
    try:
        df = stock.get_market_trading_volume_by_date(start, end, ticker)
        if df.empty:
            return pd.DataFrame()
        df.index = pd.to_datetime(df.index)
        return df
    except Exception:
        return pd.DataFrame()


def get_supply_zone(ticker, months=6):
    """매물대: 가격대별 거래량 분포"""
    start, end = get_date_range(months)
    ohlcv = stock.get_market_ohlcv_by_date(start, end, ticker)
    ohlcv.columns = ['open', 'high', 'low', 'close', 'volume', 'amount']

    if ohlcv.empty:
        return pd.DataFrame({'price_mid': [], 'volume': []})

    price_min = ohlcv['low'].min()
    price_max = ohlcv['high'].max()

    # 가격 범위가 너무 좁으면 현재가 기준으로 확장
    if price_max - price_min < price_min * 0.01:
        price_min = price_min * 0.97
        price_max = price_max * 1.03

    n_bins = 25
    ohlcv['avg_price'] = (ohlcv['high'] + ohlcv['low']) / 2

    bins = pd.cut(ohlcv['avg_price'], bins=n_bins)
    zone = ohlcv.groupby(bins, observed=False)['volume'].sum()

    zone_df = pd.DataFrame({
        'price_mid': [round((b.left + b.right) / 2) for b in zone.index],
        'volume': zone.values
    })

    # 거래량 0인 구간 제거 후 가격순 정렬
    zone_df = zone_df[zone_df['volume'] > 0].sort_values('price_mid').reset_index(drop=True)
    return zone_df
