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
        ticker = db[query]
        return ticker, query

    # 부분 일치
    for name, ticker in db.items():
        if query in name or name in query:
            return ticker, name

    return None, None


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

    price_min = ohlcv['low'].min()
    price_max = ohlcv['high'].max()
    bins = pd.interval_range(start=price_min, end=price_max, periods=30)

    zone = pd.Series(0.0, index=bins)
    for _, row in ohlcv.iterrows():
        avg = (row['high'] + row['low']) / 2
        for b in bins:
            if avg in b:
                zone[b] += row['volume']
                break

    zone_df = pd.DataFrame({
        'price_mid': [round((b.left + b.right) / 2) for b in zone.index],
        'volume': zone.values
    })
    return zone_df
