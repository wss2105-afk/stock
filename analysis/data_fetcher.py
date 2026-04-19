from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import json
import os

_TICKER_DB_PATH     = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_tickers.json')
_ALL_TICKER_DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_all_tickers.json')
_ticker_db     = None
_all_ticker_db = None


def _load_ticker_db():
    global _ticker_db
    if _ticker_db is None:
        with open(_TICKER_DB_PATH, encoding='utf-8') as f:
            _ticker_db = json.load(f)
    return _ticker_db


def _load_all_ticker_db():
    """전종목 DB (KOSPI + KOSDAQ 전체, 검색·기업소개용)"""
    global _all_ticker_db
    if _all_ticker_db is None:
        path = _ALL_TICKER_DB_PATH
        if not os.path.exists(path):
            # 전종목 DB가 없으면 기존 800종목 DB로 폴백
            return _load_ticker_db()
        with open(path, encoding='utf-8') as f:
            _all_ticker_db = json.load(f)
    return _all_ticker_db


def get_date_range(months=3):
    end = datetime.today()
    start = end - timedelta(days=months * 30)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def get_ticker(name_or_ticker):
    """종목명 또는 티커 코드로 (ticker, name) 반환 — 전종목 DB 우선 검색"""
    all_db = _load_all_ticker_db()
    query  = name_or_ticker.strip()
    query_lower = query.lower()

    # 6자리 코드 직접 입력
    if query.isdigit() and len(query) == 6:
        # 전종목 DB에서 역방향 조회
        for name, code in all_db.items():
            if code == query:
                return code, name
        # pykrx 폴백
        try:
            name = stock.get_market_ticker_name(query)
            if name:
                return query, name
        except Exception:
            pass

    # 완전 일치
    for name, ticker in all_db.items():
        if name.lower() == query_lower:
            return ticker, name

    # 부분 일치 (짧은 이름 우선)
    candidates = [(name, ticker) for name, ticker in all_db.items()
                  if query_lower in name.lower()]
    if candidates:
        candidates.sort(key=lambda x: len(x[0]))
        return candidates[0][1], candidates[0][0]

    return None, None


def is_main_stock(ticker):
    """분석 대상 종목 여부 (KOSPI 500 + KOSDAQ 300 = 800개)"""
    db = _load_ticker_db()
    return ticker in db.values()


def get_today_price(ticker):
    """오늘 종가(또는 현재가)만 빠르게 조회 — 캐시 사용 시 현재가 갱신용"""
    # 1차: pykrx 오늘 하루치
    try:
        today = datetime.today().strftime('%Y%m%d')
        df = stock.get_market_ohlcv_by_date(today, today, ticker)
        if not df.empty:
            row = df.iloc[-1]
            return {
                'date':   df.index[-1],
                'open':   int(row.get('시가', row.iloc[0])),
                'high':   int(row.get('고가', row.iloc[1])),
                'low':    int(row.get('저가', row.iloc[2])),
                'close':  int(row.get('종가', row.iloc[3])),
                'volume': int(row.get('거래량', row.iloc[4])),
                'amount': int(row.get('거래대금', row.iloc[5]) if len(row) > 5 else 0),
            }
    except Exception:
        pass

    # 2차: Naver 현재가 스크래핑
    try:
        import requests
        from bs4 import BeautifulSoup
        url = f'https://finance.naver.com/item/main.naver?code={ticker}'
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        text = res.content.decode('euc-kr', errors='replace')
        soup = BeautifulSoup(text, 'html.parser')
        el = soup.find('strong', id='_nowVal') or soup.select_one('p.no_today em')
        if el:
            price = int(el.get_text(strip=True).replace(',', '').split('\n')[0])
            return {'date': pd.Timestamp.today().normalize(), 'close': price,
                    'open': price, 'high': price, 'low': price, 'volume': 0, 'amount': 0}
    except Exception:
        pass

    return None


def append_today(ohlcv_df, today_info):
    """캐시된 OHLCV에 오늘 행을 추가 (이미 있으면 갱신)"""
    if today_info is None:
        return ohlcv_df
    row = pd.DataFrame([{
        'open': today_info['open'], 'high': today_info['high'],
        'low':  today_info['low'],  'close': today_info['close'],
        'volume': today_info['volume'], 'amount': today_info['amount'],
    }], index=[pd.Timestamp(today_info['date'])])
    df = ohlcv_df.copy()
    if row.index[0] in df.index:
        df.loc[row.index[0]] = row.iloc[0]
    else:
        df = pd.concat([df, row])
    return df


def get_ohlcv(ticker, months=3):
    start, end = get_date_range(months)
    df = stock.get_market_ohlcv_by_date(start, end, ticker)
    df.index = pd.to_datetime(df.index)
    df.columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
    return df


def get_investor_detail(ticker, months=3):
    """연기금/금융투자 포함 상세 수급 (pykrx → Naver 폴백)"""
    start, end = get_date_range(months)
    # 1차: pykrx
    try:
        df = stock.get_market_trading_volume_by_date(start, end, ticker)
        if not df.empty:
            df.index = pd.to_datetime(df.index)
            return df
    except Exception:
        pass
    # 2차: Naver Finance 스크래핑 (외국인·기관 순매수)
    return _get_investor_naver(ticker, months)


def _parse_naver_num(s):
    s = s.replace(',', '').replace('+', '').strip()
    try:
        return int(s)
    except Exception:
        return 0


def _get_investor_naver(ticker, months=3):
    """Naver Finance frgn 페이지에서 외국인·기관 순매수 스크래핑"""
    import requests
    from bs4 import BeautifulSoup
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    records = []
    page = 1
    limit = months * 22

    while len(records) < limit:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}&page={page}"
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            text = res.content.decode('euc-kr', errors='replace')
            soup = BeautifulSoup(text, 'html.parser')

            # 날짜 패턴(YYYY.MM.DD)이 있는 행을 포함한 테이블 자동 탐색
            target_table = None
            for tbl in soup.find_all('table'):
                rows = tbl.find_all('tr')
                for tr in rows:
                    tds = tr.find_all('td')
                    if tds and len(tds) >= 5:
                        first = tds[0].get_text(strip=True)
                        if len(first) == 10 and first.count('.') == 2:
                            target_table = tbl
                            break
                if target_table:
                    break

            if target_table is None:
                break

            found = False
            for tr in target_table.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) < 5:
                    continue
                date_str = tds[0].get_text(strip=True)
                if len(date_str) != 10 or date_str.count('.') != 2:
                    continue

                # frgn.naver 컬럼: 날짜|종가|전일비|거래량|외국인순매수|보유주수|지분율|기관순매수
                # (일부 페이지는 컬럼이 달라질 수 있으므로 마지막 숫자 컬럼 탐지)
                if len(tds) >= 8:
                    foreign_net = _parse_naver_num(tds[4].get_text(strip=True))
                    inst_net    = _parse_naver_num(tds[7].get_text(strip=True))
                elif len(tds) >= 7:
                    foreign_net = _parse_naver_num(tds[4].get_text(strip=True))
                    inst_net    = _parse_naver_num(tds[6].get_text(strip=True))
                elif len(tds) >= 5:
                    foreign_net = _parse_naver_num(tds[3].get_text(strip=True))
                    inst_net    = 0
                else:
                    continue

                records.append({
                    'date': date_str,
                    '외국인합계': foreign_net,
                    '기관합계':   inst_net,
                })
                found = True

            if not found:
                break
            page += 1
        except Exception:
            break

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'], format='%Y.%m.%d', errors='coerce')
    df = df.dropna(subset=['date']).set_index('date').sort_index()
    return df


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
