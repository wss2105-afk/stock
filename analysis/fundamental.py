import requests
from bs4 import BeautifulSoup
from pykrx import stock
from datetime import datetime, timedelta

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def get_market_profile(ticker):
    """시가총액, 52주 최고/최저, 시장구분"""
    result = {'market_cap': 'N/A', 'w52_high': 'N/A', 'w52_low': 'N/A', 'market_type': 'N/A'}
    try:
        today = datetime.today().strftime('%Y%m%d')
        w52_start = (datetime.today() - timedelta(days=365)).strftime('%Y%m%d')

        # 52주 고저 (pykrx)
        ohlcv = stock.get_market_ohlcv_by_date(w52_start, today, ticker)
        if not ohlcv.empty:
            result['w52_high'] = f"{int(ohlcv['고가'].max()):,}"
            result['w52_low'] = f"{int(ohlcv['저가'].min()):,}"
    except Exception:
        pass

    try:
        # 시가총액 & 시장구분 — 네이버 금융
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')

        # 시가총액
        cap_el = soup.select_one('em#_market_sum')
        if cap_el:
            result['market_cap'] = cap_el.text.split()[0].strip() + '억원'

        # 시장구분
        market_el = soup.select_one('em.stk_market')
        if not market_el:
            market_el = soup.select_one('span.stk_market')
        if market_el:
            txt = market_el.text.strip()
            if 'KOSPI' in txt.upper():
                result['market_type'] = 'KOSPI'
            elif 'KOSDAQ' in txt.upper():
                result['market_type'] = 'KOSDAQ'
        else:
            # 코드 범위로 추정
            result['market_type'] = 'KOSDAQ' if ticker.startswith(('0', '1')) and int(ticker) < 200000 and int(ticker) >= 100000 else 'KOSPI'
    except Exception:
        pass

    return result


def get_fundamental(ticker):
    """네이버 금융에서 PER, PBR, 영업이익, Forward PER 스크래핑"""
    result = {
        'per': 'N/A', 'forward_per': 'N/A', 'pbr': 'N/A',
        'operating_profit': [], 'eps': 'N/A',
        'roe': 'N/A', 'op_margin': 'N/A', 'debt_ratio': 'N/A',
        'revenue': [],
    }

    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')

        # PER, PBR
        table = soup.select_one('table.per_table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.text.strip()
                    val = td.text.strip().replace(',', '')
                    if 'PER' in key and 'Forward' not in key:
                        result['per'] = val
                    elif 'PBR' in key:
                        result['pbr'] = val

        # 영업이익 (연간 실적)
        fin_url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}&target=finsum_more"
        fin_res = requests.get(fin_url, headers=HEADERS, timeout=5)
        fin_soup = BeautifulSoup(fin_res.text, 'html.parser')

        table2 = fin_soup.select_one('table.tb_type1')
        if table2:
            rows = table2.find_all('tr')
            for row in rows:
                th = row.find('th')
                if th and '영업이익' in th.text:
                    tds = row.find_all('td')
                    result['operating_profit'] = [td.text.strip().replace(',', '') for td in tds[:4]]
                    break

        # ROE, 부채비율, 영업이익률
        ratio_url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}&target=finsum_more"
        ratio_res = requests.get(ratio_url, headers=HEADERS, timeout=5)
        ratio_soup = BeautifulSoup(ratio_res.text, 'html.parser')
        ratio_table = ratio_soup.select_one('table.tb_type1')
        if ratio_table:
            for row in ratio_table.find_all('tr'):
                th = row.find('th')
                if not th:
                    continue
                key = th.text.strip()
                tds = row.find_all('td')
                last_val = tds[-1].text.strip().replace(',', '') if tds else 'N/A'
                if 'ROE' in key:
                    result['roe'] = last_val
                elif '영업이익률' in key:
                    result['op_margin'] = last_val
                elif '부채비율' in key:
                    result['debt_ratio'] = last_val
                elif '매출액' in key and not result['revenue']:
                    result['revenue'] = [td.text.strip().replace(',', '') for td in tds[:4]]

        # Forward PER (컨센서스)
        consensus_url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}&target=consensus"
        con_res = requests.get(consensus_url, headers=HEADERS, timeout=5)
        con_soup = BeautifulSoup(con_res.text, 'html.parser')

        con_table = con_soup.select_one('table.tb_type1')
        if con_table:
            rows = con_table.find_all('tr')
            for row in rows:
                th = row.find('th')
                if th and 'PER' in th.text:
                    tds = row.find_all('td')
                    if tds:
                        result['forward_per'] = tds[-1].text.strip().replace(',', '')
                    break

    except Exception as e:
        print(f"펀더멘털 수집 오류: {e}")

    return result
