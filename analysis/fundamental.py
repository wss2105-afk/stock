import requests
from bs4 import BeautifulSoup


HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def get_fundamental(ticker):
    """네이버 금융에서 PER, PBR, 영업이익, Forward PER 스크래핑"""
    result = {
        'per': 'N/A', 'forward_per': 'N/A', 'pbr': 'N/A',
        'operating_profit': [], 'eps': 'N/A'
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
