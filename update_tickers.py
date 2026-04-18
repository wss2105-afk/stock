"""
KOSPI 시총 상위 200 + KOSDAQ 시총 상위 150 종목 DB 업데이트
실행: python update_tickers.py
"""
import requests
from bs4 import BeautifulSoup
import json
import os

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
OUTPUT = os.path.join(os.path.dirname(__file__), 'data', 'krx_tickers.json')


def fetch_naver_rank(market='KOSPI', top_n=200):
    """네이버 금융 시총 상위 종목 수집"""
    sosok = 0 if market == 'KOSPI' else 1
    tickers = {}
    page = 1
    while len(tickers) < top_n:
        url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}'
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        rows = soup.select('table.type_2 tbody tr')
        found = 0
        for row in rows:
            tds = row.find_all('td')
            if len(tds) < 2:
                continue
            a = tds[1].find('a')
            if not a:
                continue
            name = a.text.strip()
            href = a.get('href', '')
            if 'code=' not in href:
                continue
            code = href.split('code=')[1][:6]
            if name and code and code not in tickers.values():
                tickers[name] = code
                found += 1
        if found == 0:
            break
        page += 1

    # 상위 top_n개만
    return dict(list(tickers.items())[:top_n])


def update():
    print('KOSPI 시총 상위 200 수집 중...')
    kospi = fetch_naver_rank('KOSPI', top_n=200)
    print(f'  → {len(kospi)}개 수집')

    print('KOSDAQ 시총 상위 150 수집 중...')
    kosdaq = fetch_naver_rank('KOSDAQ', top_n=150)
    print(f'  → {len(kosdaq)}개 수집')

    # 합산 (중복 코드 제거)
    combined = {}
    seen_codes = set()
    for name, code in {**kospi, **kosdaq}.items():
        if code not in seen_codes:
            combined[name] = code
            seen_codes.add(code)

    with open(OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    print(f'\n완료: 총 {len(combined)}개 종목 저장 → {OUTPUT}')
    print(f'KOSPI 200 + KOSDAQ 150 = {len(kospi) + len(kosdaq)}개 (중복 제외 {len(combined)}개)')


if __name__ == '__main__':
    update()
