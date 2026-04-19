"""
종목 DB 업데이트
- krx_tickers.json     : KOSPI 시총 상위 500 + KOSDAQ 시총 상위 300 (스캔·분석용)
- krx_all_tickers.json : KOSPI + KOSDAQ 전종목 (검색·기업소개용)
실행: python update_tickers.py
"""
import requests
from bs4 import BeautifulSoup
import json
import os

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
DATA_DIR    = os.path.join(os.path.dirname(__file__), 'data')
OUTPUT_MAIN = os.path.join(DATA_DIR, 'krx_tickers.json')
OUTPUT_ALL  = os.path.join(DATA_DIR, 'krx_all_tickers.json')


def fetch_naver_market(market='KOSPI', top_n=None):
    """
    네이버 금융 시총 순위 페이지에서 종목 수집.
    top_n=None 이면 전체 페이지 수집.
    """
    sosok = 0 if market == 'KOSPI' else 1
    tickers = {}
    page = 1
    while True:
        url = (f'https://finance.naver.com/sise/sise_market_sum.naver'
               f'?sosok={sosok}&page={page}')
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            rows = soup.select('table.type_2 tbody tr')
        except Exception:
            break

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
        if top_n and len(tickers) >= top_n:
            break
        page += 1

    if top_n:
        return dict(list(tickers.items())[:top_n])
    return tickers


def _merge(a, b):
    combined = {}
    seen = set()
    for name, code in {**a, **b}.items():
        if code not in seen:
            combined[name] = code
            seen.add(code)
    return combined


def update():
    os.makedirs(DATA_DIR, exist_ok=True)

    # ── 스캔용 DB (상위 800) ──
    print('KOSPI 시총 상위 500 수집 중...')
    kospi_main = fetch_naver_market('KOSPI', top_n=500)
    print(f'  → {len(kospi_main)}개')

    print('KOSDAQ 시총 상위 300 수집 중...')
    kosdaq_main = fetch_naver_market('KOSDAQ', top_n=300)
    print(f'  → {len(kosdaq_main)}개')

    main_db = _merge(kospi_main, kosdaq_main)
    with open(OUTPUT_MAIN, 'w', encoding='utf-8') as f:
        json.dump(main_db, f, ensure_ascii=False, indent=2)
    print(f'krx_tickers.json 저장: {len(main_db)}개')

    # ── 전종목 DB (검색용) ──
    print('\nKOSPI 전종목 수집 중...')
    kospi_all = fetch_naver_market('KOSPI')
    print(f'  → {len(kospi_all)}개')

    print('KOSDAQ 전종목 수집 중...')
    kosdaq_all = fetch_naver_market('KOSDAQ')
    print(f'  → {len(kosdaq_all)}개')

    all_db = _merge(kospi_all, kosdaq_all)
    with open(OUTPUT_ALL, 'w', encoding='utf-8') as f:
        json.dump(all_db, f, ensure_ascii=False, indent=2)
    print(f'krx_all_tickers.json 저장: {len(all_db)}개')

    print(f'\n완료: 스캔용 {len(main_db)}개 / 전종목 {len(all_db)}개')


if __name__ == '__main__':
    update()
