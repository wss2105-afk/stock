"""
매출 고성장 수출주 스캐너
- Naver Finance 분기 매출액 데이터로 전년동기대비 30%+ 증가 종목 탐지
- 결과를 data/export_cache.json에 캐시
"""
import os
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
_CACHE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'export_cache.json')
_TICKER_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_tickers.json')

# 수출 비중이 높은 업종 키워드
EXPORT_SECTORS = [
    '반도체', '전자', '디스플레이', '배터리', '2차전지', '자동차', '조선',
    '철강', '석유화학', '화학', '기계', '방산', '항공', '의약품', '바이오',
    '전기차', '부품', '소재', '수출',
]


def _fetch_quarterly_revenue(ticker):
    """Naver Finance에서 분기별 매출액 4개 분기 반환 (최신순)"""
    url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}&target=finsum_more"
    try:
        res = requests.get(url, headers=HEADERS, timeout=6)
        soup = BeautifulSoup(res.text, 'html.parser')

        # 분기 탭 테이블 (두 번째 table.tb_type1 이 분기)
        tables = soup.select('table.tb_type1')
        target_table = None
        for t in tables:
            ths = [th.text.strip() for th in t.select('th')]
            # 분기 테이블은 헤더에 'Q' 또는 '분기' 포함
            header_text = ' '.join(ths)
            if 'Q' in header_text or '분기' in header_text:
                target_table = t
                break

        # fallback: 첫 번째 테이블
        if not target_table and tables:
            target_table = tables[0]

        if not target_table:
            return []

        for row in target_table.find_all('tr'):
            th = row.find('th')
            if th and '매출액' in th.text:
                tds = row.find_all('td')
                vals = []
                for td in tds[:4]:
                    txt = td.text.strip().replace(',', '').replace('-', '0')
                    try:
                        vals.append(float(txt) if txt else 0)
                    except ValueError:
                        vals.append(0)
                return vals  # [최신분기, 전분기, 2분기전, 3분기전]
    except Exception:
        pass
    return []


def _get_sector_tag(name):
    """종목명에서 수출 업종 태그 추출"""
    for kw in EXPORT_SECTORS:
        if kw in name:
            return kw
    return ''


def _get_company_sector_from_naver(ticker):
    """Naver Finance에서 업종 정보 스크래핑"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        sector_el = soup.select_one('.sub_section .info_group .coinfo_tit')
        if not sector_el:
            # 다른 선택자 시도
            for span in soup.select('em, span, td'):
                txt = span.text.strip()
                for kw in EXPORT_SECTORS:
                    if kw in txt and len(txt) < 30:
                        return txt
        return sector_el.text.strip() if sector_el else ''
    except Exception:
        return ''


def _get_current_price(ticker):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        el = soup.select_one('#_nowVal')
        if el:
            return el.text.strip().replace(',', '')
    except Exception:
        pass
    return 'N/A'


def scan_export_growth(growth_threshold=30, max_stocks=350):
    """
    전년동기대비 OR 전분기대비 매출 30%+ 증가 수출주 스캔
    결과 dict 반환 및 캐시 저장
    """
    if not os.path.exists(_TICKER_PATH):
        return []

    with open(_TICKER_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    results = []
    count = 0

    for name, ticker in list(tickers.items())[:max_stocks]:
        count += 1
        try:
            rev = _fetch_quarterly_revenue(ticker)
            if len(rev) < 4:
                continue

            latest, prev_q, q2, q3 = rev[0], rev[1], rev[2], rev[3]

            if latest <= 0:
                continue

            yoy_growth = None
            qoq_growth = None

            # 전년동기대비 (4분기 전과 비교)
            if q3 > 0:
                yoy_growth = round((latest - q3) / q3 * 100, 1)

            # 전분기대비
            if prev_q > 0:
                qoq_growth = round((latest - prev_q) / prev_q * 100, 1)

            max_growth = max(
                yoy_growth if yoy_growth is not None else -999,
                qoq_growth if qoq_growth is not None else -999,
            )

            if max_growth < growth_threshold:
                continue

            price = _get_current_price(ticker)

            results.append({
                'ticker': ticker,
                'name': name,
                'price': price,
                'latest_revenue': int(latest),
                'yoy_growth': yoy_growth,
                'qoq_growth': qoq_growth,
                'max_growth': max_growth,
                'is_yoy': yoy_growth is not None and yoy_growth >= growth_threshold,
                'is_qoq': qoq_growth is not None and qoq_growth >= growth_threshold,
            })

        except Exception:
            continue

        # 과도한 요청 방지
        time.sleep(0.15)

    results.sort(key=lambda x: x['max_growth'], reverse=True)

    cache = {
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'updated_ts': time.time(),
        'count': len(results),
        'results': results,
    }
    os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
    with open(_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    return results


def load_cache():
    """캐시된 결과 로드. 없으면 None 반환"""
    if not os.path.exists(_CACHE_PATH):
        return None
    try:
        with open(_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def is_new_update(hours=48):
    """최근 N시간 내 업데이트 여부"""
    cache = load_cache()
    if not cache:
        return False
    age = time.time() - cache.get('updated_ts', 0)
    return age < hours * 3600
