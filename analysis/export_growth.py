"""
매출 고성장 수출주 스캐너
- Naver Finance 분기 매출액 데이터로 성장률 계산
- 30%↑ (강한 성장) / 10~30% (양호한 성장) 2단계 분류
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

        tables = soup.select('table.tb_type1')
        target_table = None
        for t in tables:
            ths = [th.text.strip() for th in t.select('th')]
            header_text = ' '.join(ths)
            if 'Q' in header_text or '분기' in header_text:
                target_table = t
                break

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
                return vals  # [최신분기, 전분기, 2분기전, 3분기전(=전년동기)]
    except Exception:
        pass
    return []


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


def scan_export_growth(growth_threshold=10, high_threshold=30, max_stocks=800):
    """
    전년동기대비(YoY) / 전분기대비(QoQ) 매출 성장률 스캔
    - high  tier: max_growth >= 30% (강한 성장)
    - moderate tier: 10% <= max_growth < 30% (양호한 성장)
    """
    if not os.path.exists(_TICKER_PATH):
        return []

    with open(_TICKER_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    results = []

    for name, ticker in list(tickers.items())[:max_stocks]:
        try:
            rev = _fetch_quarterly_revenue(ticker)
            if len(rev) < 4:
                continue

            latest, prev_q, q2, q3 = rev[0], rev[1], rev[2], rev[3]

            if latest <= 0:
                continue

            # 전년동기대비 (3분기 전과 비교)
            yoy_growth = round((latest - q3) / q3 * 100, 1) if q3 > 0 else None
            # 전분기대비
            qoq_growth = round((latest - prev_q) / prev_q * 100, 1) if prev_q > 0 else None

            max_growth = max(
                yoy_growth if yoy_growth is not None else -999,
                qoq_growth if qoq_growth is not None else -999,
            )

            if max_growth < growth_threshold:
                continue

            tier = 'high' if max_growth >= high_threshold else 'moderate'
            price = _get_current_price(ticker)

            results.append({
                'ticker': ticker,
                'name': name,
                'price': price,
                'latest_revenue': int(latest),
                'yoy_growth': yoy_growth,
                'qoq_growth': qoq_growth,
                'max_growth': max_growth,
                'tier': tier,
                # 30% 기준 달성 여부
                'yoy_high': yoy_growth is not None and yoy_growth >= high_threshold,
                'qoq_high': qoq_growth is not None and qoq_growth >= high_threshold,
                # 10% 기준 달성 여부
                'yoy_mod': yoy_growth is not None and yoy_growth >= growth_threshold,
                'qoq_mod': qoq_growth is not None and qoq_growth >= growth_threshold,
                # 하위 호환
                'is_yoy': yoy_growth is not None and yoy_growth >= high_threshold,
                'is_qoq': qoq_growth is not None and qoq_growth >= high_threshold,
            })

        except Exception:
            continue

        time.sleep(0.15)

    results.sort(key=lambda x: x['max_growth'], reverse=True)

    high_list = [r for r in results if r['tier'] == 'high']
    moderate_list = [r for r in results if r['tier'] == 'moderate']

    cache = {
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'updated_ts': time.time(),
        'count': len(results),
        'high_count': len(high_list),
        'moderate_count': len(moderate_list),
        'results': results,
    }
    os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
    with open(_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    return results


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
