"""
매출 고성장 수출주 스캐너
- FnGuide 분기 손익계산서 매출액으로 YoY / QoQ 성장률 계산
- 30%↑ (강한 성장) / 10~30% (양호한 성장) 2단계 분류
- 결과를 /data/export_cache.json 에 캐시
"""
import os
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://comp.fnguide.com/',
}
_DATA_DIR    = os.path.join(os.path.dirname(__file__), '..', 'data')
_CACHE_PATH  = os.environ.get('EXPORT_CACHE_PATH',
               '/data/export_cache.json' if os.path.isdir('/data') else
               os.path.join(_DATA_DIR, 'export_cache.json'))
_TICKER_PATH = os.path.join(_DATA_DIR, 'krx_tickers.json')


def _fetch_quarterly_revenue(ticker):
    """FnGuide 분기 손익계산서에서 매출액 최근 4분기 반환 [최신→구형 순서]"""
    url = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp"
           f"?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=Q&NewMenuID=103&stkGb=701")
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            return []
        soup = BeautifulSoup(res.content.decode('utf-8', errors='replace'), 'html.parser')

        for row in soup.find_all('tr'):
            # th 또는 첫 번째 td에서 레이블 추출
            label_el = row.find('th') or (row.find_all('td')[0] if row.find_all('td') else None)
            if not label_el:
                continue
            label_txt = label_el.get_text(separator=' ', strip=True)
            # '매출액' 포함 AND 증감률/성장률 행은 제외
            if '매출액' not in label_txt:
                continue
            if any(x in label_txt for x in ('증감', '성장', '률', '비율')):
                continue

            all_tds = row.find_all('td')
            start = 0 if row.find('th') else 1
            vals = []
            for td in all_tds[start:]:
                txt = td.get_text(separator='', strip=True).replace(',', '').strip()
                if not txt or txt in ('-', '--', 'N/A', '해당없음'):
                    continue
                try:
                    vals.append(float(txt))
                except ValueError:
                    continue
            if len(vals) >= 2:
                # FnGuide 왼→오(구→신) 순서, 최대 4분기
                take = vals[-4:] if len(vals) >= 4 else vals
                return list(reversed(take))
    except Exception as e:
        print(f"[export_growth] {ticker} 매출 fetch 오류: {e}")
    return []


def _get_current_price(ticker):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers={'User-Agent': HEADERS['User-Agent']}, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        el = soup.select_one('#_nowVal')
        if el:
            return el.text.strip().replace(',', '')
    except Exception:
        pass
    return 'N/A'


def _scan_one(name, ticker, growth_threshold, high_threshold):
    try:
        rev = _fetch_quarterly_revenue(ticker)
        if len(rev) < 2:
            return None

        latest = rev[0]
        prev_q = rev[1] if len(rev) >= 2 else None
        q3     = rev[3] if len(rev) >= 4 else None

        if latest <= 0:
            return None

        yoy_growth = round((latest - q3) / q3 * 100, 1) if q3 and q3 > 0 else None
        qoq_growth = round((latest - prev_q) / prev_q * 100, 1) if prev_q and prev_q > 0 else None

        max_growth = max(
            yoy_growth if yoy_growth is not None else -999,
            qoq_growth if qoq_growth is not None else -999,
        )
        if max_growth < growth_threshold:
            return None

        tier  = 'high' if max_growth >= high_threshold else 'moderate'
        price = _get_current_price(ticker)

        return {
            'ticker': ticker,
            'name': name,
            'price': price,
            'latest_revenue': int(latest),
            'yoy_growth': yoy_growth,
            'qoq_growth': qoq_growth,
            'max_growth': max_growth,
            'tier': tier,
            'yoy_high': yoy_growth is not None and yoy_growth >= high_threshold,
            'qoq_high': qoq_growth is not None and qoq_growth >= high_threshold,
            'yoy_mod':  yoy_growth is not None and yoy_growth >= growth_threshold,
            'qoq_mod':  qoq_growth is not None and qoq_growth >= growth_threshold,
            'is_yoy':   yoy_growth is not None and yoy_growth >= high_threshold,
            'is_qoq':   qoq_growth is not None and qoq_growth >= high_threshold,
        }
    except Exception:
        return None


def scan_export_growth(growth_threshold=10, high_threshold=30, max_stocks=800):
    """전년동기(YoY) / 전분기(QoQ) 매출 성장률 병렬 스캔"""
    if not os.path.exists(_TICKER_PATH):
        return []

    with open(_TICKER_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    ticker_list = list(tickers.items())[:max_stocks]
    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_scan_one, name, ticker, growth_threshold, high_threshold): name
            for name, ticker in ticker_list
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: x['max_growth'], reverse=True)

    high_list     = [r for r in results if r['tier'] == 'high']
    moderate_list = [r for r in results if r['tier'] == 'moderate']

    cache = {
        'updated_at':     datetime.now().strftime('%Y-%m-%d %H:%M'),
        'updated_ts':     time.time(),
        'count':          len(results),
        'high_count':     len(high_list),
        'moderate_count': len(moderate_list),
        'results':        results,
    }
    os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
    with open(_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    print(f"[수출주 스캔 완료] high:{len(high_list)} moderate:{len(moderate_list)} / 전체:{len(results)}")
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
