import requests
from bs4 import BeautifulSoup
from pykrx import stock
from datetime import datetime, timedelta

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def get_company_info_naver(ticker):
    """Naver Finance coinfo 페이지에서 기업 기본정보 스크래핑"""
    result = {}
    try:
        url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=6)
        soup = BeautifulSoup(res.content.decode('euc-kr', errors='replace'), 'html.parser')

        # 여러 테이블 셀렉터 시도
        table = (soup.select_one('table.coinfo_t1') or
                 soup.select_one('table.tb_type1') or
                 soup.find('table', attrs={'summary': lambda s: s and '기업' in s}))

        if table:
            rows = table.find_all('tr')
            for row in rows:
                ths = row.find_all('th')
                tds = row.find_all('td')
                for i, th in enumerate(ths):
                    if i >= len(tds):
                        continue
                    key = th.get_text(strip=True)
                    td  = tds[i]
                    val = td.get_text(strip=True)
                    if '업종' in key and 'industry' not in result:
                        result['industry'] = val
                    elif '대표자' in key and 'ceo' not in result:
                        result['ceo'] = val
                    elif '결산' in key and 'fiscal_month' not in result:
                        result['fiscal_month'] = val if val else 'N/A'
                    elif '설립일' in key and 'founded' not in result:
                        result['founded'] = val.replace('.', '').replace('-', '') if val else ''
                    elif '홈페이지' in key and 'website' not in result:
                        a_tag = td.find('a')
                        result['website'] = a_tag['href'] if a_tag else val
                    elif ('상장일' in key or '상장' in key) and 'listing_date' not in result:
                        result['listing_date'] = val.replace('.', '').replace('-', '') if val else ''
                    elif '직원' in key and 'employees' not in result:
                        result['employees'] = val
                    elif '자본금' in key and 'capital' not in result:
                        result['capital'] = val
                    elif ('주요제품' in key or '주요 제품' in key or '사업' in key) and 'products' not in result:
                        result['products'] = val[:80] if val else ''

        # 업종이 없으면 main 페이지 업종 링크에서 추출 (main.naver는 UTF-8)
        if 'industry' not in result:
            main_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            main_res = requests.get(main_url, headers=HEADERS, timeout=5)
            main_soup = BeautifulSoup(main_res.content.decode('utf-8', errors='replace'), 'html.parser')
            for a in main_soup.find_all('a', href=True):
                if 'upjong' in a['href'] or 'type=upjong' in a['href']:
                    industry_text = a.get_text(strip=True)
                    if industry_text:
                        result['industry'] = industry_text
                        break

        # 상장일·직원수·자본금이 없으면 KRX 정보 페이지 시도
        if not result.get('listing_date') or not result.get('employees'):
            try:
                krx_url = f"https://kind.krx.co.kr/corpgeneral/corpList.do?method=searchCorpList&currentPageSize=5&pageIndex=1&comAbbrv={ticker}"
                kr = requests.get(krx_url, headers=HEADERS, timeout=5)
                ks = BeautifulSoup(kr.content.decode('utf-8', errors='replace'), 'html.parser')
                for td in ks.find_all('td'):
                    txt = td.get_text(strip=True)
                    if len(txt) == 8 and txt.isdigit() and 'listing_date' not in result:
                        result['listing_date'] = txt
            except Exception:
                pass

    except Exception:
        pass
    return result


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
        # 시가총액 & 시장구분 — 네이버 금융 (main.naver는 UTF-8)
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.content.decode('utf-8', errors='replace'), 'html.parser')

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
            result['market_type'] = 'KOSDAQ' if ticker.startswith(('0', '1')) and int(ticker) < 200000 and int(ticker) >= 100000 else 'KOSPI'
    except Exception:
        pass

    return result


def _decode(res):
    """네이버 응답 디코딩 — UTF-8 우선(main.naver), 실패 시 EUC-KR(coinfo.naver)"""
    for enc in ('utf-8', 'euc-kr'):
        try:
            return res.content.decode(enc, errors='strict')
        except Exception:
            pass
    return res.content.decode('euc-kr', errors='replace')


def _extract_num(text):
    """'32.68배\n6564원' 같은 셀에서 숫자 부분만 추출"""
    import re
    # '배' 앞의 숫자만 추출 (PER/PBR)
    m = re.match(r'[\d.]+', text.replace(',', '').strip())
    return m.group(0) if m else text.strip()


def get_fundamental(ticker):
    """네이버 금융에서 PER, PBR, 영업이익률, ROE, 부채비율, Forward PER 스크래핑"""
    result = {
        'per': 'N/A', 'forward_per': 'N/A', 'pbr': 'N/A',
        'operating_profit': [], 'eps': 'N/A',
        'roe': 'N/A', 'op_margin': 'N/A', 'debt_ratio': 'N/A',
        'revenue': [],
    }

    _coinfo_headers = {
        **HEADERS,
        'Referer': f'https://finance.naver.com/item/coinfo.naver?code={ticker}',
        'X-Requested-With': 'XMLHttpRequest',
    }

    try:
        # ── 1. PER / PBR (main 페이지, UTF-8)
        main_res = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers=HEADERS, timeout=5)
        main_soup = BeautifulSoup(main_res.content.decode('utf-8', errors='replace'), 'html.parser')

        table = main_soup.select_one('table.per_table')
        if table:
            for row in table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                raw = td.get_text(strip=True).replace(',', '')
                val = _extract_num(raw)
                if 'PER' in key and 'Forward' not in key and result['per'] == 'N/A':
                    result['per'] = val
                elif 'PBR' in key and result['pbr'] == 'N/A':
                    result['pbr'] = val

        # ── 2. 매출액 / 영업이익 — FnGuide SVD_Finance (재무제표 수치)
        try:
            fg_url = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp"
                      f"?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701")
            fg_res = requests.get(fg_url,
                headers={**HEADERS, 'Referer': 'https://comp.fnguide.com/'}, timeout=8)
            fg_soup = BeautifulSoup(fg_res.content.decode('utf-8', errors='replace'), 'html.parser')

            for tbl in fg_soup.find_all('table'):
                for tr in tbl.find_all('tr'):
                    th = tr.find('th') or (tr.find_all('td')[0] if tr.find_all('td') else None)
                    if not th:
                        continue
                    key = th.get_text(strip=True)
                    tds = tr.find_all('td')
                    if not tds:
                        continue
                    vals = [td.get_text(strip=True).replace(',', '') for td in tds]
                    if '매출액' in key and not result['revenue']:
                        result['revenue'] = [v for v in vals[:4] if v]
                    elif '영업이익' in key and '률' not in key and not result['operating_profit']:
                        result['operating_profit'] = [v for v in vals[:4] if v]
        except Exception as e:
            print(f"FnGuide 재무 수집 오류: {e}")

        # 영업이익률 — 영업이익/매출액으로 직접 계산
        try:
            if result['operating_profit'] and result['revenue']:
                op = float(result['operating_profit'][0].replace(',', ''))
                rev = float(result['revenue'][0].replace(',', ''))
                if rev > 0:
                    result['op_margin'] = f"{op / rev * 100:.1f}"
        except Exception:
            pass

        # ── 3. ROE / 부채비율 — FnGuide SVD_Main (주요 투자지표)
        try:
            main_url = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
                        f"?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=&NewMenuID=11&stkGb=701")
            main_res2 = requests.get(main_url,
                headers={**HEADERS, 'Referer': 'https://comp.fnguide.com/'}, timeout=8)
            main_soup2 = BeautifulSoup(main_res2.content.decode('utf-8', errors='replace'), 'html.parser')

            for tbl in main_soup2.find_all('table'):
                for tr in tbl.find_all('tr'):
                    th = tr.find('th')
                    if not th:
                        continue
                    key = th.get_text(strip=True)
                    tds = tr.find_all('td')
                    non_empty = [td.get_text(strip=True).replace(',', '')
                                 for td in tds if td.get_text(strip=True) not in ('', '-', 'N/A')]
                    if not non_empty:
                        continue
                    last = non_empty[-1]
                    if 'ROE' in key and result['roe'] == 'N/A':
                        result['roe'] = _extract_num(last)
                    elif '부채비율' in key and result['debt_ratio'] == 'N/A':
                        result['debt_ratio'] = _extract_num(last)
        except Exception as e:
            print(f"FnGuide 투자지표 오류: {e}")

        # ── 3. Forward PER — FnGuide 컨센서스
        try:
            con_url = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Consensus.asp"
                       f"?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=&NewMenuID=7&stkGb=701")
            con_res = requests.get(con_url,
                headers={**HEADERS, 'Referer': 'https://comp.fnguide.com/'}, timeout=8)
            con_soup = BeautifulSoup(con_res.content.decode('utf-8', errors='replace'), 'html.parser')
            for tbl in con_soup.find_all('table'):
                for tr in tbl.find_all('tr'):
                    th = tr.find('th')
                    if th and 'PER' in th.get_text(strip=True):
                        tds = tr.find_all('td')
                        non_empty = [td.get_text(strip=True).replace(',', '')
                                     for td in tds if td.get_text(strip=True) not in ('', '-')]
                        if non_empty:
                            result['forward_per'] = _extract_num(non_empty[-1])
                        break
        except Exception as e:
            print(f"FnGuide 컨센서스 오류: {e}")

    except Exception as e:
        print(f"펀더멘털 수집 오류: {e}")

    return result
