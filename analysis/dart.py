import os
import json
import zipfile
import io
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'), override=True)

DART_API_KEY = os.environ.get('DART_API_KEY', '')
_CORP_CACHE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'dart_corp_codes.json')
_NAME_CACHE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'dart_name_map.json')
_CACHE_TTL = 60 * 60 * 24  # 24시간


def _load_corp_codes():
    """DART 고유번호 목록 로드 (24시간 캐시)"""
    if os.path.exists(_CORP_CACHE_PATH) and os.path.exists(_NAME_CACHE_PATH):
        mtime = os.path.getmtime(_CORP_CACHE_PATH)
        if time.time() - mtime < _CACHE_TTL:
            with open(_CORP_CACHE_PATH, encoding='utf-8') as f:
                return json.load(f)

    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
    resp = requests.get(url, timeout=15)
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    xml_data = z.read('CORPCODE.xml')
    root = ET.fromstring(xml_data)

    corp_map = {}   # stock_code → corp_code
    name_map = {}   # corp_name → stock_code  (상장 종목만)
    for item in root.findall('list'):
        stock_code = item.findtext('stock_code', '').strip()
        corp_code  = item.findtext('corp_code',  '').strip()
        corp_name  = item.findtext('corp_name',  '').strip()
        if stock_code:
            corp_map[stock_code] = corp_code
            if corp_name:
                name_map[corp_name] = stock_code

    with open(_CORP_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(corp_map, f, ensure_ascii=False)
    with open(_NAME_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(name_map, f, ensure_ascii=False)

    return corp_map


def search_ticker_by_name(query):
    """종목명으로 티커 검색 (DART 전체 상장사 대상)"""
    if not DART_API_KEY:
        return None, None
    try:
        # 캐시 로드 (없으면 corp_codes 갱신 시 자동 생성됨)
        if not os.path.exists(_NAME_CACHE_PATH):
            _load_corp_codes()
        with open(_NAME_CACHE_PATH, encoding='utf-8') as f:
            name_map = json.load(f)
    except Exception:
        return None, None

    query = query.strip()
    # 완전 일치
    if query in name_map:
        return name_map[query], query
    # 부분 일치 (포함)
    matches = [(name, ticker) for name, ticker in name_map.items()
               if query in name or name in query]
    if matches:
        # 이름 길이가 가장 가까운 것 선택
        matches.sort(key=lambda x: abs(len(x[0]) - len(query)))
        return matches[0][1], matches[0][0]
    return None, None


def get_corp_code(ticker):
    try:
        corp_map = _load_corp_codes()
        return corp_map.get(ticker)
    except Exception:
        return None


# 공시 유형 한글 매핑
_PBLNTF_LABELS = {
    'A': '정기공시', 'B': '주요사항보고', 'C': '발행공시',
    'D': '지분공시', 'E': '기타공시', 'F': '외부감사관련',
    'G': '펀드공시', 'H': '자산유동화', 'I': '거래소공시', 'J': '공정위공시'
}

_IMPORTANT_TYPES = {'B', 'C', 'D', 'I'}  # 중요 공시 유형


def get_company_info(ticker):
    """DART에서 기업 기본정보 조회 (실패 시 Naver Finance 폴백)"""
    from analysis.fundamental import get_company_info_naver

    dart_info = {}
    if DART_API_KEY:
        corp_code = get_corp_code(ticker)
        if corp_code:
            try:
                resp = requests.get(
                    "https://opendart.fss.or.kr/api/company.json",
                    params={'crtfc_key': DART_API_KEY, 'corp_code': corp_code},
                    timeout=10
                )
                data = resp.json()
                if data.get('status') == '000':
                    dart_info = {
                        'ceo': data.get('ceo_nm', ''),
                        'industry': '',  # induty_code는 숫자코드라 미사용
                        'founded': data.get('est_dt', ''),
                        'fiscal_month': (data.get('acc_mt', '') + '월') if data.get('acc_mt') else '',
                        'website': data.get('hm_url', ''),
                        'address': data.get('adres', ''),
                    }
            except Exception:
                pass

    # Naver Finance에서 부족한 정보 보완
    naver_info = get_company_info_naver(ticker)

    merged = {
        'ceo': dart_info.get('ceo') or naver_info.get('ceo', 'N/A'),
        'industry': naver_info.get('industry', dart_info.get('industry', 'N/A')),
        'founded': dart_info.get('founded') or naver_info.get('founded', ''),
        'fiscal_month': dart_info.get('fiscal_month') or naver_info.get('fiscal_month', 'N/A'),
        'website': dart_info.get('website') or naver_info.get('website', ''),
        'address': dart_info.get('address', ''),
    }
    return merged


_IMPORTANT_KEYWORDS = (
    '유상증자', '무상증자', '합병', '분할', '자사주', '배당',
    '대량보유', '최대주주', '임원', '대표이사', '영업양도', '주요사항',
    '취득', '처분', '상장폐지', '감자', '전환사채', '신주인수권',
)


def _classify(report_nm):
    """공시 제목 키워드로 유형·중요도 분류"""
    for kw in _IMPORTANT_KEYWORDS:
        if kw in report_nm:
            return '주요공시', True
    if '사업보고서' in report_nm or '반기보고서' in report_nm or '분기보고서' in report_nm:
        return '정기공시', False
    if '감사보고서' in report_nm or '내부회계' in report_nm:
        return '외부감사', False
    return '기타공시', False


def _scrape_dart_html(name, days=60):
    """DART 공시 목록 HTML 스크래핑 — API 키 없을 때 폴백"""
    from bs4 import BeautifulSoup
    end_dt   = datetime.today()
    start_dt = end_dt - timedelta(days=days)
    try:
        resp = requests.get(
            "https://dart.fss.or.kr/dsac001/searchResult.do",
            params={
                'currentPage': '1', 'maxResults': '20', 'maxLinks': '10',
                'startPage': '1', 'textCrpCik': '', 'textCrpNm': name,
                'startDt': start_dt.strftime('%Y%m%d'),
                'endDt':   end_dt.strftime('%Y%m%d'),
                'publicType': '',
            },
            headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://dart.fss.or.kr/'},
            timeout=10
        )
        soup = BeautifulSoup(resp.content.decode('utf-8', errors='replace'), 'html.parser')
        result = []
        for tbl in soup.find_all('table'):
            rows = tbl.find_all('tr')
            for tr in rows:
                tds = tr.find_all('td')
                if len(tds) < 5:
                    continue
                report_a = tds[2].find('a') if len(tds) > 2 else None
                if not report_a:
                    continue
                href = report_a.get('href', '')
                rcp_no = href.split('rcpNo=')[-1].split('&')[0] if 'rcpNo=' in href else ''
                report_nm = report_a.get_text(strip=True)
                submitter = tds[3].get_text(strip=True) if len(tds) > 3 else ''
                date_raw  = tds[4].get_text(strip=True).replace('.', '').replace('-', '') if len(tds) > 4 else ''
                disc_type, important = _classify(report_nm)
                result.append({
                    'date': date_raw, 'title': report_nm,
                    'type': disc_type, 'important': important,
                    'url': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}",
                    'submitter': submitter,
                })
            if result:
                break
        return result
    except Exception:
        return []


def get_disclosures(ticker, days=60):
    """최근 공시 목록 반환"""
    if not DART_API_KEY:
        # API 키 없으면 DART 웹 HTML 스크래핑
        from analysis.data_fetcher import get_ticker as _get_ticker
        _, name = _get_ticker(ticker)
        if name:
            return _scrape_dart_html(name, days)
        return []

    corp_code = get_corp_code(ticker)
    if not corp_code:
        return []

    end_dt = datetime.today()
    start_dt = end_dt - timedelta(days=days)

    try:
        resp = requests.get(
            "https://opendart.fss.or.kr/api/list.json",
            params={
                'crtfc_key': DART_API_KEY,
                'corp_code': corp_code,
                'bgn_de': start_dt.strftime('%Y%m%d'),
                'end_de': end_dt.strftime('%Y%m%d'),
                'page_count': 20,
                'sort': 'date',
                'sort_mth': 'desc',
            },
            timeout=10
        )
        data = resp.json()
    except Exception:
        return []

    if data.get('status') != '000':
        return []

    result = []
    for item in data.get('list', []):
        report_nm = item.get('report_nm', '')
        rcept_no  = item.get('rcept_no', '')
        disc_type, important = _classify(report_nm)
        result.append({
            'date':      item.get('rcept_dt', ''),
            'title':     report_nm,
            'type':      disc_type,
            'important': important,
            'url':       f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
            'submitter': item.get('flr_nm', ''),
        })

    return result
