import os
import json
import zipfile
import io
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

DART_API_KEY = os.environ.get('DART_API_KEY', '')
_CORP_CACHE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'dart_corp_codes.json')
_NAME_CACHE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'dart_name_map.json')
_CACHE_TTL = 60 * 60 * 24  # 24시간


def _load_corp_codes():
    """DART 고유번호 목록 로드 (24시간 캐시)"""
    if os.path.exists(_CORP_CACHE_PATH):
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
    """DART에서 기업 기본정보 조회"""
    if not DART_API_KEY:
        return {}
    corp_code = get_corp_code(ticker)
    if not corp_code:
        return {}
    try:
        resp = requests.get(
            "https://opendart.fss.or.kr/api/company.json",
            params={'crtfc_key': DART_API_KEY, 'corp_code': corp_code},
            timeout=10
        )
        data = resp.json()
        if data.get('status') != '000':
            return {}
        return {
            'ceo': data.get('ceo_nm', 'N/A'),
            'industry': data.get('induty_code', 'N/A'),
            'founded': data.get('est_dt', ''),
            'fiscal_month': data.get('acc_mt', 'N/A') + '월',
            'website': data.get('hm_url', ''),
            'address': data.get('adres', ''),
        }
    except Exception:
        return {}


def get_disclosures(ticker, days=60):
    """최근 공시 목록 반환"""
    if not DART_API_KEY:
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
        pblntf_ty = item.get('pblntf_ty', 'E')
        rcp_no = item.get('rcp_no', '')
        result.append({
            'date': item.get('rcept_dt', ''),
            'title': item.get('report_nm', ''),
            'type': _PBLNTF_LABELS.get(pblntf_ty, '기타'),
            'important': pblntf_ty in _IMPORTANT_TYPES,
            'url': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}",
            'submitter': item.get('flr_nm', ''),
        })

    return result
