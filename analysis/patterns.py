"""
규칙 기반 차트 패턴 탐지
헤드앤숄더 / 역헤드앤숄더 / 이중 천정 / 이중 바닥 / 상승 삼각형 / 하강 삼각형
"""
import numpy as np


_ORDER = 5  # 로컬 피크 탐지 윈도우 크기


def _find_peaks(prices, order=_ORDER):
    """로컬 최고점 인덱스 목록 (최소 간격 order봉)"""
    result = []
    n = len(prices)
    for i in range(order, n - order):
        seg = prices[i - order: i + order + 1]
        if prices[i] >= max(seg):
            if not result or i - result[-1] >= order:
                result.append(i)
    return result


def _find_troughs(prices, order=_ORDER):
    """로컬 최저점 인덱스 목록"""
    result = []
    n = len(prices)
    for i in range(order, n - order):
        seg = prices[i - order: i + order + 1]
        if prices[i] <= min(seg):
            if not result or i - result[-1] >= order:
                result.append(i)
    return result


def _pct_diff(a, b):
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom


def detect_patterns(df, lookback=120):
    """
    OHLCV DataFrame에서 차트 패턴을 탐지한다.

    반환: list of dict
      name        - 한국어 패턴 이름
      name_en     - 영어 패턴 이름
      signal      - 'bullish' | 'bearish'
      confirmed   - 현재 가격 기준 돌파/이탈 확인 여부 (bool)
      desc        - 짧은 설명 문자열
      key_pts     - [(timestamp, price, label), ...]  차트 마커용
      neckline    - (x0, y0, x1, y1) 또는 None      차트 선 그리기용
      score_idx   - 패턴 발생 위치 (lookback 내 인덱스)
    """
    if df is None or len(df) < 30:
        return []

    sub = df.tail(lookback).copy()
    closes = sub['close'].values.astype(float)
    dates  = list(sub.index)
    n = len(closes)

    pk = _find_peaks(closes)
    tr = _find_troughs(closes)

    patterns = []

    # ── 1. 헤드앤숄더 ────────────────────────────────────────────────
    for i in range(len(pk) - 2):
        ls_i, hd_i, rs_i = pk[i], pk[i + 1], pk[i + 2]
        ls, hd, rs = closes[ls_i], closes[hd_i], closes[rs_i]
        if hd <= ls or hd <= rs:
            continue
        if _pct_diff(ls, rs) > 0.09:
            continue
        nt1 = [t for t in tr if ls_i < t < hd_i]
        nt2 = [t for t in tr if hd_i < t < rs_i]
        if not nt1 or not nt2:
            continue
        nl_i, nr_i = nt1[-1], nt2[0]
        nl, nr = closes[nl_i], closes[nr_i]
        # 넥라인을 우어깨 이후까지 연장
        extend_i = min(rs_i + max(10, (rs_i - hd_i)), n - 1)
        neckline_end = nr + (nr - nl) / max(nr_i - nl_i, 1) * (extend_i - nr_i)
        confirmed = closes[-1] < nr * 0.99
        patterns.append({
            'name': '헤드앤숄더',
            'name_en': 'Head & Shoulders',
            'signal': 'bearish',
            'confirmed': confirmed,
            'desc': '하락 반전 신호 — 넥라인 이탈 시 매도 고려',
            'key_pts': [
                (dates[ls_i], ls, '좌어깨'),
                (dates[hd_i], hd, '머리'),
                (dates[rs_i], rs, '우어깨'),
            ],
            'neckline': (dates[nl_i], nl, dates[extend_i], neckline_end),
            'score_idx': rs_i,
        })

    # ── 2. 역헤드앤숄더 ──────────────────────────────────────────────
    for i in range(len(tr) - 2):
        ls_i, hd_i, rs_i = tr[i], tr[i + 1], tr[i + 2]
        ls, hd, rs = closes[ls_i], closes[hd_i], closes[rs_i]
        if hd >= ls or hd >= rs:
            continue
        if _pct_diff(ls, rs) > 0.09:
            continue
        np1 = [p for p in pk if ls_i < p < hd_i]
        np2 = [p for p in pk if hd_i < p < rs_i]
        if not np1 or not np2:
            continue
        nl_i, nr_i = np1[-1], np2[0]
        nl, nr = closes[nl_i], closes[nr_i]
        extend_i = min(rs_i + max(10, (rs_i - hd_i)), n - 1)
        neckline_end = nr + (nr - nl) / max(nr_i - nl_i, 1) * (extend_i - nr_i)
        confirmed = closes[-1] > nr * 1.01
        patterns.append({
            'name': '역헤드앤숄더',
            'name_en': 'Inverse H&S',
            'signal': 'bullish',
            'confirmed': confirmed,
            'desc': '상승 반전 신호 — 넥라인 돌파 시 매수 고려',
            'key_pts': [
                (dates[ls_i], ls, '좌어깨'),
                (dates[hd_i], hd, '머리'),
                (dates[rs_i], rs, '우어깨'),
            ],
            'neckline': (dates[nl_i], nl, dates[extend_i], neckline_end),
            'score_idx': rs_i,
        })

    # ── 3. 이중 천정 (Double Top) ────────────────────────────────────
    for i in range(len(pk) - 1):
        p1_i, p2_i = pk[i], pk[i + 1]
        if p2_i - p1_i < 10:
            continue
        p1, p2 = closes[p1_i], closes[p2_i]
        if _pct_diff(p1, p2) > 0.04:
            continue
        mid = [t for t in tr if p1_i < t < p2_i]
        if not mid:
            continue
        valley_i = mid[0]
        valley = closes[valley_i]
        extend_i = min(p2_i + (p2_i - p1_i) // 2, n - 1)
        confirmed = closes[-1] < valley * 0.99
        patterns.append({
            'name': '이중 천정',
            'name_en': 'Double Top',
            'signal': 'bearish',
            'confirmed': confirmed,
            'desc': '하락 반전 — M자형 천정 형성, 지지선 이탈 주의',
            'key_pts': [
                (dates[p1_i], p1, '1차 고점'),
                (dates[p2_i], p2, '2차 고점'),
            ],
            'neckline': (dates[valley_i], valley, dates[extend_i], valley),
            'score_idx': p2_i,
        })

    # ── 4. 이중 바닥 (Double Bottom) ─────────────────────────────────
    for i in range(len(tr) - 1):
        t1_i, t2_i = tr[i], tr[i + 1]
        if t2_i - t1_i < 10:
            continue
        t1, t2 = closes[t1_i], closes[t2_i]
        if _pct_diff(t1, t2) > 0.04:
            continue
        mid = [p for p in pk if t1_i < p < t2_i]
        if not mid:
            continue
        peak_i = mid[0]
        peak_v = closes[peak_i]
        extend_i = min(t2_i + (t2_i - t1_i) // 2, n - 1)
        confirmed = closes[-1] > peak_v * 1.01
        patterns.append({
            'name': '이중 바닥',
            'name_en': 'Double Bottom',
            'signal': 'bullish',
            'confirmed': confirmed,
            'desc': '상승 반전 — W자형 바닥 형성, 저항선 돌파 기대',
            'key_pts': [
                (dates[t1_i], t1, '1차 저점'),
                (dates[t2_i], t2, '2차 저점'),
            ],
            'neckline': (dates[peak_i], peak_v, dates[extend_i], peak_v),
            'score_idx': t2_i,
        })

    # ── 5. 상승 삼각형 (Ascending Triangle) ──────────────────────────
    if len(pk) >= 2 and len(tr) >= 2:
        rpk = pk[-3:] if len(pk) >= 3 else pk[-2:]
        rtr = tr[-3:] if len(tr) >= 3 else tr[-2:]
        pk_vals = [closes[i] for i in rpk]
        tr_vals = [closes[i] for i in rtr]
        resistance = np.mean(pk_vals)
        pk_flat    = all(_pct_diff(v, resistance) <= 0.025 for v in pk_vals)
        tr_rising  = all(tr_vals[j + 1] > tr_vals[j] for j in range(len(tr_vals) - 1))
        if pk_flat and tr_rising and len(rpk) >= 2:
            confirmed = closes[-1] > resistance * 1.01
            patterns.append({
                'name': '상승 삼각형',
                'name_en': 'Ascending Triangle',
                'signal': 'bullish',
                'confirmed': confirmed,
                'desc': '상방 돌파 기대 — 저점 상승 + 수평 저항선',
                'key_pts': [(dates[i], closes[i], '') for i in rpk],
                'neckline': (dates[rpk[0]], resistance, dates[rpk[-1]], resistance),
                'score_idx': rpk[-1],
            })

    # ── 6. 하강 삼각형 (Descending Triangle) ─────────────────────────
    if len(pk) >= 2 and len(tr) >= 2:
        rpk = pk[-3:] if len(pk) >= 3 else pk[-2:]
        rtr = tr[-3:] if len(tr) >= 3 else tr[-2:]
        pk_vals = [closes[i] for i in rpk]
        tr_vals = [closes[i] for i in rtr]
        support   = np.mean(tr_vals)
        tr_flat   = all(_pct_diff(v, support) <= 0.025 for v in tr_vals)
        pk_falling = all(pk_vals[j + 1] < pk_vals[j] for j in range(len(pk_vals) - 1))
        if tr_flat and pk_falling and len(rtr) >= 2:
            confirmed = closes[-1] < support * 0.99
            patterns.append({
                'name': '하강 삼각형',
                'name_en': 'Descending Triangle',
                'signal': 'bearish',
                'confirmed': confirmed,
                'desc': '하방 이탈 경계 — 고점 하락 + 수평 지지선',
                'key_pts': [(dates[i], closes[i], '') for i in rtr],
                'neckline': (dates[rtr[0]], support, dates[rtr[-1]], support),
                'score_idx': rtr[-1],
            })

    # 최신 순 정렬 후 최대 3개만 반환
    patterns.sort(key=lambda x: x['score_idx'], reverse=True)
    return patterns[:3]


def simplify_for_template(patterns):
    """Jinja2 템플릿용으로 타임스탬프 등 직렬화 불가 항목 제거"""
    out = []
    for p in patterns:
        out.append({
            'name':      p['name'],
            'name_en':   p['name_en'],
            'signal':    p['signal'],
            'confirmed': p['confirmed'],
            'desc':      p['desc'],
        })
    return out
