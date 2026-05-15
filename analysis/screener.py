import json, os, requests, time
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from analysis.data_fetcher import get_ohlcv, get_investor_detail
from analysis.indicators import calc_indicators, get_ma_arrangement, get_latest_signals
from analysis.news import search_naver_news, get_news_signal_score, analyze_news
from analysis.signal import calc_score, get_recommendation
from analysis.cache_manager import load_stock_cache

_NAVER_HDR = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# ETF·리츠·인덱스 펀드 이름 패턴 (수급주도·추천 스캔에서 제외)
_ETF_PATTERNS = [
    'KODEX','TIGER','RISE','ACE','KBSTAR','HANARO','KOSEF',
    'ARIRANG','TIMEFOLIO','PLUS','SOL ','WOORI ETF','MASTER',
    'FOCUS','SMART','KIM','TREX','DB','HANWHA','MIRAE',
    'ETF','리츠','인프라펀드',
]

_TICKER_DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_tickers.json')

# 실적·수주·정책 모멘텀 키워드 (DART 공시 제목 기반)
# (탐지 키워드, 표시 태그, 보너스 점수)
_MOMENTUM_KW = [
    ('수주',         '수주',        5),
    ('공급계약',     '공급계약',    5),
    ('납품계약',     '납품계약',    5),
    ('수출계약',     '수출계약',    5),
    ('잠정실적',     '잠정실적',    4),
    ('국책과제',     '국책과제',    4),
    ('과제선정',     '과제선정',    4),
    ('기술이전',     '기술이전',    4),
    ('영업이익증가', '이익증가',    3),
    ('매출증가',     '매출증가',    3),
    ('정부지원',     '정부지원',    3),
    ('보조금',       '보조금',      3),
    ('R&D',          'R&D과제',     3),
    ('신사업',       '신사업',      3),
    ('증설',         '설비증설',    3),
    ('MOU',          'MOU',         2),
    ('업무협약',     '업무협약',    2),
    ('전략적제휴',   '전략제휴',    2),
    ('투자협약',     '투자협약',    2),
]


def _count_consecutive_buying(investor_df, col_keyword, days=5):
    """외인 또는 기관의 연속 순매수 일수 계산"""
    if investor_df.empty:
        return 0
    cols = [c for c in investor_df.columns if col_keyword in c]
    if not cols:
        return 0
    series = investor_df[cols[0]].tail(days)
    count = 0
    for v in reversed(series.values):
        if v > 0:
            count += 1
        else:
            break
    return count


def _calc_joint_buying(investor_df, days=20, threshold=15):
    """외인+기관 동시 순매수 일수 계산 (최근 days일 중 threshold일 초과 여부)"""
    if investor_df.empty:
        return 0, False
    foreign_col = next((c for c in investor_df.columns if '외국인' in c or '외인' in c), None)
    inst_col = next((c for c in investor_df.columns if '기관' in c
                     and '금융' not in c and '연기금' not in c), None)
    if not foreign_col or not inst_col:
        return 0, False
    recent = investor_df.tail(days)
    joint_days = int(((recent[foreign_col] > 0) & (recent[inst_col] > 0)).sum())
    return joint_days, joint_days > threshold


def _calc_buying_surge_star(investor_df, recent_days=10, past_days=20):
    """최근 10거래일 기관+외인 매수세가 직전 20거래일 대비 2배 이상 증가 여부"""
    if investor_df.empty or len(investor_df) < recent_days + past_days:
        return False
    foreign_col = next((c for c in investor_df.columns if '외국인' in c or '외인' in c), None)
    inst_col = next((c for c in investor_df.columns if '기관' in c
                     and '금융' not in c and '연기금' not in c), None)
    if not foreign_col or not inst_col:
        return False
    combined = investor_df[foreign_col] + investor_df[inst_col]
    recent = combined.tail(recent_days)
    past = combined.iloc[-(recent_days + past_days):-recent_days]
    recent_avg = recent[recent > 0].sum() / recent_days
    past_avg = past[past > 0].sum() / past_days
    return past_avg > 0 and recent_avg >= past_avg * 2


def _calc_volume_surge(investor_df, days=20, surge_ratio=1.5):
    """외인+기관 합산 수급량이 평시 대비 surge_ratio배 이상인지 확인"""
    if investor_df.empty:
        return False
    foreign_col = next((c for c in investor_df.columns if '외국인' in c or '외인' in c), None)
    inst_col    = next((c for c in investor_df.columns if '기관' in c
                        and '금융' not in c and '연기금' not in c), None)
    if not foreign_col:
        return False
    series = investor_df[foreign_col].abs()
    if inst_col:
        series = series + investor_df[inst_col].abs()
    if len(series) < days * 2:
        return False
    recent_avg = series.tail(days).mean()
    past_avg   = series.iloc[-(days * 2):-days].mean()
    return past_avg > 0 and recent_avg >= past_avg * surge_ratio


def _analyze_one(name, ticker, months=6):
    try:
        cached = load_stock_cache(ticker)
        if cached:
            ohlcv = cached['ohlcv'].tail(max(months * 22, 60))
            investor_df = cached['investor_df']
        else:
            ohlcv = get_ohlcv(ticker, months)
            try:
                investor_df = get_investor_detail(ticker, months=2)
            except Exception:
                investor_df = pd.DataFrame()

        if ohlcv is None or ohlcv.empty or len(ohlcv) < 20:
            return None
        df = calc_indicators(ohlcv)
        ma_status = get_ma_arrangement(df)
        signals = get_latest_signals(df)

        # 뉴스는 개별 종목 분석에서 로드 — 대량 스캔 시 제외하여 속도 향상
        news_result = {'total': 0, 'positive': 0, 'negative': 0, 'neutral': 0,
                       'sentiment_score': 0, 'top_keywords': [], 'press_counts': {},
                       'exclusive_count': 0, 'articles': []}

        score, reasons = calc_score(ma_status, signals, investor_df, news_result, df)
        current_price = int(df['close'].iloc[-1])

        # 외인·기관 연속 매수 일수
        foreign_streak = _count_consecutive_buying(investor_df, '외국인')
        if foreign_streak == 0:
            foreign_streak = _count_consecutive_buying(investor_df, '외인')
        inst_streak = _count_consecutive_buying(investor_df, '기관')

        # 외인+기관 동시매수 20일 중 10일 초과 여부 (절반 이상)
        joint_days, joint_star = _calc_joint_buying(investor_df, days=20, threshold=10)

        # 수급량 급증 여부
        volume_surge = _calc_volume_surge(investor_df, days=20, surge_ratio=1.5)

        # 최근 10거래일 매수세 2배 급증 여부
        buying_surge_star = _calc_buying_surge_star(investor_df)

        # ── 수급 보너스를 score에 직접 합산 (정렬·표시 일치) ──────────
        # 외인 연속매수: 1~2일 +1 / 3~4일 +2 / 5일↑ +4
        if foreign_streak >= 5:
            score += 4; reasons.append(f"외인 {foreign_streak}일 연속 매수 (+4점)")
        elif foreign_streak >= 3:
            score += 2; reasons.append(f"외인 {foreign_streak}일 연속 매수 (+2점)")
        elif foreign_streak >= 1:
            score += 1

        # 기관 연속매수: 1~2일 +1 / 3일↑ +2
        if inst_streak >= 3:
            score += 2; reasons.append(f"기관 {inst_streak}일 연속 매수 (+2점)")
        elif inst_streak >= 1:
            score += 1

        # 동시매수 지속 (20일 중 절반 이상)
        if joint_star:
            score += 4; reasons.append(f"외인+기관 동시매수 {joint_days}/20일 (+4점)")

        # 단기 매수세 2배 급증
        if buying_surge_star:
            score += 3; reasons.append("최근 매수세 2배↑ 급증 (+3점)")

        # 외인+기관 합산 수급량 급증
        if volume_surge:
            score += 2; reasons.append("외인+기관 수급량 1.5배↑ (+2점)")

        # 점수 정규화: 기술(max~20) + 수급보너스(max~15) → 총 max~35, min -14
        score_pct = max(0, min(100, round((score + 14) / 50 * 100)))
        recommendation, rec_color = get_recommendation(score)

        return {
            'name': name,
            'ticker': ticker,
            'score': score,
            'score_pct': score_pct,
            'foreign_streak': foreign_streak,
            'inst_streak': inst_streak,
            'joint_days': joint_days,
            'joint_star': bool(joint_star),
            'volume_surge': bool(volume_surge),
            'buying_surge_star': bool(buying_surge_star),
            'recommendation': recommendation,
            'rec_color': rec_color,
            'price': f"{current_price:,}",
            'ma_label': ma_status[0],
            'rsi': signals['rsi']['value'],
            'macd': signals['macd']['signal'],
            'reasons': reasons[:5],
        }
    except Exception:
        return None


def _check_supply_one(name, ticker, months=3):
    """수급 조건 전용 경량 스캔 (가격·오실레이터 생략)"""
    try:
        investor_df = get_investor_detail(ticker, months)
        if investor_df.empty or len(investor_df) < 10:
            return None

        foreign_col = next((c for c in investor_df.columns if '외국인' in c or '외인' in c), None)
        inst_col = next((c for c in investor_df.columns if '기관' in c
                         and '금융' not in c and '연기금' not in c), None)
        if not foreign_col or not inst_col:
            return None

        # 조건 A: 외인+기관 동시 연속 3거래일 이상
        joint_streak = 0
        for i in range(len(investor_df) - 1, -1, -1):
            row = investor_df.iloc[i]
            if row[foreign_col] > 0 and row[inst_col] > 0:
                joint_streak += 1
            else:
                break

        # 조건 B: 최근 10거래일 중 외인 또는 기관이 7일 이상 순매수
        last10 = investor_df.tail(10)
        foreign_days = int((last10[foreign_col] > 0).sum())
        inst_days = int((last10[inst_col] > 0).sum())

        meets_a = joint_streak >= 3
        meets_b = foreign_days >= 7 or inst_days >= 7

        if not meets_a and not meets_b:
            return None

        # 현재가
        try:
            ohlcv = get_ohlcv(ticker, months=1)
            current_price = int(ohlcv['close'].iloc[-1]) if not ohlcv.empty else 0
        except Exception:
            current_price = 0

        f_streak = _count_consecutive_buying(investor_df, '외국인')
        if f_streak == 0:
            f_streak = _count_consecutive_buying(investor_df, '외인')
        i_streak = _count_consecutive_buying(investor_df, '기관')

        # 최근 10일 외인·기관 순매수 합계
        f_net = int(last10[foreign_col].sum())
        i_net = int(last10[inst_col].sum())

        combined = investor_df[foreign_col] + investor_df[inst_col]

        # ① 수급 가속도: 최근 5일 합산 vs 직전 20일 일평균
        recent5 = combined.tail(5)
        past20  = combined.iloc[-25:-5] if len(combined) >= 25 else combined.iloc[:-5]
        past_avg = float(past20.mean()) if len(past20) > 0 else 0
        recent5_sum = float(recent5.sum())
        if past_avg > 0 and recent5_sum > 0:
            surge_ratio = round(recent5_sum / (past_avg * 5), 1)
        else:
            surge_ratio = 0.0
        surge_signal = surge_ratio >= 2.0

        # ② 수급 전환: 직전 10~20일 순매도 → 최근 10일 순매수
        prev_period = combined.iloc[-20:-10] if len(combined) >= 20 else pd.Series(dtype=float)
        reversal_foreign = False
        reversal_inst    = False
        if not prev_period.empty:
            prev_f  = float(investor_df[foreign_col].iloc[-20:-10].sum()) if len(investor_df) >= 20 else 0
            prev_i  = float(investor_df[inst_col].iloc[-20:-10].sum())    if len(investor_df) >= 20 else 0
            rec_f   = float(investor_df[foreign_col].tail(10).sum())
            rec_i   = float(investor_df[inst_col].tail(10).sum())
            reversal_foreign = prev_f < 0 and rec_f > 0
            reversal_inst    = prev_i < 0 and rec_i > 0
        reversal_signal = reversal_foreign or reversal_inst

        # ③ 최근 집중 비중: 전체 3개월 중 최근 10일 순매수 비중
        total_sum  = float(combined.sum())
        recent10_sum = float(combined.tail(10).sum())
        if total_sum > 0 and recent10_sum > 0:
            recent_weight_pct = round(recent10_sum / total_sum * 100)
        else:
            recent_weight_pct = 0
        concentration_signal = recent_weight_pct >= 50

        # ── 100점 만점 종합 점수 ──────────────────────────────
        # ① 수급 지속성: 외인+기관 동시 연속 1일당 3점 (최대 30점)
        score_streak   = min(joint_streak * 3, 30)
        # ② 수급 빈도: 외인/기관 각 10점 (최대 20점)
        score_freq_f   = min(foreign_days, 10)
        score_freq_i   = min(inst_days, 10)
        # ③ 수급 가속도: (배율-1)*10점, 최대 25점
        score_surge    = min(round((surge_ratio - 1) * 10), 25) if surge_ratio >= 2.0 else 0
        # ④ 수급 전환: 외인 +8점, 기관 +7점
        score_rev_f    = 8 if bool(reversal_foreign) else 0
        score_rev_i    = 7 if bool(reversal_inst) else 0
        # ⑤ 최근 집중도: 비중%÷10점 (최대 10점)
        score_conc     = min(round(recent_weight_pct / 10), 10) if concentration_signal else 0

        supply_score = (score_streak + score_freq_f + score_freq_i +
                        score_surge + score_rev_f + score_rev_i + score_conc)

        score_breakdown = {
            '수급지속성': score_streak,
            '외인빈도':   score_freq_f,
            '기관빈도':   score_freq_i,
            '수급가속도': score_surge,
            '외인전환':   score_rev_f,
            '기관전환':   score_rev_i,
            '최근집중도': score_conc,
        }

        return {
            'name': name,
            'ticker': ticker,
            'price': f"{current_price:,}",
            'joint_streak': joint_streak,
            'foreign_days': foreign_days,
            'inst_days': inst_days,
            'foreign_streak': f_streak,
            'inst_streak': i_streak,
            'foreign_net': f_net,
            'inst_net': i_net,
            'meets_a': meets_a,
            'meets_b': meets_b,
            'surge_ratio': surge_ratio,
            'surge_signal': bool(surge_signal),
            'reversal_foreign': bool(reversal_foreign),
            'reversal_inst': bool(reversal_inst),
            'reversal_signal': bool(reversal_signal),
            'recent_weight_pct': recent_weight_pct,
            'concentration_signal': bool(concentration_signal),
            'supply_score': supply_score,
            'score_breakdown': score_breakdown,
            'sort_key': supply_score,
        }
    except Exception:
        return None


def scan_supply_leaders(months=3, max_workers=8):
    """외인·기관 수급 주도 종목 스캔"""
    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_check_supply_one, name, ticker, months): name
                   for name, ticker in tickers.items()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    results.sort(key=lambda x: x['sort_key'], reverse=True)
    return results


# 바닥 반등용 (10/20/60/120일선) — 중장기 지지선 반등
_BOUNCE_MA_COLS = [('ma10', '10일'), ('ma20', '20일'), ('ma60', '60일'), ('ma120', '120일')]
_TOUCH_PCT  = 0.03    # MA ±3% 이내면 "터치"로 인정
_BOUNCE_MIN = 2.0     # 최저점 대비 최소 반등률(%)
_LOOKBACK   = 10      # 최근 10거래일 내 터치 확인


def _check_ma_bounce(name, ticker):
    """10/20/60/120일선 바닥 터치 후 반등 종목 감지"""
    try:
        cached = load_stock_cache(ticker)
        if not cached:
            return None
        ohlcv = cached['ohlcv']
        if ohlcv is None or ohlcv.empty or len(ohlcv) < 130:
            return None

        df = ohlcv.copy()
        for col, _ in _BOUNCE_MA_COLS:
            days = int(col[2:])
            df[col] = df['close'].rolling(days).mean()
        df['ma5'] = df['close'].rolling(5).mean()

        last = df.iloc[-1]
        cur  = float(last['close'])

        touched = []
        for col, label in _BOUNCE_MA_COLS:
            cur_ma = last[col]
            if pd.isna(cur_ma):
                continue
            cur_ma = float(cur_ma)
            # 현재가가 MA 위에 있어야 (반등 완료)
            if cur <= cur_ma * 1.005:
                continue
            # 최근 LOOKBACK일 중 저가가 MA를 터치했는지
            window = df.iloc[-(1 + _LOOKBACK):-1]
            for i in range(len(window)):
                low  = float(window['low'].iloc[i])
                ma_d = window[col].iloc[i]
                if pd.isna(ma_d):
                    continue
                ma_d = float(ma_d)
                if abs(low - ma_d) / ma_d <= _TOUCH_PCT:
                    touched.append(label)
                    break

        if not touched:
            return None

        recent_low  = float(df.tail(_LOOKBACK + 1)['low'].min())
        rebound_pct = (cur - recent_low) / recent_low * 100 if recent_low > 0 else 0
        if rebound_pct < _BOUNCE_MIN:
            return None

        recent_vol = df.tail(5)['volume'].mean()
        old_vol    = df.iloc[-25:-5]['volume'].mean()
        vol_ratio  = round(recent_vol / old_vol, 1) if old_vol > 0 else 1.0

        return {
            'name':        name,
            'ticker':      ticker,
            'price':       f"{int(cur):,}",
            'rebound_pct': round(rebound_pct, 1),
            'vol_ratio':   vol_ratio,
            'touched_mas': touched,
            'touch_count': len(touched),
            'sort_key':    len(touched) * 10 + rebound_pct,
            'type':        'bounce',
        }
    except Exception:
        return None


def _check_ma5_riding(name, ticker):
    """5일선 타고 상승 중인 종목 감지 — 5일선 위에서 연속 상승"""
    try:
        cached = load_stock_cache(ticker)
        if not cached:
            return None
        ohlcv = cached['ohlcv']
        if ohlcv is None or ohlcv.empty or len(ohlcv) < 20:
            return None

        df = ohlcv.copy()
        df['ma5']  = df['close'].rolling(5).mean()
        df['ma20'] = df['close'].rolling(20).mean()

        last = df.iloc[-1]
        cur  = float(last['close'])
        ma5  = float(last['ma5']) if not pd.isna(last['ma5']) else None
        ma20 = float(last['ma20']) if not pd.isna(last['ma20']) else None

        if ma5 is None or ma20 is None:
            return None

        # 현재가가 5일선 위, 5일선이 20일선 위
        if cur <= ma5 * 1.001:
            return None
        if ma5 <= ma20 * 1.00:
            return None

        # 현재가가 5일선에서 너무 멀면 제외 (이미 급등 상태)
        gap_pct = (cur - ma5) / ma5 * 100
        if gap_pct > 5.0:
            return None

        # 5일선 기울기 (최근 5일 평균 → 최근 ma5 변화율)
        ma5_series = df['ma5'].dropna().tail(6)
        if len(ma5_series) < 6:
            return None
        ma5_slope = (float(ma5_series.iloc[-1]) - float(ma5_series.iloc[0])) / float(ma5_series.iloc[0]) * 100
        if ma5_slope < 0.5:  # 5일선이 상승 기울기
            return None

        # 최근 5거래일 중 4일 이상 5일선 위에서 마감
        recent = df.tail(5)
        above_count = sum(
            1 for _, row in recent.iterrows()
            if not pd.isna(row['ma5']) and float(row['close']) > float(row['ma5'])
        )
        if above_count < 4:
            return None

        recent_vol = df.tail(5)['volume'].mean()
        old_vol    = df.iloc[-20:-5]['volume'].mean()
        vol_ratio  = round(recent_vol / old_vol, 1) if old_vol > 0 else 1.0

        # 전일 대비 등락률
        prev_close = float(df.iloc[-2]['close']) if len(df) >= 2 else cur
        chg_pct = (cur - prev_close) / prev_close * 100

        return {
            'name':        name,
            'ticker':      ticker,
            'price':       f"{int(cur):,}",
            'ma5_gap':     round(gap_pct, 1),
            'ma5_slope':   round(ma5_slope, 2),
            'chg_pct':     round(chg_pct, 2),
            'vol_ratio':   vol_ratio,
            'touched_mas': ['5일'],
            'touch_count': 1,
            'sort_key':    ma5_slope * 10 + above_count,
            'type':        'riding',
        }
    except Exception:
        return None


def scan_ma_bounce_stocks(top_n=20, max_workers=8):
    """10/20/60/120일선 바닥 반등 + 5일선 상승 타기 종목 스캔"""
    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    bounce_list = []
    riding_list = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 바닥 반등 스캔
        f_bounce = {executor.submit(_check_ma_bounce, name, ticker): name
                    for name, ticker in tickers.items()}
        for future in as_completed(f_bounce):
            r = future.result()
            if r:
                bounce_list.append(r)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 5일선 타기 스캔
        f_riding = {executor.submit(_check_ma5_riding, name, ticker): name
                    for name, ticker in tickers.items()}
        for future in as_completed(f_riding):
            r = future.result()
            if r:
                riding_list.append(r)

    bounce_list.sort(key=lambda x: x['sort_key'], reverse=True)
    riding_list.sort(key=lambda x: x['sort_key'], reverse=True)

    # 두 목록을 합쳐서 반환 (type 필드로 구분)
    return bounce_list[:top_n] + riding_list[:top_n]


def _check_osc_one(name, ticker):
    """RSI·Stochastic·BB·MFI 기반 과매도/과매수 종목 감지"""
    try:
        ohlcv = get_ohlcv(ticker, months=2)
        if ohlcv.empty or len(ohlcv) < 20:
            return None
        df = calc_indicators(ohlcv)
        last = df.iloc[-1]

        rsi    = float(last['rsi'])
        stoch  = float(last['stoch_k'])
        bb_pct = float(last['bb_pct'])
        mfi    = float(last['mfi'])
        cur    = int(last['close'])

        # 과매도 점수 (높을수록 강한 과매도)
        os_score = 0
        os_tags  = []
        if rsi   < 25: os_score += 3; os_tags.append(f'RSI {rsi:.0f}')
        elif rsi < 30: os_score += 2; os_tags.append(f'RSI {rsi:.0f}')
        elif rsi < 40: os_score += 1
        if stoch < 15: os_score += 3; os_tags.append(f'Stoch {stoch:.0f}')
        elif stoch < 20: os_score += 2; os_tags.append(f'Stoch {stoch:.0f}')
        if bb_pct < 0.05: os_score += 3; os_tags.append('BB 하단')
        elif bb_pct < 0.1: os_score += 2; os_tags.append('BB 하단')
        if mfi < 20: os_score += 2; os_tags.append(f'MFI {mfi:.0f}')

        # 과매수 점수
        ob_score = 0
        ob_tags  = []
        if rsi   > 75: ob_score += 3; ob_tags.append(f'RSI {rsi:.0f}')
        elif rsi > 70: ob_score += 2; ob_tags.append(f'RSI {rsi:.0f}')
        elif rsi > 60: ob_score += 1
        if stoch > 85: ob_score += 3; ob_tags.append(f'Stoch {stoch:.0f}')
        elif stoch > 80: ob_score += 2; ob_tags.append(f'Stoch {stoch:.0f}')
        if bb_pct > 0.95: ob_score += 3; ob_tags.append('BB 상단')
        elif bb_pct > 0.9: ob_score += 2; ob_tags.append('BB 상단')
        if mfi > 80: ob_score += 2; ob_tags.append(f'MFI {mfi:.0f}')

        _MAX = 11  # RSI(3) + Stoch(3) + BB(3) + MFI(2)
        threshold = 4
        if os_score >= threshold and os_score > ob_score:
            return {'name': name, 'ticker': ticker, 'price': f"{cur:,}",
                    'kind': 'oversold',
                    'score': os_score,
                    'score100': round(os_score / _MAX * 100),
                    'tags': os_tags,
                    'rsi': round(rsi, 1), 'stoch': round(stoch, 1),
                    'bb': round(bb_pct, 2), 'mfi': round(mfi, 1)}
        if ob_score >= threshold and ob_score > os_score:
            return {'name': name, 'ticker': ticker, 'price': f"{cur:,}",
                    'kind': 'overbought',
                    'score': ob_score,
                    'score100': round(ob_score / _MAX * 100),
                    'tags': ob_tags,
                    'rsi': round(rsi, 1), 'stoch': round(stoch, 1),
                    'bb': round(bb_pct, 2), 'mfi': round(mfi, 1)}
        return None
    except Exception:
        return None


def scan_osc_stocks(top_n=30, max_workers=8):
    """과매도·과매수 종목 스캔"""
    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    oversold, overbought = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_check_osc_one, name, ticker): name
                   for name, ticker in tickers.items()}
        for future in as_completed(futures):
            r = future.result()
            if r:
                (oversold if r['kind'] == 'oversold' else overbought).append(r)

    oversold.sort(key=lambda x: x['score'], reverse=True)
    overbought.sort(key=lambda x: x['score'], reverse=True)
    return {
        'oversold':   oversold[:top_n],
        'overbought': overbought[:top_n],
    }


def _check_surge_one(name, ticker, days_back=5):
    """거래량 급증 + 가격 급등 종목 단일 스캔"""
    try:
        cached = load_stock_cache(ticker)
        if not cached:
            return None
        ohlcv = cached['ohlcv']
        if ohlcv is None or ohlcv.empty or len(ohlcv) < 30:
            return None
        df = calc_indicators(ohlcv)

        last = df.iloc[-1]
        recent = df.tail(days_back)
        older = df.iloc[-(25 + days_back):-days_back]

        if len(older) < 10:
            return None

        # 거래량 급증: 최근 N일 평균이 이전 대비 1.5배 이상
        avg_recent_vol = recent['volume'].mean()
        avg_old_vol = older['volume'].mean()
        if avg_old_vol <= 0 or avg_recent_vol < avg_old_vol * 1.5:
            return None

        # 가격 급등: 최근 N일 기준 3% 이상 상승
        price_start = df.iloc[-(days_back + 1)]['close']
        if price_start <= 0:
            return None
        price_chg_pct = (last['close'] - price_start) / price_start * 100
        if price_chg_pct < 3.0:
            return None

        # MA 배열 확인 (역배열이면 제외)
        ma_status = get_ma_arrangement(df)
        if ma_status[1] == 'bearish':
            return None

        # 5/20/60/120일선과의 근접도 (현재가 기준 각 MA 5% 이내)
        cur = float(last['close'])
        vol_ratio = round(avg_recent_vol / avg_old_vol, 1)

        return {
            'name': name,
            'ticker': ticker,
            'price': f"{int(cur):,}",
            'price_chg_pct': round(price_chg_pct, 1),
            'vol_ratio': vol_ratio,
            'ma_label': ma_status[0],
            'ma_type': ma_status[1],
            'sort_key': vol_ratio * price_chg_pct,
        }
    except Exception:
        return None


def scan_surge_stocks(top_n=20, days_back=5, max_workers=4):
    """거래량 동반 급등 종목 스캔 (5일 내 거래량 1.5배+, 가격 3%+ 상승)"""
    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_check_surge_one, name, ticker, days_back): name
                   for name, ticker in tickers.items()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    results.sort(key=lambda x: x['sort_key'], reverse=True)
    return results[:top_n]


def _is_etf(name):
    return any(p in name for p in _ETF_PATTERNS)


def _parse_op_val(v):
    """영업이익 문자열을 float으로 변환 (FnGuide 형식: 콤마 없음, 음수 포함)"""
    try:
        return float(str(v).replace(',', '').strip())
    except Exception:
        return None


def _check_buy_candidate(name, ticker):
    """매수후보(단기) 13개 조건 캐시 기반 스캔"""
    try:
        cached = load_stock_cache(ticker)
        if not cached:
            return None

        ohlcv       = cached['ohlcv']
        investor_df = cached['investor_df']
        fundamental = cached.get('fundamental', {}) or {}

        if ohlcv is None or ohlcv.empty or len(ohlcv) < 65:
            return None

        df = calc_indicators(ohlcv)
        last = df.iloc[-1]
        cur  = float(last['close'])

        # ── 1. 거래대금 50억↑ ───────────────────────────────────
        avg_tv = (ohlcv['close'] * ohlcv['volume']).tail(20).mean()
        if avg_tv < 5_000_000_000:
            return None

        # ── 2. 주가 60일선 위 ────────────────────────────────────
        if 'ma60' not in df.columns or pd.isna(last['ma60']):
            return None
        ma60 = float(last['ma60'])
        if cur <= ma60:
            return None

        # ── 3. 20일선·60일선 우상향 ─────────────────────────────
        if 'ma20' not in df.columns or len(df) < 66:
            return None
        ma20_now = float(df['ma20'].iloc[-1])
        ma20_6d  = float(df['ma20'].iloc[-7])
        ma60_now = float(df['ma60'].iloc[-1])
        ma60_6d  = float(df['ma60'].iloc[-7])
        if pd.isna(ma20_now) or pd.isna(ma20_6d) or ma20_now <= ma20_6d:
            return None
        if pd.isna(ma60_now) or pd.isna(ma60_6d) or ma60_now <= ma60_6d:
            return None

        # ── 4. RSI 40~60 ─────────────────────────────────────────
        if 'rsi' not in df.columns or pd.isna(last['rsi']):
            return None
        rsi = float(last['rsi'])
        if not (40 <= rsi <= 60):
            return None

        # ── 5. Stochastic 20~30 반등 ─────────────────────────────
        if 'stoch_k' not in df.columns:
            return None
        stoch_series = df['stoch_k'].tail(10).dropna()
        if len(stoch_series) < 5:
            return None
        stoch_cur = float(stoch_series.iloc[-1])
        stoch_min = float(stoch_series.min())
        stoch_3d  = float(stoch_series.iloc[-4]) if len(stoch_series) >= 4 else stoch_cur
        # 최근 10일 내 Stochastic이 35 이하까지 내려왔다가 현재 반등 중
        if stoch_min > 35:
            return None
        if stoch_cur <= stoch_3d:          # 3일 전보다 낮으면 반등 아님
            return None
        if stoch_cur > 65:                 # 너무 올라갔으면 제외
            return None

        # ── 6. MACD 히스토그램 개선 ──────────────────────────────
        if 'macd_hist' not in df.columns:
            return None
        hist_series = df['macd_hist'].tail(6).dropna()
        if len(hist_series) < 4:
            return None
        hist_cur = float(hist_series.iloc[-1])
        hist_4d  = float(hist_series.iloc[-4])
        if hist_cur <= hist_4d:            # 4일 전보다 히스토그램이 개선 안 됨
            return None

        # ── 7. 거래량 증가 (최근 5일 > 직전 20일 평균) ──────────
        vol5  = ohlcv['volume'].tail(5).mean()
        vol20 = ohlcv['volume'].iloc[-25:-5].mean() if len(ohlcv) >= 25 else ohlcv['volume'].mean()
        if vol5 <= vol20 * 0.9:
            return None

        # ── 8. 외국인 또는 기관 5~20일 순매수 ────────────────────
        foreign_streak = _count_consecutive_buying(investor_df, '외국인')
        if foreign_streak == 0:
            foreign_streak = _count_consecutive_buying(investor_df, '외인')
        inst_streak = _count_consecutive_buying(investor_df, '기관')
        if foreign_streak < 5 and inst_streak < 5:
            return None

        # ── 9. 펀더멘털 — 캐시된 FnGuide 분기 데이터 ───────────
        op_list = fundamental.get('operating_profit', [])
        if op_list and len(op_list) >= 4:
            # 최근 4분기 모두 흑자
            parsed = [_parse_op_val(v) for v in op_list[:4]]
            if any(v is not None and v <= 0 for v in parsed):
                return None
            # 실적 YoY 개선 (최신 분기 > 4분기 전)
            if len(op_list) >= 4 and parsed[0] is not None and parsed[3] is not None:
                if parsed[3] > 0 and parsed[0] < parsed[3]:
                    return None

        # 부채비율 200% 미만
        debt_ratio_raw = fundamental.get('debt_ratio', 'N/A')
        debt_ratio_val = None
        if debt_ratio_raw != 'N/A':
            try:
                debt_ratio_val = float(str(debt_ratio_raw).replace('%', '').replace(',', ''))
                if debt_ratio_val >= 200:
                    return None
            except Exception:
                pass

        # ── 여기까지 통과 → 태그 및 점수 계산 ───────────────────
        tags = []
        if foreign_streak >= 5:
            tags.append(f'외인 {foreign_streak}일 매수')
        if inst_streak >= 5:
            tags.append(f'기관 {inst_streak}일 매수')
        tags.append(f'RSI {round(rsi):.0f}')
        tags.append(f'Stoch {round(stoch_cur):.0f}↑')
        if hist_cur > 0:
            tags.append('MACD+')

        score = 0
        if foreign_streak >= 10 or inst_streak >= 10: score += 3
        elif foreign_streak >= 5  or inst_streak >= 5:  score += 2
        if 45 <= rsi <= 55:   score += 2
        if stoch_cur <= 45:   score += 2
        if hist_cur > 0:      score += 2
        if vol5 >= vol20 * 1.3: score += 2
        if ma20_now > ma20_6d and ma60_now > ma60_6d: score += 2

        return {
            'name': name,
            'ticker': ticker,
            'price': f"{int(cur):,}",
            'rsi': round(rsi, 1),
            'stoch': round(stoch_cur, 1),
            'macd_hist': round(hist_cur, 4),
            'foreign_streak': foreign_streak,
            'inst_streak': inst_streak,
            'debt_ratio': debt_ratio_raw,
            'avg_tv_b': round(avg_tv / 1e8, 0),
            'tags': tags,
            'score': score,
            'sort_key': score + (foreign_streak + inst_streak) * 0.1,
        }
    except Exception:
        return None


def scan_buy_candidates(top_n=10, max_workers=8):
    """매수 후보(단기) 스캔 — 캐시 기술·펀더멘털 → Naver 시총·관리종목 2단계 필터"""
    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    ticker_list = [(n, t) for n, t in tickers.items() if not _is_etf(n)]

    # Phase 1: 캐시 기반 기술·펀더멘털 필터
    phase1 = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_check_buy_candidate, n, t): (n, t) for n, t in ticker_list}
        for f in as_completed(futures):
            r = f.result()
            if r:
                phase1.append(r)

    print(f'[매수후보스캔] Phase1(기술·펀더멘털) 통과: {len(phase1)}개')

    # Phase 2: Naver 시총 3,000억↑ + 투자주의·경고·위험 제외
    def _naver_candidate_filter(r):
        try:
            url = f"https://finance.naver.com/item/main.naver?code={r['ticker']}"
            res = requests.get(url, headers=_NAVER_HDR, timeout=5)
            soup = BeautifulSoup(res.content.decode('utf-8', errors='replace'), 'html.parser')

            # 투자주의·경고·위험·관리종목 제외
            page_text = soup.get_text()
            danger_keywords = ('관리종목', '투자주의', '투자경고', '투자위험', '불성실공시')
            if any(kw in page_text for kw in danger_keywords):
                return None

            # 시총 3,000억↑
            cap_el = soup.select_one('#_market_sum')
            if cap_el:
                raw = cap_el.text.replace(',', '').strip()
                try:
                    mc = int(raw)
                    if mc < 3000:
                        return None
                    r['market_cap'] = mc
                except Exception:
                    pass
        except Exception:
            pass
        return r

    phase2 = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_naver_candidate_filter, r): r for r in phase1}
        for f in as_completed(futures):
            r = f.result()
            if r:
                phase2.append(r)

    print(f'[매수후보스캔] Phase2(시총·관리종목) 통과: {len(phase2)}개')

    phase2.sort(key=lambda x: x['sort_key'], reverse=True)
    return phase2[:top_n]


def _check_surge_phase1(name, ticker):
    """급등주 후보 Phase1: 캐시 기반 기술·펀더멘털 점수화 (0~65점)"""
    try:
        cached = load_stock_cache(ticker)
        if not cached:
            return None

        ohlcv       = cached['ohlcv']
        investor_df = cached['investor_df']
        fundamental = cached.get('fundamental', {}) or {}

        if ohlcv is None or ohlcv.empty or len(ohlcv) < 65:
            return None

        df  = calc_indicators(ohlcv)
        last = df.iloc[-1]
        cur  = float(last['close'])

        score = 0
        tags  = []

        # ── 거래대금 (hard: 50억 미만 제외) ──────────────────
        avg_tv = (ohlcv['close'] * ohlcv['volume']).tail(20).mean()
        if avg_tv < 5_000_000_000:
            return None
        if avg_tv >= 10_000_000_000:
            score += 8; tags.append('거래대금100억↑')
        else:
            score += 4; tags.append('거래대금50억↑')

        # ── 펀더멘털 ─────────────────────────────────────────
        # 부채비율 200% 이상 → 제외 (hard)
        debt_raw = fundamental.get('debt_ratio', 'N/A')
        if debt_raw != 'N/A':
            try:
                dr = float(str(debt_raw).replace('%', '').replace(',', ''))
                if dr >= 200:
                    return None
            except Exception:
                pass

        # 최근 4분기 흑자 (hard)
        op_list = fundamental.get('operating_profit', [])
        if op_list and len(op_list) >= 4:
            parsed = [_parse_op_val(v) for v in op_list[:4]]
            if any(v is not None and v <= 0 for v in parsed):
                return None

        # 실적 YoY 개선
        if op_list and len(op_list) >= 4:
            p = [_parse_op_val(v) for v in op_list[:4]]
            if p[0] is not None and p[3] is not None and p[3] > 0 and p[0] > p[3]:
                score += 7; tags.append('YoY개선')

        # ── 이동평균선 ───────────────────────────────────────
        if 'ma60' not in df.columns or pd.isna(last['ma60']):
            return None
        ma60 = float(last['ma60'])
        if cur <= ma60:
            return None                          # hard: 60일선 아래면 제외
        score += 4; tags.append('60일선위')

        if 'ma20' in df.columns and len(df) >= 66:
            ma20_now = float(df['ma20'].iloc[-1])
            ma20_6d  = float(df['ma20'].iloc[-7])
            ma60_6d  = float(df['ma60'].iloc[-7])
            if not pd.isna(ma20_now) and not pd.isna(ma20_6d) and ma20_now > ma20_6d:
                score += 3
            if not pd.isna(ma60) and not pd.isna(ma60_6d) and ma60 > ma60_6d:
                score += 2; tags.append('MA우상향')

        # 20일/60일 신고가 돌파
        if len(ohlcv) >= 61:
            h20 = float(ohlcv['high'].iloc[-21:-1].max())
            h60 = float(ohlcv['high'].iloc[-61:-1].max())
            if cur > h60:
                score += 12; tags.append('60일신고가')
            elif cur > h20:
                score += 8;  tags.append('20일신고가')

        # ── RSI 50~70 ────────────────────────────────────────
        if 'rsi' in df.columns and not pd.isna(last['rsi']):
            rsi = float(last['rsi'])
            if 50 <= rsi <= 70:
                score += 5; tags.append(f'RSI{round(rsi):.0f}')
            elif rsi > 70:
                score += 2

        # ── MACD ─────────────────────────────────────────────
        macd_val = float(last['macd'])  if ('macd'      in df.columns and not pd.isna(last['macd']))      else None
        hist_val = float(last['macd_hist']) if ('macd_hist' in df.columns and not pd.isna(last['macd_hist'])) else None
        if macd_val is not None and macd_val > 0:
            score += 3; tags.append('MACD0선위')
        if hist_val is not None:
            h_series = df['macd_hist'].tail(5).dropna()
            if len(h_series) >= 4 and hist_val > float(h_series.iloc[-4]):
                score += 2; tags.append('히스토그램↑')

        # ── 수급 ─────────────────────────────────────────────
        frgn = _count_consecutive_buying(investor_df, '외국인') or _count_consecutive_buying(investor_df, '외인')
        inst = _count_consecutive_buying(investor_df, '기관')
        if frgn >= 3 or inst >= 3:
            score += 8; tags.append(f'수급↑{max(frgn,inst)}일')
        elif frgn >= 1 or inst >= 1:
            score += 3

        if score < 22:           # 당일 데이터 없이 너무 낮으면 조기 탈락
            return None

        return {
            'name': name, 'ticker': ticker,
            'price': f"{int(cur):,}",
            'score': score, 'tags': tags,
            'debt_ratio': debt_raw,
            'avg_tv_b': round(avg_tv / 1e8, 0),
            'foreign_streak': frgn, 'inst_streak': inst,
            '_vol20': float(ohlcv['volume'].tail(20).mean()),
            '_prev_close': float(ohlcv['close'].iloc[-2]) if len(ohlcv) >= 2 else cur,
        }
    except Exception:
        return None


def _enrich_surge_today(r):
    """Phase3: 당일 실시간 OHLCV로 추가 점수 (최대+45점) + DART 재료 (+8점)"""
    try:
        today_df = get_ohlcv(r['ticker'], months=1)
        vol20    = r.pop('_vol20', None)
        prev_cls = r.pop('_prev_close', None)

        if today_df is None or today_df.empty:
            r['sort_key'] = r['score']
            return r

        today    = today_df.iloc[-1]
        t_close  = float(today['close'])
        t_high   = float(today['high'])
        t_vol    = float(today['volume'])

        # 당일 상승률 (전일 종가 대비)
        chg_pct = (t_close - prev_cls) / prev_cls * 100 if prev_cls else 0
        r['chg_pct'] = round(chg_pct, 2)
        if 5 <= chg_pct <= 12:
            r['score'] += 12; r['tags'].append(f'+{round(chg_pct,1)}%')
        elif 3 <= chg_pct < 5:
            r['score'] += 5;  r['tags'].append(f'+{round(chg_pct,1)}%')
        elif chg_pct > 12:
            r['score'] += 2   # 너무 급등 — 모멘텀은 있지만 추격 위험

        # 당일 거래량 20일 평균 대비
        if vol20 and vol20 > 0:
            vr = t_vol / vol20
            r['vol_ratio'] = round(vr, 1)
            if vr >= 2.0:
                r['score'] += 15; r['tags'].append(f'거래량{round(vr,1)}배↑')
            elif vr >= 1.5:
                r['score'] += 8;  r['tags'].append(f'거래량{round(vr,1)}배')
            elif vr >= 1.0:
                r['score'] += 3

        # 종가가 당일 고가권 마감 (고가의 95%↑)
        if t_high > 0:
            chr_ = t_close / t_high
            r['close_high_ratio'] = round(chr_, 3)
            if chr_ >= 0.97:
                r['score'] += 10; r['tags'].append('고가권마감')
            elif chr_ >= 0.93:
                r['score'] += 5;  r['tags'].append('고가근접')

        # 오늘 가격으로 업데이트
        r['price'] = f"{int(t_close):,}"

        # ── DART 명확한 재료 확인 (+최대 8점) ─────────────────
        try:
            from analysis.dart import DART_API_KEY, get_corp_code
            if DART_API_KEY:
                corp = get_corp_code(r['ticker'])
                mom_score, mom_tags = _dart_momentum_check(corp, DART_API_KEY, days=7)
                if mom_score > 0:
                    bonus = min(mom_score, 8)
                    r['score'] += bonus
                    r['tags'] += [f'재료:{t}' for t in mom_tags[:2]]
        except Exception:
            pass

    except Exception:
        r.pop('_vol20', None)
        r.pop('_prev_close', None)

    r['sort_key'] = r['score']
    return r


def scan_surge_buy_candidates(top_n=10, max_workers=8):
    """급등주 매수후보 스캔 — 점수화 방식 (조건 일부만 충족해도 합산, 상위 10종목)
    Phase1: 캐시 기술·펀더멘털 → Phase2: Naver 시총·관리종목 → Phase3: 당일 실시간+DART
    """
    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    ticker_list = [(n, t) for n, t in tickers.items() if not _is_etf(n)]

    # Phase 1
    phase1 = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_check_surge_phase1, n, t): (n, t) for n, t in ticker_list}
        for f in as_completed(futures):
            r = f.result()
            if r:
                phase1.append(r)
    print(f'[급등후보스캔] Phase1: {len(phase1)}개')

    # Phase 2: Naver 시총 3,000억↑ + 투자주의·경고·위험 제외
    def _surge_naver_filter(r):
        try:
            url = f"https://finance.naver.com/item/main.naver?code={r['ticker']}"
            res = requests.get(url, headers=_NAVER_HDR, timeout=5)
            soup = BeautifulSoup(res.content.decode('utf-8', errors='replace'), 'html.parser')
            page_text = soup.get_text()
            if any(kw in page_text for kw in ('관리종목', '투자주의', '투자경고', '투자위험', '불성실공시')):
                return None
            cap_el = soup.select_one('#_market_sum')
            if cap_el:
                raw = cap_el.text.replace(',', '').strip()
                try:
                    mc = int(raw)
                    if mc < 3000:
                        return None
                    r['market_cap'] = mc
                except Exception:
                    pass
        except Exception:
            pass
        return r

    phase2 = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_surge_naver_filter, r): r for r in phase1}
        for f in as_completed(futures):
            r = f.result()
            if r:
                phase2.append(r)
    print(f'[급등후보스캔] Phase2: {len(phase2)}개')

    # Phase 3: 당일 실시간 데이터 + DART
    phase3 = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_enrich_surge_today, r): r for r in phase2}
        for f in as_completed(futures):
            r = f.result()
            if r and r.get('score', 0) >= 28:
                phase3.append(r)
    print(f'[급등후보스캔] Phase3: {len(phase3)}개')

    phase3.sort(key=lambda x: x.get('sort_key', 0), reverse=True)
    return phase3[:top_n]


def _naver_market_info(ticker):
    """Naver Finance에서 시총(억), PER, PBR, 관리종목 여부 반환"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=_NAVER_HDR, timeout=5)
        soup = BeautifulSoup(res.content.decode('utf-8', errors='replace'), 'html.parser')

        # 관리종목·투자유의 태그
        is_admin = bool(soup.select_one('.blind') and
                        any('관리' in el.text or '투자유의' in el.text
                            for el in soup.select('.blind')))

        market_cap = None
        per = None
        pbr = None

        # 시가총액
        cap_el = soup.select_one('#_market_sum')
        if cap_el:
            raw = cap_el.text.replace(',', '').strip()
            try:
                market_cap = int(raw)  # 억원
            except Exception:
                pass

        # PER / PBR from per_table
        table = soup.select_one('table.per_table')
        if table:
            for row in table.find_all('tr'):
                cells = row.find_all(['th', 'td'])
                for i, cell in enumerate(cells):
                    key = cell.get_text(strip=True)
                    if i + 1 >= len(cells):
                        continue
                    raw = cells[i + 1].get_text(strip=True).replace(',', '')
                    try:
                        val = float(raw)
                    except Exception:
                        continue
                    if 'PER' in key and '업종' not in key and 'Forward' not in key and per is None:
                        per = val
                    elif 'PBR' in key and pbr is None:
                        pbr = val

        return {'market_cap': market_cap, 'per': per, 'pbr': pbr, 'is_admin': is_admin}
    except Exception:
        return {}


def _dart_quality_check(corp_code, dart_key):
    """DART 연간 재무제표로 품질 필터 체크.
    반환: (pass: bool, meta: dict)
    - 3년 연속 영업이익 흑자
    - 부채비율 < 150%
    - ROE > 10%
    - 최근 매출·영업이익 YoY 증가
    """
    if not corp_code or not dart_key:
        return False, {}

    base = datetime.now().year  # 2026

    def _parse_int(s):
        """DART 금액 파싱 — '1,234,567' / '-1,234,567' / '(1,234,567)' 모두 처리"""
        s = s.replace(',', '').strip()
        if s.startswith('(') and s.endswith(')'):
            try:
                return -int(s[1:-1])
            except Exception:
                return None
        try:
            return int(s)
        except Exception:
            return None

    def _fetch(year):
        url = 'https://opendart.fss.or.kr/api/fnlttSinglAcnt.json'
        params = {'crtfc_key': dart_key, 'corp_code': corp_code,
                  'bsns_year': str(year), 'reprt_code': '11011'}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            if data.get('status') != '000':
                return {}
            items = data.get('list', [])
        except Exception:
            return {}
        d = {}
        for it in items:
            nm  = it.get('account_nm', '').replace(' ', '')
            raw = it.get('thstrm_amount', '')
            v   = _parse_int(raw)
            if v is None:
                continue
            if '매출' in nm and '증감' not in nm and '성장' not in nm and 'revenue' not in d:
                d['revenue'] = v
            elif '영업이익' in nm and '영업이익률' not in nm and 'op' not in d:
                d['op'] = v                      # 손실이면 음수로 들어옴
            elif '부채총계' in nm and 'liab' not in d:
                d['liab'] = v
            elif '자본총계' in nm and 'equity' not in d:
                d['equity'] = v
            elif '당기순이익' in nm and '지배' not in nm and 'net' not in d:
                d['net'] = v                     # 손실이면 음수로 들어옴
        return d

    y1 = _fetch(base - 1)   # 2025
    y2 = _fetch(base - 2)   # 2024
    y3 = _fetch(base - 3)   # 2023

    # 데이터 없으면 통과 (보수적 — DART 미등록 소규모 기업 등)
    if not y1:
        return False, {}

    # 3년 연속 영업이익 흑자
    for d in (y1, y2, y3):
        if not d or d.get('op', -1) <= 0:
            return False, {}

    # 부채비율 < 150%
    liab   = y1.get('liab')
    equity = y1.get('equity')
    if liab is None or equity is None or equity <= 0:
        return False, {}
    debt_ratio = liab / equity * 100
    if debt_ratio >= 150:
        return False, {}

    # ROE > 10%
    net = y1.get('net')
    if net is None:
        return False, {}
    roe = net / equity * 100
    if roe < 10:
        return False, {}

    # 매출·영업이익 YoY 증가 (둘 다 데이터 있을 때만 체크)
    if y2:
        rev1, rev2 = y1.get('revenue'), y2.get('revenue')
        op1,  op2  = y1.get('op'),      y2.get('op')
        if rev1 and rev2 and rev2 > 0 and rev1 <= rev2:
            return False, {}
        if op1 and op2 and op2 > 0 and op1 <= op2:
            return False, {}

    meta = {
        'debt_ratio': round(debt_ratio, 1),
        'roe': round(roe, 1),
    }
    if y2:
        if y1.get('revenue') and y2.get('revenue') and y2['revenue'] > 0:
            meta['revenue_yoy'] = round((y1['revenue'] - y2['revenue']) / y2['revenue'] * 100, 1)
        if y1.get('op') and y2.get('op') and y2['op'] > 0:
            meta['op_yoy'] = round((y1['op'] - y2['op']) / y2['op'] * 100, 1)
    return True, meta


def _dart_momentum_check(corp_code, dart_key, days=60):
    """DART 공시 제목에서 실적·수주·정책 모멘텀 탐지.
    반환: (bonus_score: int, tags: list[str])  bonus_score 최대 10점
    """
    if not corp_code or not dart_key:
        return 0, []
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    try:
        r = requests.get(
            'https://opendart.fss.or.kr/api/list.json',
            params={
                'crtfc_key':  dart_key,
                'corp_code':  corp_code,
                'bgn_de':     start_dt.strftime('%Y%m%d'),
                'end_de':     end_dt.strftime('%Y%m%d'),
                'page_count': 20,
                'sort': 'date', 'sort_mth': 'desc',
            },
            timeout=8,
        )
        data = r.json()
        if data.get('status') != '000':
            return 0, []
        disclosures = data.get('list', [])
    except Exception:
        return 0, []

    seen = set()
    hit_tags  = []
    hit_score = 0
    for disc in disclosures:
        title = disc.get('report_nm', '')
        for kw, tag, pts in _MOMENTUM_KW:
            if kw in title and tag not in seen:
                seen.add(tag)
                hit_tags.append(tag)
                hit_score += pts

    return min(hit_score, 10), hit_tags


def scan_top_stocks(top_n=20, months=6, max_workers=8):
    """추천 종목 스캔.
    캐시 기반 Phase1 → Naver 밸류필터 → 수급필터 → DART 펀더멘털 → 점수 정렬
    """
    from analysis.dart import DART_API_KEY, get_corp_code

    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    ticker_list = [(n, t) for n, t in tickers.items() if not _is_etf(n)]

    # ── Phase 1: 캐시 기반 점수 계산 (pykrx 호출 없음) ──────────────
    # 캐시 없는 종목은 스킵 — Railway에서 pykrx 행(hang) 방지
    phase1 = []
    def _analyze_cached(name, ticker):
        cached = load_stock_cache(ticker)
        if not cached:
            return None   # 캐시 없으면 스킵
        try:
            ohlcv       = cached['ohlcv'].tail(max(months * 22, 65))
            investor_df = cached['investor_df']
            if ohlcv is None or len(ohlcv) < 60:
                return None

            # 거래대금 50억↑ (최근 20일 평균) — 빠른 조기 탈락
            avg_tv = (ohlcv['close'] * ohlcv['volume']).tail(20).mean()
            if avg_tv < 5_000_000_000:
                return None

            df         = calc_indicators(ohlcv)
            ma_status  = get_ma_arrangement(df)
            signals    = get_latest_signals(df)
            news_result = {'total': 0, 'positive': 0, 'negative': 0, 'neutral': 0,
                           'sentiment_score': 0, 'top_keywords': [], 'press_counts': {},
                           'exclusive_count': 0, 'articles': []}

            score, reasons = calc_score(ma_status, signals, investor_df, news_result, df)
            current_price  = int(df['close'].iloc[-1])

            # 60일선 우상향 확인
            if 'ma60' in df.columns and len(df) >= 65:
                ma60_now = float(df['ma60'].iloc[-1])
                ma60_5d  = float(df['ma60'].iloc[-6])
                if pd.isna(ma60_now) or ma60_now <= ma60_5d:
                    return None

            foreign_streak    = _count_consecutive_buying(investor_df, '외국인')
            if foreign_streak == 0:
                foreign_streak = _count_consecutive_buying(investor_df, '외인')
            inst_streak       = _count_consecutive_buying(investor_df, '기관')
            joint_days, joint_star  = _calc_joint_buying(investor_df, days=20, threshold=10)
            volume_surge            = _calc_volume_surge(investor_df, days=20, surge_ratio=1.5)
            buying_surge_star       = _calc_buying_surge_star(investor_df)

            if foreign_streak >= 5:   score += 4; reasons.append(f"외인 {foreign_streak}일 연속 매수 (+4점)")
            elif foreign_streak >= 3: score += 2; reasons.append(f"외인 {foreign_streak}일 연속 매수 (+2점)")
            elif foreign_streak >= 1: score += 1
            if inst_streak >= 3:      score += 2; reasons.append(f"기관 {inst_streak}일 연속 매수 (+2점)")
            elif inst_streak >= 1:    score += 1
            if joint_star:            score += 4; reasons.append(f"외인+기관 동시매수 {joint_days}/20일 (+4점)")
            if buying_surge_star:     score += 3; reasons.append("최근 매수세 2배↑ 급증 (+3점)")
            if volume_surge:          score += 2; reasons.append("외인+기관 수급량 1.5배↑ (+2점)")

            score_pct = max(0, min(100, round((score + 14) / 50 * 100)))
            recommendation, rec_color = get_recommendation(score)

            # 오실레이터 값 추출 (시각화용)
            _last = df.iloc[-1]
            def _sv(col, default=50.0):
                try:
                    v = float(_last[col]); return default if pd.isna(v) else v
                except Exception: return default
            stoch_v  = round(_sv('stoch_k'), 1)
            mfi_v    = round(_sv('mfi'), 1)
            bb_pct_v = round(_sv('bb_pct', 0.5) * 100, 1)

            return {
                'name': name, 'ticker': ticker, 'score': score,
                'score_pct': score_pct, 'recommendation': recommendation,
                'rec_color': rec_color, 'price': f"{current_price:,}",
                'ma_label': ma_status[0], 'rsi': signals['rsi']['value'],
                'macd': signals['macd']['signal'], 'reasons': reasons[:5],
                'foreign_streak': foreign_streak, 'inst_streak': inst_streak,
                'joint_days': joint_days, 'joint_star': bool(joint_star),
                'volume_surge': bool(volume_surge),
                'buying_surge_star': bool(buying_surge_star),
                'avg_trading_value_b': round(avg_tv / 1e8, 0),
                'stoch': stoch_v, 'mfi': mfi_v, 'bb_pct': bb_pct_v,
                '_investor_df': investor_df,   # Phase3용 임시 보관
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_analyze_cached, n, t): (n, t) for n, t in ticker_list}
        for f in as_completed(futures):
            r = f.result()
            if r:
                phase1.append(r)

    print(f'[추천스캔] Phase1(캐시·거래대금·60일선) 통과: {len(phase1)}개')

    # ── Phase 2: Naver 시총·PER·PBR·관리종목 (병렬, 워커 줄여서 rate limit 방지) ─
    def _naver_filter(r):
        try:
            info = _naver_market_info(r['ticker'])
        except Exception:
            return r
        if not info:
            return r
        if info.get('is_admin'):
            return None
        mc  = info.get('market_cap')
        pbr = info.get('pbr')
        per = info.get('per')
        if mc  is not None and mc  < 5000:           return None  # 시총 5000억 미만
        if pbr is not None and pbr > 1.5:            return None  # PBR 1.5 초과
        if per is not None and (per <= 0 or per > 30): return None  # PER 범위 이탈
        r['market_cap'] = mc
        r['pbr_val']    = pbr
        r['per_val']    = per
        return r

    phase2 = []
    with ThreadPoolExecutor(max_workers=3) as ex:   # 워커 줄여서 Naver rate limit 방지
        futures = {ex.submit(_naver_filter, r): r for r in phase1}
        for f in as_completed(futures):
            r = f.result()
            if r:
                phase2.append(r)

    print(f'[추천스캔] Phase2(시총·PBR·PER) 통과: {len(phase2)}개')

    # ── Phase 3: 외인/기관 20일 누적 순매수 ──────────────────────────
    phase3 = []
    for r in phase2:
        try:
            inv    = r.pop('_investor_df', pd.DataFrame())
            last20 = inv.tail(20) if not inv.empty else pd.DataFrame()
            if last20.empty:
                phase3.append(r)
                continue
            fc = next((c for c in last20.columns if '외국인' in c or '외인' in c), None)
            ic = next((c for c in last20.columns if '기관' in c
                       and '금융' not in c and '연기금' not in c), None)
            f_net = float(last20[fc].sum()) if fc else 0
            i_net = float(last20[ic].sum()) if ic else 0
            if f_net <= 0 and i_net <= 0:
                continue
            r['f_net_20d'] = int(f_net)
            r['i_net_20d'] = int(i_net)
            phase3.append(r)
        except Exception:
            r.pop('_investor_df', None)
            phase3.append(r)

    print(f'[추천스캔] Phase3(외인/기관 수급) 통과: {len(phase3)}개')

    # ── Phase 4: DART 펀더멘털 (통과 종목만 API 호출) ────────────────
    if not DART_API_KEY:
        print('[추천스캔] DART_API_KEY 없음 — 펀더멘털 필터 생략')
        phase4 = phase3
    else:
        def _dart_filter(r):
            corp = get_corp_code(r['ticker'])
            ok, meta = _dart_quality_check(corp, DART_API_KEY)
            if not ok:
                return None
            r.update(meta)
            # 실적·수주·정책 모멘텀 보너스 (최대 +10점)
            mom_score, mom_tags = _dart_momentum_check(corp, DART_API_KEY)
            r['has_momentum']  = mom_score > 0
            r['momentum_tags'] = mom_tags
            if mom_score > 0:
                r['score'] += mom_score
                r['score_pct'] = max(0, min(100, round((r['score'] + 14) / 50 * 100)))
                if mom_tags:
                    tag_str = ', '.join(mom_tags[:2])
                    r['reasons'] = r.get('reasons', []) + [f"모멘텀: {tag_str} (+{mom_score}점)"]
            return r

        phase4 = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(_dart_filter, r): r for r in phase3}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    phase4.append(r)

    print(f'[추천스캔] Phase4(DART 펀더멘털) 통과: {len(phase4)}개')

    # 캐시 없어서 phase1=0 인 경우 폴백 — 기존 방식으로 재시도
    if not phase4 and not phase1:
        print('[추천스캔] 캐시 없음 — 기존 스캔으로 폴백')
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_analyze_one, n, t, months): n for n, t in ticker_list}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:top_n]

    phase4.sort(key=lambda x: x['score'], reverse=True)
    return phase4[:top_n]
