import json, os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from analysis.data_fetcher import get_ohlcv, get_investor_detail
from analysis.indicators import calc_indicators, get_ma_arrangement, get_latest_signals
from analysis.news import search_naver_news, get_news_signal_score, analyze_news
from analysis.signal import calc_score, get_recommendation
from analysis.cache_manager import load_stock_cache

_TICKER_DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_tickers.json')


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


def scan_top_stocks(top_n=20, months=6, max_workers=16):
    with open(_TICKER_DB_PATH, encoding='utf-8') as f:
        tickers = json.load(f)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_analyze_one, name, ticker, months): name
                   for name, ticker in tickers.items()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    # 수급 보너스가 통합된 종합 점수 기준 정렬
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_n]
