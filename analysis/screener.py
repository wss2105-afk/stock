import json, os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from analysis.data_fetcher import get_ohlcv, get_investor_detail
from analysis.indicators import calc_indicators, get_ma_arrangement, get_latest_signals
from analysis.news import search_naver_news, get_news_signal_score, analyze_news
from analysis.signal import calc_score, get_recommendation

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
    """최근 수급량이 평시 대비 surge_ratio배 이상인지 확인"""
    if investor_df.empty:
        return False
    foreign_col = next((c for c in investor_df.columns if '외국인' in c or '외인' in c), None)
    if not foreign_col:
        return False
    series = investor_df[foreign_col].abs()
    if len(series) < days * 2:
        return False
    recent_avg = series.tail(days).mean()
    past_avg = series.iloc[-(days * 2):-days].mean()
    return past_avg > 0 and recent_avg >= past_avg * surge_ratio


def _analyze_one(name, ticker, months=6):
    try:
        ohlcv = get_ohlcv(ticker, months)
        if ohlcv.empty or len(ohlcv) < 20:
            return None
        df = calc_indicators(ohlcv)
        ma_status = get_ma_arrangement(df)
        signals = get_latest_signals(df)

        try:
            investor_df = get_investor_detail(ticker, months)
        except Exception:
            investor_df = pd.DataFrame()

        try:
            articles = search_naver_news(name, days=14)
            news_result = analyze_news(articles)
        except Exception:
            news_result = {'total': 0, 'positive': 0, 'negative': 0, 'neutral': 0,
                           'sentiment_score': 0, 'top_keywords': [], 'press_counts': {},
                           'exclusive_count': 0, 'articles': []}

        score, reasons = calc_score(ma_status, signals, investor_df, news_result)
        score_pct = max(0, min(100, round((score + 14) / 28 * 100)))
        recommendation, rec_color = get_recommendation(score)
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

        # 연속 매수 보너스 점수 (정렬 우선순위용)
        streak_bonus = (foreign_streak * 2) + (inst_streak * 1.5) + (10 if joint_star else 0) + (8 if buying_surge_star else 0)

        return {
            'name': name,
            'ticker': ticker,
            'score': score,
            'score_pct': score_pct,
            'streak_bonus': streak_bonus,
            'foreign_streak': foreign_streak,
            'inst_streak': inst_streak,
            'joint_days': joint_days,
            'joint_star': joint_star,
            'volume_surge': volume_surge,
            'buying_surge_star': buying_surge_star,
            'recommendation': recommendation,
            'rec_color': rec_color,
            'price': f"{current_price:,}",
            'ma_label': ma_status[0],
            'rsi': signals['rsi']['value'],
            'macd': signals['macd']['signal'],
            'reasons': reasons[:3],
        }
    except Exception:
        return None


def scan_top_stocks(top_n=20, months=6, max_workers=8):
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

    # 외인·기관 연속 매수 우선, 그 다음 종합 점수
    results.sort(key=lambda x: (x['streak_bonus'], x['score']), reverse=True)
    return results[:top_n]
