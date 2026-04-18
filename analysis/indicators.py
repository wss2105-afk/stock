import pandas as pd
import ta


def calc_indicators(df):
    close = df['close']
    high = df['high']
    low = df['low']
    volume = df['volume']

    # 이동평균선
    df['ma5'] = close.rolling(5).mean()
    df['ma20'] = close.rolling(20).mean()
    df['ma60'] = close.rolling(60).mean()
    df['ma115'] = close.rolling(115).mean()

    # RSI
    df['rsi'] = ta.momentum.RSIIndicator(close, window=14).rsi()

    # MACD
    macd = ta.trend.MACD(close, window_fast=12, window_slow=26, window_sign=9)
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    df['macd_hist'] = macd.macd_diff()

    # Stochastic
    stoch = ta.momentum.StochasticOscillator(high, low, close, window=14, smooth_window=3)
    df['stoch_k'] = stoch.stoch()
    df['stoch_d'] = stoch.stoch_signal()

    # 볼린저밴드
    bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_mid'] = bb.bollinger_mavg()
    df['bb_lower'] = bb.bollinger_lband()
    df['bb_pct'] = bb.bollinger_pband()

    # MFI
    df['mfi'] = ta.volume.MFIIndicator(high, low, close, volume, window=14).money_flow_index()

    return df


def get_ma_arrangement(df):
    last = df.iloc[-1]
    ma5, ma20, ma60, ma115 = last['ma5'], last['ma20'], last['ma60'], last['ma115']

    if pd.isna(ma115):
        return "데이터 부족", "neutral"

    if ma5 > ma20 > ma60 > ma115:
        return "정배열 (강한 상승추세)", "bullish"
    elif ma5 < ma20 < ma60 < ma115:
        return "역배열 (하락추세)", "bearish"
    elif ma5 > ma20 and ma20 > ma60:
        return "단기 정배열 (상승 전환 중)", "mild_bullish"
    elif ma5 < ma20 and ma20 < ma60:
        return "단기 역배열 (하락 전환 중)", "mild_bearish"
    else:
        return "혼조 (방향성 불명확)", "neutral"


def get_latest_signals(df):
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    signals = {}

    rsi = last['rsi']
    signals['rsi'] = {'value': round(rsi, 1),
                      'signal': 'oversold' if rsi < 30 else 'overbought' if rsi > 70 else 'neutral'}

    macd_cross = 'golden' if last['macd'] > last['macd_signal'] and prev['macd'] <= prev['macd_signal'] else \
                 'dead' if last['macd'] < last['macd_signal'] and prev['macd'] >= prev['macd_signal'] else 'neutral'
    signals['macd'] = {'value': round(last['macd'], 2), 'signal': macd_cross,
                       'hist': round(last['macd_hist'], 2)}

    sk = last['stoch_k']
    signals['stoch'] = {'k': round(sk, 1), 'd': round(last['stoch_d'], 1),
                        'signal': 'oversold' if sk < 20 else 'overbought' if sk > 80 else 'neutral'}

    bb_pct = last['bb_pct']
    signals['bb'] = {
        'upper': round(last['bb_upper']),
        'mid': round(last['bb_mid']),
        'lower': round(last['bb_lower']),
        'pct': round(bb_pct, 2),
        'signal': 'oversold' if bb_pct < 0.1 else 'overbought' if bb_pct > 0.9 else 'neutral'
    }

    mfi = last['mfi']
    signals['mfi'] = {'value': round(mfi, 1),
                      'signal': 'oversold' if mfi < 20 else 'overbought' if mfi > 80 else 'neutral'}

    return signals
