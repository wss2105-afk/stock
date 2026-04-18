import json, os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from analysis.data_fetcher import get_ohlcv, get_investor_detail
from analysis.indicators import calc_indicators, get_ma_arrangement, get_latest_signals
from analysis.news import search_naver_news, get_news_signal_score, analyze_news
from analysis.signal import calc_score, get_recommendation

_TICKER_DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'krx_tickers.json')


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
        recommendation, rec_color = get_recommendation(score)
        current_price = int(df['close'].iloc[-1])

        return {
            'name': name,
            'ticker': ticker,
            'score': score,
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


def scan_top_stocks(top_n=10, months=6, max_workers=8):
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

    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_n]
