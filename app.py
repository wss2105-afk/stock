from flask import Flask, render_template, request, jsonify
from analysis.screener import scan_top_stocks
from analysis.data_fetcher import get_ticker, get_ohlcv, get_investor_detail, get_supply_zone
from analysis.indicators import calc_indicators, get_ma_arrangement, get_latest_signals
from analysis.fundamental import get_fundamental
from analysis.news import search_naver_news, analyze_news
from analysis.signal import calc_score, get_recommendation, get_ai_analysis
from analysis.charts import make_main_chart, make_supply_zone_chart, make_investor_chart, make_ma_chart
from analysis.dart import get_disclosures, get_company_info
from analysis.fundamental import get_market_profile
from dotenv import load_dotenv
import os
import json
import threading
from datetime import datetime

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=True)
app = Flask(__name__)

_TICKER_PATH = os.path.join(os.path.dirname(__file__), 'data', 'krx_tickers.json')
_LAST_UPDATE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'ticker_last_update.txt')


def _auto_update_tickers():
    """매월 1일 종목 DB 자동 갱신"""
    today = datetime.today()
    if today.day != 1:
        return
    last = ''
    if os.path.exists(_LAST_UPDATE_PATH):
        with open(_LAST_UPDATE_PATH) as f:
            last = f.read().strip()
    this_month = today.strftime('%Y-%m')
    if last == this_month:
        return
    try:
        from update_tickers import update
        update()
        with open(_LAST_UPDATE_PATH, 'w') as f:
            f.write(this_month)
        print(f'[{this_month}] 종목 DB 자동 갱신 완료')
    except Exception as e:
        print(f'종목 DB 갱신 오류: {e}')


threading.Thread(target=_auto_update_tickers, daemon=True).start()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/analyze', methods=['POST'])
def analyze():
    query = request.form.get('query', '').strip()
    months = int(request.form.get('months', 3))

    if not query:
        return render_template('index.html', error="종목명 또는 코드를 입력하세요.")

    ticker, name = get_ticker(query)
    if not ticker:
        return render_template('index.html', error=f"'{query}' 종목을 찾을 수 없습니다.")

    # 데이터 수집
    ohlcv = get_ohlcv(ticker, months)
    if ohlcv.empty:
        return render_template('index.html', error="주가 데이터를 불러올 수 없습니다.")

    # 지표 계산
    df = calc_indicators(ohlcv)
    ma_status = get_ma_arrangement(df)
    signals = get_latest_signals(df)

    # 수급
    try:
        investor_df = get_investor_detail(ticker, months)
    except Exception:
        investor_df = __import__('pandas').DataFrame()

    # 매물대
    try:
        supply_df = get_supply_zone(ticker, max(months, 6))
    except Exception:
        supply_df = __import__('pandas').DataFrame({'price_mid': [], 'volume': []})
    current_price = int(df['close'].iloc[-1])

    # 펀더멘털
    try:
        fundamental = get_fundamental(ticker)
    except Exception:
        fundamental = {'per': 'N/A', 'forward_per': 'N/A', 'pbr': 'N/A', 'operating_profit': [],
                       'roe': 'N/A', 'op_margin': 'N/A', 'debt_ratio': 'N/A', 'revenue': []}

    # 기업 프로필
    try:
        company_info = get_company_info(ticker)
    except Exception:
        company_info = {}
    try:
        market_profile = get_market_profile(ticker)
    except Exception:
        market_profile = {'market_cap': 'N/A', 'w52_high': 'N/A', 'w52_low': 'N/A', 'market_type': 'N/A'}

    # 뉴스
    try:
        articles = search_naver_news(name, days=30)
        news_result = analyze_news(articles)
    except Exception:
        news_result = {'total': 0, 'positive': 0, 'negative': 0, 'neutral': 0,
                       'sentiment_score': 0, 'top_keywords': [], 'press_counts': {},
                       'exclusive_count': 0, 'articles': []}

    # 신호 계산
    score, reasons = calc_score(ma_status, signals, investor_df, news_result)
    recommendation, rec_color = get_recommendation(score)
    score_pct = max(0, min(100, round((score + 14) / 28 * 100)))

    # DART 공시
    try:
        disclosures = get_disclosures(ticker, days=60)
    except Exception:
        disclosures = []

    # AI 분석
    try:
        ai_comment = get_ai_analysis(name, score, reasons, signals, fundamental, news_result)
    except Exception as e:
        ai_comment = f"AI 분석 오류: {str(e)}"

    # 차트
    main_chart = make_main_chart(df, name)
    ma_chart = make_ma_chart(df, name)
    try:
        supply_chart = make_supply_zone_chart(supply_df, current_price)
    except Exception:
        supply_chart = None
    try:
        investor_chart = make_investor_chart(investor_df)
    except Exception:
        investor_chart = None

    return render_template('result.html',
        disclosures=disclosures,
        company_info=company_info,
        market_profile=market_profile,
        name=name, ticker=ticker, current_price=f"{current_price:,}",
        ma_chart=ma_chart, score_pct=score_pct,
        months=months,
        ma_label=ma_status[0], ma_type=ma_status[1],
        signals=signals,
        fundamental=fundamental,
        score=score, recommendation=recommendation, rec_color=rec_color,
        reasons=reasons,
        news=news_result,
        ai_comment=ai_comment,
        main_chart=main_chart,
        supply_chart=supply_chart,
        investor_chart=investor_chart
    )


@app.route('/recommend')
def recommend():
    results = scan_top_stocks(top_n=20, months=6)
    return render_template('recommend.html', results=results)


if __name__ == '__main__':
    app.run(debug=True, port=5000)
