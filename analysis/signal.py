import os
import anthropic
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'), override=True)


def calc_score(ma_status, signals, investor_df, news_result):
    """점수 기반 매수/매도 신호 계산"""
    score = 0
    reasons = []

    # 1. 이동평균 배열
    ma_label, ma_type = ma_status
    ma_map = {'bullish': 2, 'mild_bullish': 1, 'neutral': 0, 'mild_bearish': -1, 'bearish': -2}
    s = ma_map.get(ma_type, 0)
    score += s
    reasons.append(f"이동평균 배열: {ma_label} ({'+' if s>=0 else ''}{s}점)")

    # 2. RSI
    rsi_val = signals['rsi']['value']
    if rsi_val < 30:
        score += 2; reasons.append(f"RSI {rsi_val} → 과매도 (+2점)")
    elif rsi_val < 40:
        score += 1; reasons.append(f"RSI {rsi_val} → 매수 구간 (+1점)")
    elif rsi_val > 70:
        score -= 2; reasons.append(f"RSI {rsi_val} → 과매수 (-2점)")
    elif rsi_val > 60:
        score -= 1; reasons.append(f"RSI {rsi_val} → 주의 구간 (-1점)")

    # 3. MACD
    macd_sig = signals['macd']['signal']
    if macd_sig == 'golden':
        score += 2; reasons.append("MACD 골든크로스 (+2점)")
    elif macd_sig == 'dead':
        score -= 2; reasons.append("MACD 데드크로스 (-2점)")
    elif signals['macd']['hist'] > 0:
        score += 1; reasons.append("MACD 히스토그램 양전환 (+1점)")
    elif signals['macd']['hist'] < 0:
        score -= 1; reasons.append("MACD 히스토그램 음전환 (-1점)")

    # 4. Stochastic
    sk = signals['stoch']['k']
    if sk < 20:
        score += 1; reasons.append(f"Stochastic {sk} → 과매도 (+1점)")
    elif sk > 80:
        score -= 1; reasons.append(f"Stochastic {sk} → 과매수 (-1점)")

    # 5. 볼린저밴드
    bb_pct = signals['bb']['pct']
    if bb_pct < 0.1:
        score += 1; reasons.append("볼린저밴드 하단 근접 (+1점)")
    elif bb_pct > 0.9:
        score -= 1; reasons.append("볼린저밴드 상단 근접 (-1점)")

    # 6. MFI
    mfi = signals['mfi']['value']
    if mfi < 20:
        score += 1; reasons.append(f"MFI {mfi} → 과매도 (+1점)")
    elif mfi > 80:
        score -= 1; reasons.append(f"MFI {mfi} → 과매수 (-1점)")

    # 7. 수급 (외인/기관)
    if not investor_df.empty:
        try:
            last = investor_df.iloc[-1]
            cols = investor_df.columns.tolist()

            foreign_col = next((c for c in cols if '외국인' in c or '외인' in c), None)
            inst_col = next((c for c in cols if '기관' in c and '금융' not in c and '연기금' not in c), None)

            if foreign_col and last[foreign_col] > 0:
                score += 2; reasons.append("외국인 순매수 (+2점)")
            elif foreign_col and last[foreign_col] < 0:
                score -= 2; reasons.append("외국인 순매도 (-2점)")

            if inst_col and last[inst_col] > 0:
                score += 1; reasons.append("기관 순매수 (+1점)")
            elif inst_col and last[inst_col] < 0:
                score -= 1; reasons.append("기관 순매도 (-1점)")
        except Exception:
            pass

    # 8. 뉴스
    from analysis.news import get_news_signal_score
    news_score, news_label = get_news_signal_score(news_result)
    score += news_score
    reasons.append(f"뉴스: {news_label} ({'+' if news_score>=0 else ''}{news_score}점)")

    return score, reasons


def get_recommendation(score):
    if score >= 7:
        return "강력 매수", "danger"  # 빨강
    elif score >= 4:
        return "매수 고려", "warning"  # 주황
    elif score >= 1:
        return "중립 (관망)", "secondary"
    elif score >= -3:
        return "관망 / 비중축소", "info"
    elif score >= -6:
        return "매도 고려", "primary"
    else:
        return "강력 매도", "dark"


def get_ai_analysis(ticker_name, score, reasons, signals, fundamental, news_result):
    """Claude API로 종합 분석 코멘트 생성"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "AI 분석을 위해 ANTHROPIC_API_KEY 환경변수를 설정해 주세요."

    client = anthropic.Anthropic(api_key=api_key)

    news_summary = f"뉴스 총 {news_result['total']}건, 긍정 {news_result['positive']}건, 부정 {news_result['negative']}건"
    keywords = ', '.join([kw for kw, _ in news_result.get('top_keywords', [])[:5]])

    prompt = f"""당신은 한국 주식 시장 전문 애널리스트입니다. 아래 데이터를 바탕으로 투자자에게 명확하고 실용적인 분석을 제공하세요.

종목명: {ticker_name}
종합 점수: {score}점
추천: {get_recommendation(score)[0]}

[기술적 지표]
- RSI: {signals['rsi']['value']} ({signals['rsi']['signal']})
- MACD 신호: {signals['macd']['signal']}
- Stochastic K: {signals['stoch']['k']}
- 볼린저밴드 위치: {signals['bb']['pct']} (0=하단, 1=상단)
- MFI: {signals['mfi']['value']}

[펀더멘털]
- PER: {fundamental.get('per', 'N/A')}
- Forward PER: {fundamental.get('forward_per', 'N/A')}
- PBR: {fundamental.get('pbr', 'N/A')}
- 영업이익 추이: {', '.join(fundamental.get('operating_profit', []))}억원

[뉴스]
- {news_summary}
- 주요 키워드: {keywords}

[신호 요약]
{chr(10).join(reasons)}

위 데이터를 종합하여 다음을 작성하세요:
1. 현재 상황 요약 (2-3문장)
2. 매수/매도 타이밍 판단 근거 (핵심 3가지)
3. 주의해야 할 리스크 (1-2가지)
4. 단기/중기 전망 (각 1문장)

전문적이지만 초보 투자자도 이해할 수 있게 설명하세요."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        return _fallback_analysis(score, reasons, signals, fundamental) + f"\n\n[API 오류: {error_msg}]"


def get_business_description(name, industry, ticker):
    """Claude API로 기업 사업 내용 및 매출 구조 설명 생성"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "API 키가 설정되지 않았습니다."

    client = anthropic.Anthropic(api_key=api_key)
    prompt = f"""한국 상장기업 '{name}' (종목코드: {ticker}, 업종코드: {industry})에 대해 아래 항목을 간결하게 설명해 주세요.

1. 주요 사업 내용 (어떤 제품/서비스를 만드는지, 2~3문장)
2. 주요 매출원 (어디서 돈을 버는지, 매출 비중이 큰 순서로)
3. 주요 고객 및 시장 (국내/해외, B2B/B2C 등)
4. 대표 경쟁사 또는 산업 내 위치 (1~2문장)

초보 투자자도 쉽게 이해할 수 있게 간단한 언어로 작성하세요. 총 200~300자 내외."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except Exception as e:
        return f"사업 설명을 불러오는 중 오류가 발생했습니다: {type(e).__name__}"


def _fallback_analysis(score, reasons, signals, fundamental):
    rec, _ = get_recommendation(score)
    rsi = signals['rsi']['value']
    macd = signals['macd']['signal']
    bb = signals['bb']['pct']

    lines = [f"[ 종합 점수: {score}점 → {rec} ]", ""]
    lines.append("■ 기술적 지표 요약")
    lines.append(f"  - RSI {rsi}: {'과매도 구간 (매수 고려)' if rsi < 30 else '과매수 구간 (주의)' if rsi > 70 else '중립 구간'}")
    lines.append(f"  - MACD: {'골든크로스 발생 (상승 신호)' if macd=='golden' else '데드크로스 발생 (하락 신호)' if macd=='dead' else '크로스 없음'}")
    lines.append(f"  - 볼린저밴드 위치: {round(bb*100)}% ({'하단 근접' if bb < 0.2 else '상단 근접' if bb > 0.8 else '중간'})")
    lines.append("")
    lines.append("■ 신호 근거")
    for r in reasons[:5]:
        lines.append(f"  {r}")
    lines.append("")
    per = fundamental.get('per', 'N/A')
    pbr = fundamental.get('pbr', 'N/A')
    lines.append(f"■ 펀더멘털: PER {per} / PBR {pbr}")
    lines.append("")
    lines.append("※ AI 크레딧 부족으로 자동 분석 텍스트가 표시됩니다. Anthropic 콘솔에서 크레딧 충전 후 AI 분석이 활성화됩니다.")
    return "\n".join(lines)
