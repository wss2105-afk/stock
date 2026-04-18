import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
from collections import Counter


HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

POSITIVE_WORDS = ['급등', '상승', '호실적', '매수', '목표가상향', '신고가', '흑자', '성장', '수주', '호재', '강세', '돌파']
NEGATIVE_WORDS = ['급락', '하락', '부진', '매도', '목표가하향', '신저가', '적자', '감소', '손실', '악재', '약세', '폭락']


def search_naver_news(query, days=30):
    """네이버 뉴스 검색"""
    articles = []
    try:
        for page in range(1, 4):
            url = (f"https://search.naver.com/search.naver?where=news&query={query}"
                   f"&sort=1&nso=so:dd,p:{days}d&start={(page-1)*10+1}")
            res = requests.get(url, headers=HEADERS, timeout=8)
            soup = BeautifulSoup(res.text, 'html.parser')

            title_links = soup.find_all('a', {'data-heatmap-target': '.tit'})
            if not title_links:
                break

            for a in title_links:
                title = a.get_text(strip=True)
                href = a.get('href', '')

                # 상위 컨테이너에서 언론사·날짜 추출
                container = a
                press, date = '', ''
                for _ in range(8):
                    container = container.parent
                    text_parts = [s.strip() for s in container.get_text(separator='|').split('|') if s.strip()]
                    for t in text_parts:
                        if any(k in t for k in ['분 전', '시간 전', '일 전', '어제']) and not date:
                            date = t
                        if (not press and len(t) < 15 and t not in title
                                and not any(k in t for k in ['저장', '바로', '전', 'Keep'])):
                            press = t
                    if date:
                        break

                if title:
                    articles.append({
                        'title': title,
                        'press': press,
                        'date': date,
                        'url': href
                    })
    except Exception as e:
        print(f"뉴스 검색 오류: {e}")

    return articles


def analyze_news(articles):
    """뉴스 감성 분석 및 통계"""
    if not articles:
        return {
            'total': 0, 'positive': 0, 'negative': 0, 'neutral': 0,
            'sentiment_score': 0, 'top_keywords': [], 'press_counts': {},
            'exclusive_count': 0
        }

    positive = negative = exclusive = 0
    all_words = []

    for a in articles:
        title = a['title']
        pos = sum(1 for w in POSITIVE_WORDS if w in title)
        neg = sum(1 for w in NEGATIVE_WORDS if w in title)

        if pos > neg:
            positive += 1
        elif neg > pos:
            negative += 1

        if '단독' in title or '독점' in title:
            exclusive += 1

        # 키워드 추출 (2글자 이상 명사 유사)
        words = re.findall(r'[가-힣]{2,}', title)
        all_words.extend(words)

    neutral = len(articles) - positive - negative
    sentiment_score = round((positive - negative) / len(articles) * 100, 1)

    stop_words = {'이번', '지난', '오늘', '내일', '이후', '관련', '대한', '통해', '위한', '하는', '있는', '없는'}
    word_counts = Counter(w for w in all_words if w not in stop_words)
    top_keywords = word_counts.most_common(10)

    press_counts = Counter(a['press'] for a in articles if a['press'])

    return {
        'total': len(articles),
        'positive': positive,
        'negative': negative,
        'neutral': neutral,
        'sentiment_score': sentiment_score,
        'top_keywords': top_keywords,
        'press_counts': dict(press_counts.most_common(5)),
        'exclusive_count': exclusive,
        'articles': articles[:10]
    }


def get_news_signal_score(news_result):
    """뉴스 기반 신호 점수 (-2 ~ +2)"""
    if news_result['total'] == 0:
        return 0, "뉴스 없음"

    score = news_result['sentiment_score']
    volume_bonus = 1 if news_result['total'] >= 20 else 0
    exclusive_bonus = 0.5 if news_result['exclusive_count'] > 0 else 0

    if score >= 30:
        sig = 2 + exclusive_bonus
        label = f"뉴스 긍정 우세 ({news_result['positive']}건 긍정)"
    elif score >= 10:
        sig = 1
        label = "뉴스 소폭 긍정"
    elif score <= -30:
        sig = -2
        label = f"뉴스 부정 우세 ({news_result['negative']}건 부정)"
    elif score <= -10:
        sig = -1
        label = "뉴스 소폭 부정"
    else:
        sig = 0
        label = "뉴스 중립"

    return min(2, sig + volume_bonus), label
