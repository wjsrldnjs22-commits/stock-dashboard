#!/usr/bin/env python3
import os, json, requests, logging
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

class EconomicCalendar:
    def __init__(self, data_dir='.'):
        self.output = os.path.join(data_dir, 'weekly_calendar.json')
        
    # 이벤트 한글 번역 매핑
    EVENT_KO_MAP = {
        # 통화/금리
        'FOMC': '🏛️ FOMC 금리 결정',
        'Federal Reserve': '🏛️ 미연준(FED) 관련',
        'Fed ': '🏛️ 연준 이벤트',
        'Interest Rate': '🏛️ 기준금리 결정',
        # 물가
        'CPI': '📊 소비자물가지수(CPI)',
        'PPI': '🏭 생산자물가지수(PPI)',
        'PCE': '🛍️ 개인소비지출(PCE)',
        # 고용
        'Nonfarm': '👷 비농업부문 고용',
        'Unemployment': '📉 실업률',
        'ADP': '🏢 ADP 민간 고용 변화',
        'Jobless': '📋 신규/계속 실업수당 청구',
        'JOLTS': '💼 구인구직 보고서(JOLTS)',
        'Employment': '🧑‍🏭 고용 지표',
        # 성장/생산
        'GDP': '📈 국내총생산(GDP)',
        'Factory Orders': '📦 공장 주문',
        'Durable Goods': '🛋️ 내구재 주문',
        'Industrial Production': '⚙️ 산업 생산',
        'Retail': '🛒 소매판매',
        # 주택/모기지
        'Housing': '🏠 주택 관련 지표',
        'Mortgage': '🏦 모기지(주택담보대출) 지수',
        'MBA': '🏦 MBA 모기지 지수',
        'Building Permits': '🏗️ 건축 허가 건수',
        'Home Sales': '🏘️ 주택 판매',
        # 경기지수
        'PMI': '🏗️ 구매관리자지수(PMI)',
        'ISM': '🏭 ISM 제조업/비제조업 지수',
        'Consumer Confidence': '😊 소비자신뢰지수',
        'Consumer Sentiment': '😊 소비자심리지수',
        'Michigan': '📊 미시간대 소비자심리지수',
        # 기타
        'Earnings': '🏢 주요 기업 실적발표',
        'Trade Balance': '🚢 무역수지',
        'Crude Oil': '🛢️ 원유 재고',
        'Natural Gas': '🔥 천연가스 재고',
        'Export': '📦 수출 지표',
        'Import': '📥 수입 지표'
    }
    
    def _translate_event(self, event_name):
        """이벤트명을 한글로 번역"""
        for key, ko in self.EVENT_KO_MAP.items():
            if key.lower() in event_name.lower():
                return ko
        return event_name  # 매칭 안 되면 원본 유지

    def get_events(self):
        events = []
        news_feed = []
        
        # 1. Scrape Yahoo Finance Calendar
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            url = f"https://finance.yahoo.com/calendar/economic"
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                dfs = pd.read_html(StringIO(resp.text))
                if dfs:
                    df = dfs[0]
                    if 'Country' in df.columns:
                        us = df[df['Country'] == 'US']
                    else:
                        us = df
                        
                    for _, row in us.head(10).iterrows():
                        raw_event = str(row.get('Event', 'Unknown Event'))
                        events.append({
                            'date': datetime.now().strftime('%Y-%m-%d'), 
                            'event': self._translate_event(raw_event),
                            'event_en': raw_event,
                            'impact': '높음' if 'High' in str(row) else '보통',
                            'description': f"실제: {row.get('Actual','-')} | 예상: {row.get('Market Expectation','-')}"
                        })
        except Exception as e:
            logging.error(f"Error scraping economic calendar: {e}")
            pass
            
        # 2. Fetch Daily News (Yahoo Finance RSS)
        try:
            import xml.etree.ElementTree as ET
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            rss_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=SPY,QQQ,AAPL,NVDA,TSLA"
            resp = requests.get(rss_url, headers=headers)
            if resp.status_code == 200:
                root = ET.fromstring(resp.text)
                for item in root.findall('./channel/item')[:5]:
                    news_feed.append({
                        'title': item.find('title').text,
                        'link': item.find('link').text,
                        'pubDate': item.find('pubDate').text
                    })
        except Exception as e:
            logging.error(f"Error fetching news: {e}")
            pass
        
        # Add Manual Major Events (Fallback)
        if not events:
            events.append({
                'date': datetime.now().strftime('%Y-%m-%d'), 
                'event': '🏛️ FOMC 금리 결정', 
                'impact': '높음', 
                'description': '연준 금리 결정 회의.'
            })
            
        return events, news_feed
    
    def enrich_ai(self, events):
        key = os.getenv('GOOGLE_API_KEY')
        if not key: return events
        
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
        
        for ev in events:
            if ev['impact'] == '높음':
                try:
                    prompt = f"""너는 에일리(Ailey)야! 대장에게 이 경제 이벤트가 주식 시장에 어떤 영향을 미치는지 한국어 반말로 쉽게 2~3문장으로 설명해줘. 이모지도 써줘!
이벤트: {ev['event']}"""
                    payload = {"contents": [{"parts": [{"text": prompt}]}]}
                    resp = requests.post(f"{url}?key={key}", json=payload)
                    if resp.status_code == 200:
                        ev['description'] += "\n\n👧🏻 에일리: " + resp.json()['candidates'][0]['content']['parts'][0]['text']
                except: pass
        return events

    def run(self):
        events, news_feed = self.get_events()
        events = self.enrich_ai(events)
        
        output = {
            'updated': datetime.now().isoformat(),
            'events': events,
            'news_feed': news_feed,
            'week_start': datetime.now().strftime('%Y-%m-%d')
        }
        with open(self.output, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved economic calendar ({len(events)} events, {len(news_feed)} news items)")

if __name__ == "__main__":
    EconomicCalendar().run()
