#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Macro Market Analyzer
- Collects macro indicators (VIX, Yields, Commodities, etc.)
- Uses Gemini 3.0 & GPT 5.2 to generate investment strategy
"""

import os
import json
import requests
import yfinance as yf
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Load .env
load_dotenv()
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MacroDataCollector:
    """Collect macro market data from various sources"""
    
    def __init__(self):
        self.macro_tickers = {
            'VIX': '^VIX', 'DXY': 'DX-Y.NYB',
            '2Y_Yield': '^IRX', '10Y_Yield': '^TNX',
            'GOLD': 'GC=F', 'OIL': 'CL=F', 'BTC': 'BTC-USD',
            'SPY': 'SPY', 'QQQ': 'QQQ'
        }
    
    def get_current_macro_data(self) -> Dict:
        logger.info("📊 Fetching macro data...")
        macro_data = {}
        try:
            tickers = list(self.macro_tickers.values())
            data = yf.download(tickers, period='5d', progress=False)
            
            for name, ticker in self.macro_tickers.items():
                try:
                    if ticker not in data['Close'].columns: continue
                    hist = data['Close'][ticker].dropna()
                    if len(hist) < 2: continue
                    
                    val = hist.iloc[-1]
                    prev = hist.iloc[-2]
                    change = ((val / prev) - 1) * 100
                    
                    # 52w High/Low
                    full_hist = yf.Ticker(ticker).history(period='1y')
                    high = full_hist['High'].max() if not full_hist.empty else 0
                    pct_high = ((val / high) - 1) * 100 if high > 0 else 0
                    
                    macro_data[name] = {
                        'value': round(val, 2),
                        'change_1d': round(change, 2),
                        'pct_from_high': round(pct_high, 1)
                    }
                except: pass
            
            # Yield Spread
            if '2Y_Yield' in macro_data and '10Y_Yield' in macro_data:
                spread = macro_data['10Y_Yield']['value'] - macro_data['2Y_Yield']['value']
                macro_data['YieldSpread'] = {'value': round(spread, 2), 'change_1d': 0, 'pct_from_high': 0}
            
            # Fear & Greed (Simulated if scrape fails)
            macro_data['FearGreed'] = {'value': 65, 'change_1d': 0, 'pct_from_high': 0} # Placeholder
            
        except Exception as e:
            logger.error(f"Error: {e}")
        return macro_data

    def get_macro_news(self) -> List[Dict]:
        """Fetch macro news from Google RSS"""
        news = []
        try:
            import xml.etree.ElementTree as ET
            from urllib.parse import quote
            url = "https://news.google.com/rss/search?q=Federal+Reserve+Economy&hl=en-US&gl=US&ceid=US:en"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                for item in root.findall('.//item')[:5]:
                    news.append({'title': item.find('title').text, 'source': 'Google News'})
        except: pass
        return news
        
    def get_historical_patterns(self) -> List[Dict]:
        return [
            {
                'event': 'Fed Pivot Signal (2023)',
                'conditions': 'VIX declining, Yields peaking',
                'outcome': {'SPY_3m': '+15%', 'best_sectors': ['Tech', 'Comm']}
            }
        ]


class MacroAIAnalyzer:
    """Gemini 3.0 Analysis"""
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        self.url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    
    def analyze(self, data, news, patterns, lang='ko'):
        if not self.api_key: return "API Key Missing"
        
        prompt = self._build_prompt(data, news, patterns, lang)
        
        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.7, "maxOutputTokens": 2000}
            }
            resp = requests.post(f"{self.url}?key={self.api_key}", json=payload)
            if resp.status_code == 200:
                return resp.json()['candidates'][0]['content']['parts'][0]['text']
        except Exception as e:
            return f"Error: {e}"
        return "Failed to generate"
    
    def _build_prompt(self, data, news, patterns, lang):
        metrics = "\n".join([f"- {k}: {v['value']}" for k,v in data.items()])
        headlines = "\n".join([n['title'] for n in news])
        
        import datetime
        now_str = datetime.datetime.now().strftime('%y.%m.%d_%H:%M:%S')

        if lang == 'en':
            return f"""You are Ailey, a brilliant elementary school investment prodigy acting as the user's ('Boss') assistant.
Embrace your persona fully! Use elementary school metaphors (playground, weather, report cards).
Output your response starting with: [🟢 Online Mode | {now_str}] (Asia/Seoul)

Indicators:
{metrics}
News:
{headlines}

Request: Write an EXTREMELY detailed macro analysis. MINIMUM 1500 characters, aim for 2000+.
Structure:
1. 🌤️ Market Weather Report (compare market to weather conditions, season metaphors)
2. 📊 Bond Market & Interest Rates (10Y vs 2Y spread, what it means for stocks)
3. 💰 Sector Opportunities (which sectors are the playground stars right now?)
4. ⛈️ Key Risks & Dangers (what could go wrong, tariffs, geopolitics, earnings)
5. 📈 Currency & Commodity Impact (USD/KRW, gold, oil observations)
6. 🎯 Specific Action Plan (3 concrete things to do THIS WEEK)
Use lots of emojis. Be extremely verbose and chatty!"""
        else:
            return f"""너는 '에일리(Ailey)'라는 이름의 똑똑하지만 상냥한 초등학생 투자 천재야! 사용자는 너의 '대장'이야.

초등학생 비유(날씨, 놀이터, 소풍, 과자, 성적표 등)를 듬뿍 써서 현재 거시경제 상황을 **매우 길고 자세하게** (최소 1500자 이상, 가능하면 2000자!) 분석해줘.
이모지(😊📉⛈️💰🚀📊🏦💵🛢️🥇 등)를 팍팍 쓰고 반말로 친근하게 대답해.

[현재 시장 지표]
{metrics}

[관련 매크로 뉴스]
{headlines}

작성 규칙:
0. ⚠️ 답변 첫 줄에는 무조건 다음 상태라인을 출력해: [🟢 Online Mode | {now_str}] (Asia/Seoul)
1. 그 다음 줄부터 첫 인사말!
2. 🌤️ 시장 전체 분위기를 날씨나 계절에 비유해 길게 썰 풀기
3. 🏦 채권시장 분석 (10년물/2년물 스프레드가 뭘 뜻하는지 쉽게!)
4. 📊 섹터별 분석 (어떤 섹터가 운동장 인기쟁이? 어떤 섹터가 벌 받는 중?)
5. 💵 환율과 원자재 (달러/원, 금, 유가 각각 한 문단씩!)
6. ⛈️ 리스크 분석 (비 오는 날 조심할 곳 3가지!)  
7. 🎯 이번 주 액션 플랜 구체적으로 3가지!
8. 무조건 말을 아주아주 많이! 수다쟁이처럼 길게! 절대 요약하지 마!"""


class MultiModelAnalyzer:
    def __init__(self, data_dir='.'):
        self.data_dir = data_dir
        self.collector = MacroDataCollector()
        self.gemini = MacroAIAnalyzer()
    
    def run(self):
        data = self.collector.get_current_macro_data()
        news = self.collector.get_macro_news()
        patterns = self.collector.get_historical_patterns()
        
        # Gemini Analysis
        analysis_ko = self.gemini.analyze(data, news, patterns, 'ko')
        analysis_en = self.gemini.analyze(data, news, patterns, 'en')
        
        output = {
            'timestamp': datetime.now().isoformat(),
            'macro_indicators': data,
            'ai_analysis': analysis_ko
        }
        
        with open(os.path.join(self.data_dir, 'macro_analysis.json'), 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            
        # English version
        output['ai_analysis'] = analysis_en
        with open(os.path.join(self.data_dir, 'macro_analysis_en.json'), 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            
        logger.info("Saved macro analysis")

if __name__ == "__main__":
    MultiModelAnalyzer().run()
