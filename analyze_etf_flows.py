import os
import json
import logging
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETFFlowAnalyzer:
    def __init__(self, data_dir='.'):
        self.data_dir = data_dir
        self.output_csv = os.path.join(data_dir, 'us_etf_flows.csv')
        self.output_json = os.path.join(data_dir, 'etf_flow_analysis.json')
        self.api_key = os.getenv('GOOGLE_API_KEY')
        
        # 24 Major ETFs
        self.etfs = [
            'SPY', 'QQQ', 'DIA', 'IWM',  # Broad Market
            'XLK', 'XLF', 'XLV', 'XLE', 'XLY', 'XLP', 'XLI', 'XLB', 'XLU', 'XLRE', 'XLC', # Sectors
            'GLD', 'SLV', 'USO', 'UNG', 'TLT', 'IEF', 'HYG', 'LQD', 'VNQ' # Commodities & Bonds & Real Estate
        ]

    def calculate_flow_proxy(self) -> pd.DataFrame:
        logger.info(f"📊 Fetching data for {len(self.etfs)} ETFs...")
        results = []
        try:
            data = yf.download(self.etfs, period="2mo", progress=False)
            if data.empty:
                return pd.DataFrame()
            
            for ticker in self.etfs:
                try:
                    if ticker not in data['Close'].columns:
                        continue
                    
                    df = pd.DataFrame({
                        'close': data['Close'][ticker],
                        'volume': data['Volume'][ticker]
                    }).dropna()
                    
                    if len(df) < 20:
                        continue
                        
                    # Calculate simple Flow Score based on recent volume and price change
                    recent_vol = df['volume'].tail(5).mean()
                    prev_vol = df['volume'].tail(20).mean()
                    vol_ratio = recent_vol / prev_vol if prev_vol > 0 else 1
                    
                    price_change = (df['close'].iloc[-1] / df['close'].iloc[-20]) - 1
                    
                    # Flow Score (0-100)
                    score = 50
                    if price_change > 0 and vol_ratio > 1.2:
                        score += 30
                    elif price_change > 0 and vol_ratio > 1.0:
                        score += 15
                    elif price_change < 0 and vol_ratio > 1.2:
                        score -= 30
                    elif price_change < 0 and vol_ratio > 1.0:
                        score -= 15
                        
                    score = max(0, min(100, score))
                    
                    results.append({
                        'ticker': ticker,
                        'price': round(df['close'].iloc[-1], 2),
                        'price_change_20d_pct': round(price_change * 100, 2),
                        'vol_ratio': round(vol_ratio, 2),
                        'flow_score': int(score)
                    })
                except Exception as e:
                    logger.debug(f"Error processing {ticker}: {e}")
                    
        except Exception as e:
            logger.error(f"Download error: {e}")
            
        return pd.DataFrame(results)

    def generate_ai_analysis(self, results_df: pd.DataFrame) -> None:
        if not self.api_key:
            logger.warning("No GOOGLE_API_KEY found. Skipping AI analysis.")
            return
            
        logger.info("🤖 Requesting Gemini 3.0 AI Analysis...")
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
        
        # Prepare context
        top_inflows = results_df.nlargest(3, 'flow_score').to_dict('records')
        top_outflows = results_df.nsmallest(3, 'flow_score').to_dict('records')
        
        prompt = f"""
        Analyze the current US ETF fund flows based on the following data:
        Top Inflows (High Flow Score): {top_inflows}
        Top Outflows (Low Flow Score): {top_outflows}
        
        Please provide a 3-4 sentence concise market insight explaining "WHY" money is moving this way. 
        Write the response in Korean. Do not use markdown or emojis.
        """
        
        insight = "AI 분석을 불러오지 못했습니다."
        try:
            payload = {"contents": [{"parts": [{"text": prompt}]}]}
            resp = requests.post(f"{url}?key={self.api_key}", json=payload)
            if resp.status_code == 200:
                insight = resp.json()['candidates'][0]['content']['parts'][0]['text']
            else:
                logger.error(f"API Error: {resp.text}")
        except Exception as e:
            logger.error(f"AI Generation Failed: {e}")
            
        # Save JSON
        json_out = {
            'timestamp': datetime.now().isoformat(),
            'top_inflows': top_inflows,
            'top_outflows': top_outflows,
            'ai_insight': insight.strip()
        }
        
        with open(self.output_json, 'w', encoding='utf-8') as f:
            json.dump(json_out, f, indent=2, ensure_ascii=False)
            
        logger.info(f"✅ AI Insight: {insight.strip()}")

    def run(self):
        df = self.calculate_flow_proxy()
        if not df.empty:
            df = df.sort_values('flow_score', ascending=False)
            df.to_csv(self.output_csv, index=False)
            logger.info(f"✅ Saved ETF flows to {self.output_csv}")
            
            self.generate_ai_analysis(df)
        else:
            logger.error("No ETF data processed.")

if __name__ == "__main__":
    analyzer = ETFFlowAnalyzer()
    analyzer.run()
