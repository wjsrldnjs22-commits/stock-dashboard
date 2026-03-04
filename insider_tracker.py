#!/usr/bin/env python3
import os, json, logging
import pandas as pd
import yfinance as yf
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InsiderTracker:
    def __init__(self, data_dir: str = '.'):
        self.output_file = os.path.join(data_dir, 'insider_moves.json')
        
    def get_insider_activity(self, ticker: str):
        try:
            stock = yf.Ticker(ticker)
            df = stock.insider_transactions
            if df is None or df.empty: return []
            
            # Filter buys in last 6 months
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=180)
            df = df.sort_index(ascending=False)
            
            recent_buys = []
            for date, row in df.iterrows():
                if date < cutoff: continue
                text = str(row.get('Text', '')).lower()
                if 'purchase' not in text and 'buy' not in text: continue
                
                recent_buys.append({
                    'date': str(date.date()),
                    'insider': row.get('Insider', 'N/A'),
                    'value': float(row.get('Value', 0) or 0),
                    'shares': int(row.get('Shares', 0) or 0)
                })
            return recent_buys
        except: return []

    def analyze_tickers(self, tickers):
        results = {}
        for t in tickers:
            activities = self.get_insider_activity(t)
            if activities:
                score = sum(10 for a in activities if a['value'] > 100000)
                results[t] = {'score': score, 'transactions': activities[:5]}
        
        with open(self.output_file, 'w') as f:
            json.dump({'details': results}, f, indent=2)
        logger.info("Saved insider_moves.json")

if __name__ == "__main__":
    # Top stocks example
    InsiderTracker().analyze_tickers(['AAPL', 'NVDA', 'TSLA', 'MSFT', 'AMZN'])
