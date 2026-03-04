#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, logging
import yfinance as yf
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptionsFlowAnalyzer:
    def __init__(self):
        self.watchlist = ['AAPL', 'NVDA', 'TSLA', 'MSFT', 'AMZN', 'META', 'GOOGL', 'SPY', 'QQQ', 'AMD']
    
    def get_options_summary(self, ticker: str):
        try:
            stock = yf.Ticker(ticker)
            exps = stock.options
            if not exps: return {'error': 'No options'}
            
            opt = stock.option_chain(exps[0])
            calls, puts = opt.calls, opt.puts
            
            call_vol, put_vol = calls['volume'].sum(), puts['volume'].sum()
            call_oi, put_oi = calls['openInterest'].sum(), puts['openInterest'].sum()
            
            pc_ratio = put_vol / call_vol if call_vol > 0 else 0
            
            # Unusual activity
            avg_call = calls['volume'].mean()
            unusual_calls = len(calls[calls['volume'] > avg_call * 3])
            unusual_puts = len(puts[puts['volume'] > puts['volume'].mean() * 3])
            
            return {
                'ticker': ticker,
                'metrics': {
                    'pc_ratio': round(pc_ratio, 2),
                    'call_vol': int(call_vol), 'put_vol': int(put_vol),
                    'call_oi': int(call_oi), 'put_oi': int(put_oi)
                },
                'unusual': {'calls': unusual_calls, 'puts': unusual_puts}
            }
        except Exception as e:
            return {'error': str(e)}

    def analyze_watchlist(self):
        results = []
        for t in self.watchlist:
            res = self.get_options_summary(t)
            if 'error' not in res: results.append(res)
        
        with open('options_flow.json', 'w') as f:
            json.dump({'options_flow': results}, f, indent=2)
        logger.info("Saved options_flow.json")

if __name__ == "__main__":
    OptionsFlowAnalyzer().analyze_watchlist()
