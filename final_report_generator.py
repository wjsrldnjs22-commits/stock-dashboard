#!/usr/bin/env python3
import os, json, logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)

class FinalReportGenerator:
    def __init__(self, data_dir='.'):
        self.data_dir = data_dir
        
    def run(self, top_n=10):
        # Load Quant Data
        stats_path = os.path.join(self.data_dir, 'smart_money_picks_v2.csv')
        if not os.path.exists(stats_path): return
        df = pd.read_csv(stats_path)
        
        # Load AI Data
        ai_path = os.path.join(self.data_dir, 'ai_summaries.json')
        ai_data = {}
        if os.path.exists(ai_path):
            with open(ai_path, encoding='utf-8') as f: ai_data = json.load(f)
            
        results = []
        for _, row in df.iterrows():
            ticker = row['ticker']
            if ticker not in ai_data: continue
            
            summary = ai_data[ticker].get('summary', '')
            
            # AI Bonus Score
            ai_score = 0
            rec = "Hold"
            if "매수" in summary or "Buy" in summary: 
                ai_score = 10
                rec = "Buy"
            if "적극" in summary or "Strong" in summary:
                ai_score = 20
                rec = "Strong Buy"
                
            final_score = row['composite_score'] * 0.8 + ai_score
            
            results.append({
                'ticker': ticker,
                'name': row.get('name', ticker),
                'final_score': round(final_score, 1),
                'quant_score': row['composite_score'],
                'ai_recommendation': rec,
                'current_price': row['current_price'],
                'ai_summary': summary,
                'sector': row.get('sector', 'N/A')
            })
            
        # Sort and Rank
        results.sort(key=lambda x: x['final_score'], reverse=True)
        top_picks = results[:top_n]
        for i, p in enumerate(top_picks, 1): p['rank'] = i
        
        # Save Report
        with open(os.path.join(self.data_dir, 'final_top10_report.json'), 'w', encoding='utf-8') as f:
            json.dump({'top_picks': top_picks}, f, indent=2, ensure_ascii=False)
            
        # Save for Dashboard
        with open(os.path.join(self.data_dir, 'smart_money_current.json'), 'w', encoding='utf-8') as f:
            json.dump({'picks': top_picks}, f, indent=2, ensure_ascii=False)
            
        print(f"Generated Final Report for {len(top_picks)} stocks")

if __name__ == "__main__":
    FinalReportGenerator().run()
