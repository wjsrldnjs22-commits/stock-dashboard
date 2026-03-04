#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
US 13F Institutional Holdings Analysis
Fetches and analyzes institutional holdings from SEC EDGAR
"""

import os
import pandas as pd
import numpy as np
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import time

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SEC13FAnalyzer:
    """
    Analyze institutional holdings from SEC 13F filings
    Note: 13F filings are quarterly, with 45-day delay after quarter end
    """
    
    def __init__(self, data_dir: str = '.'):
        self.data_dir = data_dir
        self.output_file = os.path.join(data_dir, 'us_13f_holdings.csv')
        self.cache_file = os.path.join(data_dir, 'us_13f_cache.json')
        
        # SEC EDGAR API base URL
        self.sec_base_url = "https://data.sec.gov"
        
        # User-Agent required by SEC
        self.headers = {
            'User-Agent': 'StockAnalysis/1.0 (contact@example.com)',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        }
        
        # Major institutional investors (CIK numbers)
        self.major_institutions = {
            '0001067983': 'Berkshire Hathaway',
            '0001350694': 'Citadel Advisors',
            '0001423053': 'Renaissance Technologies',
            '0001037389': 'Bridgewater Associates',
            '0001336528': 'Millennium Management',
            '0001649339': 'Point72 Asset Management',
            '0001364742': 'Two Sigma Investments',
            '0001167483': 'Elliott Investment Management',
            '0001061165': 'Tiger Global Management',
            '0001697748': 'BlackRock Inc.',
            '0001040280': 'Vanguard Group',
            '0001166559': 'Fidelity Management',
            '0001095620': 'State Street Corporation',
            '0000895421': 'Soros Fund Management',
            '0001273087': 'Appaloosa Management',
        }
    
    def analyze_institutional_changes(self, tickers: List[str]) -> pd.DataFrame:
        """
        Analyze institutional ownership and recent changes
        Uses yfinance as primary data source
        """
        import yfinance as yf
        from tqdm import tqdm
        
        results = []
        
        for ticker in tqdm(tickers, desc="Fetching institutional data"):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # Basic ownership info
                inst_pct = info.get('heldPercentInstitutions', 0) or 0
                insider_pct = info.get('heldPercentInsiders', 0) or 0
                
                # Float and shares
                float_shares = info.get('floatShares', 0) or 0
                shares_outstanding = info.get('sharesOutstanding', 0) or 0
                short_pct = info.get('shortPercentOfFloat', 0) or 0
                
                # Insider transactions
                try:
                    insider_txns = stock.insider_transactions
                    if insider_txns is not None and len(insider_txns) > 0:
                        recent = insider_txns.head(10)
                        buys = len(recent[recent['Transaction'].str.contains('Buy', na=False)])
                        sells = len(recent[recent['Transaction'].str.contains('Sale', na=False)])
                        insider_sentiment = 'Buying' if buys > sells else ('Selling' if sells > buys else 'Neutral')
                    else:
                        insider_sentiment = 'Unknown'
                        buys = 0
                        sells = 0
                except:
                    insider_sentiment = 'Unknown'
                    buys = 0
                    sells = 0
                
                # Institutional holders count
                try:
                    inst_holders = stock.institutional_holders
                    num_inst_holders = len(inst_holders) if inst_holders is not None else 0
                except:
                    num_inst_holders = 0
                
                # Score calculation (0-100)
                score = 50
                
                # High institutional ownership is generally positive
                if inst_pct > 0.8:
                    score += 15
                elif inst_pct > 0.6:
                    score += 10
                elif inst_pct < 0.3:
                    score -= 10
                
                # Insider activity
                if buys > sells:
                    score += 15
                elif sells > buys:
                    score -= 10
                
                # Low short interest is positive
                if short_pct < 0.03:
                    score += 5
                elif short_pct > 0.1:
                    score -= 10
                elif short_pct > 0.2:
                    score -= 20
                
                score = max(0, min(100, score))
                
                # Determine stage
                if score >= 70:
                    stage = "Strong Institutional Support"
                elif score >= 55:
                    stage = "Institutional Support"
                elif score >= 45:
                    stage = "Neutral"
                elif score >= 30:
                    stage = "Institutional Concern"
                else:
                    stage = "Strong Institutional Selling"
                
                results.append({
                    'ticker': ticker,
                    'institutional_pct': round(inst_pct * 100, 2),
                    'insider_pct': round(insider_pct * 100, 2),
                    'short_pct': round(short_pct * 100, 2),
                    'float_shares_m': round(float_shares / 1e6, 2) if float_shares else 0,
                    'num_inst_holders': num_inst_holders,
                    'insider_buys': buys,
                    'insider_sells': sells,
                    'insider_sentiment': insider_sentiment,
                    'institutional_score': score,
                    'institutional_stage': stage
                })
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                logger.debug(f"Error analyzing {ticker}: {e}")
                continue
        
        return pd.DataFrame(results)
    
    def run(self) -> pd.DataFrame:
        """Run institutional analysis for stocks in the data directory"""
        logger.info("🚀 Starting 13F Institutional Analysis...")
        
        # Load stock list
        stocks_file = os.path.join(self.data_dir, 'us_stocks_list.csv')
        
        if os.path.exists(stocks_file):
            stocks_df = pd.read_csv(stocks_file)
            tickers = stocks_df['ticker'].tolist()
        else:
            logger.warning("Stock list not found. Using top 50 S&P 500 stocks.")
            tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
                      'UNH', 'JNJ', 'JPM', 'V', 'XOM', 'PG', 'MA', 'HD', 'CVX', 'MRK',
                      'ABBV', 'LLY', 'PEP', 'KO', 'COST', 'AVGO', 'WMT', 'MCD', 'TMO',
                      'CSCO', 'ABT', 'CRM', 'ACN', 'DHR', 'ORCL', 'NKE', 'TXN', 'PM',
                      'NEE', 'INTC', 'AMD', 'QCOM', 'IBM', 'GS', 'CAT', 'BA', 'DIS',
                      'NFLX', 'PYPL', 'ADBE', 'NOW', 'INTU']
        
        logger.info(f"📊 Analyzing {len(tickers)} stocks")
        
        # Run analysis
        results_df = self.analyze_institutional_changes(tickers)
        
        # Save results
        if not results_df.empty:
            results_df.to_csv(self.output_file, index=False)
            logger.info(f"✅ Analysis complete! Saved to {self.output_file}")
            
            # Summary
            logger.info("\n📊 Summary:")
            stage_counts = results_df['institutional_stage'].value_counts()
            for stage, count in stage_counts.items():
                logger.info(f"   {stage}: {count} stocks")
        else:
            logger.warning("No results to save")
        
        return results_df


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='13F Institutional Analysis')
    parser.add_argument('--dir', default='.', help='Data directory')
    parser.add_argument('--tickers', nargs='+', help='Specific tickers to analyze')
    args = parser.parse_args()
    
    analyzer = SEC13FAnalyzer(data_dir=args.dir)
    
    if args.tickers:
        results = analyzer.analyze_institutional_changes(args.tickers)
    else:
        results = analyzer.run()
    
    if not results.empty:
        # Show top institutional support
        print("\n🏦 Top 10 Institutional Support:")
        top_10 = results.nlargest(10, 'institutional_score')
        for _, row in top_10.iterrows():
            print(f"   {row['ticker']}: Score {row['institutional_score']} | "
                  f"Inst: {row['institutional_pct']:.1f}% | "
                  f"Insider: {row['insider_sentiment']}")
        
        # Show stocks with insider buying
        print("\n📈 Insider Buying Activity:")
        buying = results[results['insider_sentiment'] == 'Buying'].head(10)
        for _, row in buying.iterrows():
            print(f"   {row['ticker']}: {row['insider_buys']} buys vs {row['insider_sells']} sells")


if __name__ == "__main__":
    main()
