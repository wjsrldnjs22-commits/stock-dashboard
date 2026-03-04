#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sector Performance Heatmap Data Collector
"""

import os
import json
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SectorHeatmapCollector:
    """Collect sector ETF performance data for heatmap visualization"""
    
    def __init__(self):
        # Sector ETFs with full names
        self.sector_etfs = {
            'XLK': {'name': 'Technology', 'color': '#4A90A4'},
            'XLF': {'name': 'Financials', 'color': '#6B8E23'},
            'XLV': {'name': 'Healthcare', 'color': '#FF69B4'},
            'XLE': {'name': 'Energy', 'color': '#FF6347'},
            'XLY': {'name': 'Consumer Disc.', 'color': '#FFD700'},
            'XLP': {'name': 'Consumer Staples', 'color': '#98D8C8'},
            'XLI': {'name': 'Industrials', 'color': '#DDA0DD'},
            'XLB': {'name': 'Materials', 'color': '#F0E68C'},
            'XLU': {'name': 'Utilities', 'color': '#87CEEB'},
            'XLRE': {'name': 'Real Estate', 'color': '#CD853F'},
            'XLC': {'name': 'Comm. Services', 'color': '#9370DB'},
        }
        
        # Sector stocks for detail map (모든 섹터 포함)
        self.sector_stocks = {
             'Technology': ['AAPL', 'MSFT', 'NVDA', 'AVGO', 'ORCL', 'CRM', 'AMD', 'ADBE', 'PLTR', 'KEYS', 'CSCO', 'INTC', 'MU', 'QCOM'],
             'Financials': ['BRK-B', 'JPM', 'V', 'MA', 'BAC', 'WFC', 'GS', 'MS', 'ALL', 'AXP', 'C', 'BLK'],
             'Healthcare': ['UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'PFE', 'TMO', 'ABT', 'AMGN', 'ZTS'],
             'Energy': ['XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX', 'VLO', 'CTRA', 'KMI'],
             'Consumer Disc.': ['AMZN', 'TSLA', 'HD', 'MCD', 'NKE', 'SBUX', 'TJX', 'LOW', 'BKNG', 'CMG'],
             'Consumer Staples': ['PG', 'KO', 'PEP', 'COST', 'WMT', 'PM', 'CL', 'EL', 'CPB', 'GIS'],
             'Industrials': ['CAT', 'HON', 'UNP', 'UPS', 'BA', 'RTX', 'DE', 'GE', 'LMT', 'FDX', 'MSI'],
             'Materials': ['LIN', 'APD', 'NEM', 'FCX', 'ECL', 'NUE', 'VMC', 'MLM', 'BALL', 'DD'],
             'Utilities': ['NEE', 'DUK', 'SO', 'D', 'AEP', 'EXC', 'SRE', 'EIX', 'WEC', 'ED'],
             'Real Estate': ['AMT', 'PLD', 'CCI', 'PSA', 'EQIX', 'DLR', 'SPG', 'O', 'WELL', 'AVB'],
             'Comm. Services': ['GOOGL', 'META', 'NFLX', 'DIS', 'CMCSA', 'TMUS', 'VZ', 'T', 'CHTR', 'RKLB', 'ASTS'],
             'ETFs': ['VOO', 'SPY', 'QQQ', 'VTI', 'IWM'],
        }
    
    def get_full_market_map(self, period: str = '5d') -> Dict:
        """Get full market map data (Sectors -> Stocks) for Treemap"""
        logger.info(f"📊 Fetching full market map data ({period})...")
        
        all_tickers = []
        ticker_to_sector = {}
        for sector, stocks in self.sector_stocks.items():
            all_tickers.extend(stocks)
            for stock in stocks:
                ticker_to_sector[stock] = sector
                
        try:
            data = yf.download(all_tickers, period=period, progress=False)
            
            if data.empty: return {'error': 'No data'}
            
            market_map = {name: [] for name in self.sector_stocks.keys()}
            
            for ticker in all_tickers:
                try:
                    if ticker not in data['Close'].columns: continue
                    prices = data['Close'][ticker].dropna()
                    if len(prices) < 2: continue
                    
                    current = prices.iloc[-1]
                    prev = prices.iloc[-2]
                    change = ((current / prev) - 1) * 100
                    
                    # Weight by Volume * Price (Activity proxy)
                    vol = data['Volume'][ticker].iloc[-1] if 'Volume' in data.columns else 100000
                    weight = current * vol
                    
                    sector = ticker_to_sector.get(ticker, 'Unknown')
                    if sector in market_map:
                        market_map[sector].append({
                            'x': ticker,
                            'y': round(weight, 0),
                            'price': round(current, 2),
                            'change': round(change, 2),
                            'color': self._get_color(change)
                        })
                except: pass
            
            series = []
            for sector_name, stocks in market_map.items():
                if stocks:
                    stocks.sort(key=lambda x: x['y'], reverse=True)
                    series.append({'name': sector_name, 'data': stocks})
            
            series.sort(key=lambda s: sum(i['y'] for i in s['data']), reverse=True)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'period': period,
                'series': series
            }
            
        except Exception as e:
            logger.error(f"Error: {e}")
            return {'error': str(e)}
            
    def _get_color(self, change: float) -> str:
        if change >= 3: return '#00C853'
        elif change >= 1: return '#4CAF50'
        elif change >= 0: return '#81C784'
        elif change >= -1: return '#EF9A9A'
        elif change >= -3: return '#F44336'
        else: return '#B71C1C'

    def save_data(self, output_dir: str = '.'):
        data = self.get_full_market_map('5d')
        output_file = os.path.join(output_dir, 'sector_heatmap.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Saved to {output_file}")


if __name__ == "__main__":
    SectorHeatmapCollector().save_data()
