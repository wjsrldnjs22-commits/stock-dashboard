#!/usr/bin/env python3
"""한국 시장 데이터 수집기 - KOSPI/KOSDAQ 지수 및 주요 종목"""
import os, json, logging
import yfinance as yf
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KRMarketDataCollector:
    def __init__(self, data_dir='.'):
        self.data_dir = data_dir
        # KOSPI 주요 종목 (Yahoo Finance 티커)
        self.kr_stocks = {
            '005930.KS': '삼성전자',
            '000660.KS': 'SK하이닉스',
            '373220.KS': 'LG에너지솔루션',
            '207940.KS': '삼성바이오로직스',
            '005380.KS': '현대자동차',
            '000270.KS': '기아',
            '006400.KS': '삼성SDI',
            '035420.KS': 'NAVER',
            '035720.KS': '카카오',
            '051910.KS': 'LG화학',
            '028260.KS': '삼성물산',
            '105560.KS': 'KB금융',
            '055550.KS': '신한지주',
            '066570.KS': 'LG전자',
            '003670.KS': '포스코퓨처엠',
            '068270.KS': '셀트리온',
            '096770.KS': 'SK이노베이션',
            '034730.KS': 'SK',
            '012330.KS': '현대모비스',
            '086790.KS': '하나금융지주',
        }
        # 시장 지수
        self.indices = {
            '^KS11': 'KOSPI',
            '^KQ11': 'KOSDAQ',
            '^KS200': 'KOSPI 200',
        }
    
    def collect(self):
        """한국 시장 데이터 수집"""
        logger.info("🇰🇷 한국 시장 데이터 수집 시작...")
        result = {
            'updated': datetime.now().isoformat(),
            'indices': {},
            'stocks': [],
            'sectors': {}
        }
        
        # 1. 시장 지수 수집
        for ticker, name in self.indices.items():
            try:
                data = yf.download(ticker, period='5d', progress=False, auto_adjust=True)
                if not data.empty:
                    # yfinance 최신 버전 호환: Close 컬럼이 DataFrame일 수 있음
                    close_col = data['Close']
                    if hasattr(close_col, 'columns'):  # MultiIndex DataFrame
                        close_series = close_col.iloc[:, 0].dropna()
                    else:
                        close_series = close_col.dropna()
                    if len(close_series) < 1:
                        continue
                    current = float(close_series.iloc[-1])
                    prev = float(close_series.iloc[-2]) if len(close_series) > 1 else current
                    change = ((current / prev) - 1) * 100
                    result['indices'][name] = {
                        'value': round(current, 2),
                        'change': round(change, 2),
                        'prev_close': round(prev, 2)
                    }
                    logger.info(f"  {name}: {current:.2f} ({change:+.2f}%)")
            except Exception as e:
                logger.error(f"Error fetching {name}: {e}")
        
        # 2. 주요 종목 수집 (개별 티커로 안정적으로 수집)
        tickers = list(self.kr_stocks.keys())
        for ticker in tickers:
            try:
                name = self.kr_stocks[ticker]
                stock = yf.Ticker(ticker)
                hist = stock.history(period='5d')
                if hist.empty or len(hist) < 2:
                    continue
                prices = hist['Close'].dropna()
                if len(prices) < 2:
                    continue
                current = float(prices.iloc[-1])
                prev = float(prices.iloc[-2])
                change = ((current / prev) - 1) * 100
                vol = float(hist['Volume'].iloc[-1]) if 'Volume' in hist.columns else 0
                
                result['stocks'].append({
                    'ticker': ticker.replace('.KS', ''),
                    'name': name,
                    'price': round(current, 0),
                    'change': round(change, 2),
                    'volume': int(vol),
                    'market_cap': round(current * vol / 1e6, 1) if vol > 0 else 0
                })
                logger.info(f"  {name}: {current:,.0f}원 ({change:+.2f}%)")
            except Exception as e:
                logger.error(f"  Error for {ticker} ({self.kr_stocks.get(ticker, '')}): {e}")
        
        # 3. 섹터별 분류
        sector_map = {
            '반도체': ['삼성전자', 'SK하이닉스'],
            '배터리': ['LG에너지솔루션', '삼성SDI', 'LG화학', 'SK이노베이션', '포스코퓨처엠'],
            'IT/플랫폼': ['NAVER', '카카오'],
            '자동차': ['현대자동차', '기아', '현대모비스'],
            '바이오': ['삼성바이오로직스', '셀트리온'],
            '금융': ['KB금융', '신한지주', '하나금융지주'],
            '전자/제조': ['LG전자', '삼성물산', 'SK'],
        }
        for sector, names in sector_map.items():
            stocks_in_sector = [s for s in result['stocks'] if s['name'] in names]
            if stocks_in_sector:
                avg_change = sum(s['change'] for s in stocks_in_sector) / len(stocks_in_sector)
                result['sectors'][sector] = {
                    'avg_change': round(avg_change, 2),
                    'stocks': stocks_in_sector,
                    'count': len(stocks_in_sector)
                }
        
        # 저장
        output_path = os.path.join(self.data_dir, 'kr_market_data.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ 한국 시장 데이터 저장 완료: {len(result['stocks'])}개 종목, {len(result['indices'])}개 지수")
        return result

if __name__ == '__main__':
    KRMarketDataCollector().collect()
