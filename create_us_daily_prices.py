#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
US Stock Daily Prices Collection Script
Collects daily price data for NASDAQ and S&P 500 stocks using yfinance
Similar to create_complete_daily_prices.py for Korean stocks
"""

import os
import pandas as pd
import numpy as np
import yfinance as yf
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from tqdm import tqdm

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class USStockDailyPricesCreator:
    def __init__(self):
        self.data_dir = os.getenv('DATA_DIR', '.')
        self.output_dir = self.data_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Data file paths
        self.prices_file = os.path.join(self.output_dir, 'us_daily_prices.csv')
        self.stocks_list_file = os.path.join(self.output_dir, 'us_stocks_list.csv')
        
        # Start date for historical data
        self.start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        self.end_date = datetime.now(timezone.utc)
        
    def get_sp500_tickers(self) -> List[Dict]:
        """Get full S&P 500 tickers - Wikipedia 동적 수집 + 하드코딩 폴백"""
        logger.info("🌐 Wikipedia에서 S&P 500 다이나믹 수집 시도...")
        
        try:
            # Option B: Wikipedia에서 실시간 포함 종목 목록 수집
            import pandas as pd
            wiki_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            tables = pd.read_html(wiki_url)
            sp500_df = tables[0]
            
            # 컨럼명 자동 찾기 (Symbol 또는 Ticker)
            symbol_col = 'Symbol' if 'Symbol' in sp500_df.columns else sp500_df.columns[0]
            name_col = 'Security' if 'Security' in sp500_df.columns else sp500_df.columns[1]
            sector_col = 'GICS Sector' if 'GICS Sector' in sp500_df.columns else 'Sector'
            
            wiki_tickers = sp500_df[symbol_col].str.replace('.', '-', regex=False).tolist()
            wiki_names   = sp500_df[name_col].tolist()
            wiki_sectors = sp500_df.get(sector_col, pd.Series(['N/A']*len(sp500_df))).tolist()
            
            stocks = []
            for ticker, name, sector in zip(wiki_tickers, wiki_names, wiki_sectors):
                stocks.append({
                    'ticker': ticker,
                    'name': name,
                    'sector': sector,
                    'industry': 'N/A',
                    'market': 'S&P500'
                })
            
            logger.info(f"✅ Wikipedia에서 {len(stocks)}종목 S&P 500 원본 수집 성공!")
            
            # 수집된 리스트 저장 (참조용)
            import json
            save_path = os.path.join(self.output_dir, 'us_sp500_list.json')
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump({'updated': str(pd.Timestamp.now()), 'count': len(stocks), 'tickers': [s['ticker'] for s in stocks]}, f, ensure_ascii=False, indent=2)
            
            return stocks
            
        except Exception as e:
            logger.warning(f"⚠️ Wikipedia 수집 실패 ({e}), 하드코딩 리스트 사용")
            # 폴백: 기존 하드코딩 리스트
            sp500_tickers = [
                "A", "AAL", "AAPL", "ABBV", "ABNB", "ABT", "ACGL", "ACN", "ADBE", "ADI",
                "ADM", "ADP", "ADSK", "AEE", "AEP", "AES", "AFL", "AIG", "AIZ", "AJG",
                "AKAM", "ALB", "ALGN", "ALL", "ALLE", "AMAT", "AMCR", "AMD", "AME", "AMGN",
                "AMP", "AMT", "AMZN", "ANET", "ANSS", "AON", "AOS", "APA", "APD", "APH",
                "APTV", "ARE", "ATO", "AVB", "AVGO", "AVY", "AWK", "AXON", "AXP", "AZO",
                "BA", "BAC", "BALL", "BAX", "BBWI", "BBY", "BDX", "BEN", "BF-B", "BG",
                "BIIB", "BIO", "BK", "BKNG", "BKR", "BLDR", "BLK", "BMY", "BR", "BRK-B",
                "BRO", "BSX", "BWA", "BX", "BXP", "C", "CAG", "CAH", "CARR", "CAT",
                "CB", "CBOE", "CBRE", "CCI", "CCL", "CDNS", "CDW", "CE", "CEG", "CF",
                "CFG", "CHD", "CHRW", "CHTR", "CI", "CINF", "CL", "CLX", "CMCSA", "CME",
                "CMG", "CMI", "CMS", "CNC", "CNP", "COF", "COO", "COP", "COR", "COST",
                "CPAY", "CPB", "CPRT", "CPT", "CRL", "CRM", "CSCO", "CSGP", "CSX", "CTAS",
                "CTLT", "CTRA", "CTSH", "CTVA", "CVS", "CVX", "CZR", "D", "DAL", "DAY",
                "DD", "DE", "DECK", "DFS", "DG", "DGX", "DHI", "DHR", "DIS", "DLR",
                "DLTR", "DOC", "DOV", "DOW", "DPZ", "DRI", "DTE", "DUK", "DVA", "DVN",
                "DXCM", "EA", "EBAY", "ECL", "ED", "EFX", "EG", "EIX", "EL", "ELV",
                "EMN", "EMR", "ENPH", "EOG", "EPAM", "EQIX", "EQR", "EQT", "ES", "ESS",
                "ETN", "ETR", "ETSY", "EVRG", "EW", "EXC", "EXPD", "EXPE", "EXR", "F",
                "FANG", "FAST", "FCX", "FDS", "FDX", "FE", "FFIV", "FI", "FICO", "FIS",
                "FITB", "FLT", "FMC", "FOX", "FOXA", "FRT", "FSLR", "FTNT", "FTV", "GD",
                "GDDY", "GE", "GEHC", "GEN", "GEV", "GILD", "GIS", "GL", "GLW", "GM",
                "GNRC", "GOOG", "GOOGL", "GPC", "GPN", "GRMN", "GS", "GWW", "HAL", "HAS",
                "HBAN", "HCA", "HD", "HES", "HIG", "HII", "HLT", "HOLX", "HON", "HPE",
                "HPQ", "HRL", "HSIC", "HST", "HSY", "HUBB", "HUM", "HWM", "IBM", "ICE",
                "IDXX", "IEX", "IFF", "ILMN", "INCY", "INTC", "INTU", "INVH", "IP", "IPG",
                "IQV", "IR", "IRM", "ISRG", "IT", "ITW", "IVZ", "J", "JBHT", "JBL",
                "JCI", "JKHY", "JNJ", "JNPR", "JPM", "K", "KDP", "KEY", "KEYS", "KHC",
                "KIM", "KKR", "KLAC", "KMB", "KMI", "KMX", "KO", "KR", "KVUE", "L",
                "LDOS", "LEN", "LH", "LHX", "LIN", "LKQ", "LLY", "LMT", "LNT", "LOW",
                "LRCX", "LULU", "LUV", "LVS", "LW", "LYB", "LYV", "MA", "MAA", "MAR",
                "MAS", "MCD", "MCHP", "MCK", "MCO", "MDLZ", "MDT", "MET", "META", "MGM",
                "MHK", "MKC", "MKTX", "MLM", "MMC", "MMM", "MNST", "MO", "MOH", "MOS",
                "MPC", "MPWR", "MRK", "MRNA", "MRO", "MS", "MSCI", "MSFT", "MSI", "MTB",
                "MTCH", "MTD", "MU", "NCLH", "NDAQ", "NDSN", "NEE", "NEM", "NFLX", "NI",
                "NKE", "NOC", "NOW", "NRG", "NSC", "NTAP", "NTRS", "NUE", "NVDA", "NVR",
                "NWS", "NWSA", "NXPI", "O", "ODFL", "OKE", "OMC", "ON", "ORCL", "ORLY",
                "OTIS", "OXY", "PANW", "PARA", "PAYC", "PAYX", "PCAR", "PCG", "PEG", "PEP",
                "PFE", "PFG", "PG", "PGR", "PH", "PHM", "PKG", "PLD", "PM", "PNC",
                "PNR", "PNW", "PODD", "POOL", "PPG", "PPL", "PRU", "PSA", "PSX", "PTC",
                "PWR", "PYPL", "QCOM", "QRVO", "RCL", "REG", "REGN", "RF", "RJF", "RL",
                "RMD", "ROK", "ROL", "ROP", "ROST", "RSG", "RTX", "RVTY", "SBAC", "SBUX",
                "SCHW", "SHW", "SJM", "SLB", "SMCI", "SNA", "SNPS", "SO", "SOLV", "SPG",
                "SPGI", "SRE", "STE", "STLD", "STT", "STX", "STZ", "SWK", "SWKS", "SYF",
                "SYK", "SYY", "T", "TAP", "TDG", "TDY", "TECH", "TEL", "TER", "TFC",
                "TFX", "TGT", "TJX", "TMO", "TMUS", "TPR", "TRGP", "TRMB", "TROW", "TRV",
                "TSCO", "TSLA", "TSN", "TT", "TTWO", "TXN", "TXT", "TYL", "UAL", "UBER",
                "UDR", "UHS", "ULTA", "UNH", "UNP", "UPS", "URI", "USB", "V", "VICI",
                "VLO", "VLTO", "VMC", "VRSK", "VRSN", "VRTX", "VST", "VTR", "VTRS", "VZ",
                "WAB", "WAT", "WBA", "WBD", "WDC", "WEC", "WELL", "WFC", "WM", "WMB",
                "WMT", "WRB", "WST", "WTW", "WY", "WYNN", "XEL", "XOM", "XYL", "YUM",
                "ZBH", "ZBRA", "ZTS"
            ]
            stocks = [{'ticker': t, 'name': t, 'sector': 'N/A', 'industry': 'N/A', 'market': 'S&P500'} for t in sp500_tickers]
            logger.info(f"✅ 하드코딩 리스트에서 {len(stocks)}종목 로드")
            return stocks

    
    def get_nasdaq100_tickers(self) -> List[Dict]:
        """Skip NASDAQ - already covered in S&P 500"""
        logger.info("📊 Skipping NASDAQ 100 (covered in S&P 500)...")
        return []
    
    def load_or_create_stock_list(self) -> pd.DataFrame:
        """Load existing stock list or create new one"""
        if os.path.exists(self.stocks_list_file):
            logger.info(f"📂 Loading existing stock list: {self.stocks_list_file}")
            return pd.read_csv(self.stocks_list_file)
        
        # Create new stock list
        logger.info("📝 Creating new US stock list...")
        
        sp500_stocks = self.get_sp500_tickers()
        nasdaq_stocks = self.get_nasdaq100_tickers()
        
        # Combine and remove duplicates
        all_stocks = sp500_stocks + nasdaq_stocks
        stocks_df = pd.DataFrame(all_stocks)
        stocks_df = stocks_df.drop_duplicates(subset=['ticker'], keep='first')
        
        # Save stock list
        stocks_df.to_csv(self.stocks_list_file, index=False)
        logger.info(f"✅ Saved {len(stocks_df)} stocks to {self.stocks_list_file}")
        
        return stocks_df
    
    def load_existing_prices(self) -> pd.DataFrame:
        """Load existing price data"""
        if os.path.exists(self.prices_file):
            logger.info(f"📂 Loading existing prices: {self.prices_file}")
            df = pd.read_csv(self.prices_file)
            df['date'] = pd.to_datetime(df['date'], utc=True)
            return df
        return pd.DataFrame()
    
    def get_latest_dates(self, df: pd.DataFrame) -> Dict[str, datetime]:
        """Get latest date for each ticker"""
        if df.empty:
            return {}
        return df.groupby('ticker')['date'].max().to_dict()
    
    def download_stock_data(self, ticker: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Download daily price data for a single stock"""
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date)
            
            if hist.empty:
                return pd.DataFrame()
            
            hist = hist.reset_index()
            hist['ticker'] = ticker
            
            # Rename columns to match Korean stock format
            hist = hist.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'current_price',
                'Volume': 'volume'
            })
            hist['date'] = pd.to_datetime(hist['date'], utc=True)
            
            # Calculate change and change_rate
            hist['change'] = hist['current_price'].diff()
            hist['change_rate'] = hist['current_price'].pct_change() * 100
            
            # Select required columns
            cols = ['ticker', 'date', 'open', 'high', 'low', 'current_price', 'volume', 'change', 'change_rate']
            hist = hist[cols]
            
            return hist
            
        except Exception as e:
            logger.debug(f"⚠️ Failed to download {ticker}: {e}")
            return pd.DataFrame()
    
    def run(self, full_refresh: bool = False) -> bool:
        """Run data collection (incremental by default)"""
        logger.info("🚀 US Stock Daily Prices Collection Started...")
        
        try:
            # 1. Load stock list
            stocks_df = self.load_or_create_stock_list()
            if stocks_df.empty:
                logger.error("❌ No stocks to process")
                return False
            
            # 2. Load existing data
            existing_df = pd.DataFrame() if full_refresh else self.load_existing_prices()
            latest_dates = self.get_latest_dates(existing_df)
            
            # 3. Determine target end date
            now = datetime.now(timezone.utc)
            target_end_date = now
            
            # 4. Collect data
            all_new_data = []
            failed_tickers = []
            
            for idx, row in tqdm(stocks_df.iterrows(), desc="Downloading US stocks", total=len(stocks_df)):
                ticker = row['ticker']
                
                # Determine start date
                if ticker in latest_dates:
                    start_date = latest_dates[ticker] + timedelta(days=1)
                else:
                    start_date = self.start_date
                
                # Skip if already up to date
                if start_date >= target_end_date:
                    continue
                
                # Download data
                new_data = self.download_stock_data(ticker, start_date, target_end_date)
                
                if not new_data.empty:
                    # Add name from stock list
                    new_data['name'] = row['name']
                    new_data['market'] = row['market']
                    all_new_data.append(new_data)
                else:
                    failed_tickers.append(ticker)
            
            # 5. Combine and save
            if all_new_data:
                new_df = pd.concat(all_new_data, ignore_index=True)
                
                if not existing_df.empty:
                    final_df = pd.concat([existing_df, new_df])
                    final_df = final_df.drop_duplicates(subset=['ticker', 'date'], keep='last')
                else:
                    final_df = new_df
                
                # Sort and save
                final_df = final_df.sort_values(['ticker', 'date']).reset_index(drop=True)
                final_df.to_csv(self.prices_file, index=False)
                
                logger.info(f"✅ Saved {len(new_df)} new records to {self.prices_file}")
                logger.info(f"📊 Total records: {len(final_df)}")
            else:
                logger.info("✨ All data is up to date!")
            
            # 6. Summary
            logger.info(f"\n📊 Collection Summary:")
            logger.info(f"   Total stocks: {len(stocks_df)}")
            logger.info(f"   Success: {len(stocks_df) - len(failed_tickers)}")
            logger.info(f"   Failed: {len(failed_tickers)}")
            
            if failed_tickers[:10]:
                logger.warning(f"   Failed samples: {failed_tickers[:10]}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during collection: {e}")
            return False


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='US Stock Daily Prices Collector')
    parser.add_argument('--full', action='store_true', help='Full refresh (ignore existing data)')
    args = parser.parse_args()
    
    creator = USStockDailyPricesCreator()
    success = creator.run(full_refresh=args.full)
    
    if success:
        print("\n🎉 US Stock Daily Prices collection completed!")
        print(f"📁 File location: ./us_daily_prices.csv")
    else:
        print("\n❌ Collection failed.")


if __name__ == "__main__":
    main()
