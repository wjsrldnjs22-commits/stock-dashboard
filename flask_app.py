import os
import json
import threading
import pandas as pd
import numpy as np
import yfinance as yf
import subprocess
import requests
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import traceback
from datetime import datetime

# ── 일일 데이터 DB ──────────────────────────────────
try:
    from daily_db import (
        init_db, get_db_stats,
        save_portfolio_snapshot, save_ailey_analysis,
        get_portfolio_history, get_smart_money_history,
        get_market_history, get_ailey_history,
    )
    DAILY_DB_ENABLED = True
except ImportError:
    DAILY_DB_ENABLED = False
    print("⚠️ daily_db 없음 — DB 기능 비활성화")

# ── APScheduler ─────────────────────────────────────
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from zoneinfo import ZoneInfo
    SCHEDULER_ENABLED = True
except ImportError:
    SCHEDULER_ENABLED = False
    print("⚠️ apscheduler 없음 — pip install apscheduler 권장")

app = Flask(__name__)
CORS(app)

# 데이터 디렉토리 (현재 실행 위치 기준)
DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))

# Sector mapping for major US stocks (S&P 500 + popular stocks)
SECTOR_MAP = {
    # Technology
    'AAPL': 'Tech', 'MSFT': 'Tech', 'NVDA': 'Tech', 'AVGO': 'Tech', 'ORCL': 'Tech',
    'CRM': 'Tech', 'AMD': 'Tech', 'ADBE': 'Tech', 'CSCO': 'Tech', 'INTC': 'Tech',
    'IBM': 'Tech', 'MU': 'Tech', 'QCOM': 'Tech', 'TXN': 'Tech', 'NOW': 'Tech',
    'AMAT': 'Tech', 'LRCX': 'Tech', 'KLAC': 'Tech', 'SNPS': 'Tech', 'CDNS': 'Tech',
    'ADI': 'Tech', 'MRVL': 'Tech', 'FTNT': 'Tech', 'PANW': 'Tech', 'CRWD': 'Tech',
    'SNOW': 'Tech', 'DDOG': 'Tech', 'ZS': 'Tech', 'NET': 'Tech', 'PLTR': 'Tech',
    'DELL': 'Tech', 'HPQ': 'Tech', 'HPE': 'Tech', 'KEYS': 'Tech', 'SWKS': 'Tech',
    # Financials
    'BRK-B': 'Fin', 'JPM': 'Fin', 'V': 'Fin', 'MA': 'Fin', 'BAC': 'Fin',
    'WFC': 'Fin', 'GS': 'Fin', 'MS': 'Fin', 'SPGI': 'Fin', 'AXP': 'Fin',
    'C': 'Fin', 'BLK': 'Fin', 'SCHW': 'Fin', 'CME': 'Fin', 'CB': 'Fin',
    'PGR': 'Fin', 'MMC': 'Fin', 'AON': 'Fin', 'ICE': 'Fin', 'MCO': 'Fin',
    'USB': 'Fin', 'PNC': 'Fin', 'TFC': 'Fin', 'AIG': 'Fin', 'MET': 'Fin',
    'PRU': 'Fin', 'ALL': 'Fin', 'TRV': 'Fin', 'COIN': 'Fin', 'HOOD': 'Fin',
    # Healthcare
    'LLY': 'Health', 'UNH': 'Health', 'JNJ': 'Health', 'ABBV': 'Health', 'MRK': 'Health',
    'PFE': 'Health', 'TMO': 'Health', 'ABT': 'Health', 'DHR': 'Health', 'BMY': 'Health',
    'AMGN': 'Health', 'GILD': 'Health', 'VRTX': 'Health', 'ISRG': 'Health', 'MDT': 'Health',
    'SYK': 'Health', 'BSX': 'Health', 'REGN': 'Health', 'ZTS': 'Health', 'ELV': 'Health',
    'CI': 'Health', 'HUM': 'Health', 'CVS': 'Health', 'MCK': 'Health', 'CAH': 'Health',
    'GEHC': 'Health', 'DXCM': 'Health', 'IQV': 'Health', 'BIIB': 'Health', 'MRNA': 'Health',
    # Energy
    'XOM': 'Energy', 'CVX': 'Energy', 'COP': 'Energy', 'SLB': 'Energy', 'EOG': 'Energy',
    'MPC': 'Energy', 'PSX': 'Energy', 'VLO': 'Energy', 'OXY': 'Energy', 'WMB': 'Energy',
    'DVN': 'Energy', 'HES': 'Energy', 'HAL': 'Energy', 'BKR': 'Energy', 'KMI': 'Energy',
    'FANG': 'Energy', 'PXD': 'Energy', 'TRGP': 'Energy', 'OKE': 'Energy', 'ET': 'Energy',
    # Consumer Discretionary
    'AMZN': 'Cons', 'TSLA': 'Cons', 'HD': 'Cons', 'MCD': 'Cons', 'NKE': 'Cons',
    'LOW': 'Cons', 'SBUX': 'Cons', 'TJX': 'Cons', 'BKNG': 'Cons', 'CMG': 'Cons',
    'ORLY': 'Cons', 'AZO': 'Cons', 'ROST': 'Cons', 'DHI': 'Cons', 'LEN': 'Cons',
    'GM': 'Cons', 'F': 'Cons', 'MAR': 'Cons', 'HLT': 'Cons', 'YUM': 'Cons',
    'DG': 'Cons', 'DLTR': 'Cons', 'BBY': 'Cons', 'ULTA': 'Cons', 'POOL': 'Cons',
    'LULU': 'Cons',  # lululemon athletica
    # Consumer Staples
    'WMT': 'Staple', 'PG': 'Staple', 'COST': 'Staple', 'KO': 'Staple', 'PEP': 'Staple',
    'PM': 'Staple', 'MDLZ': 'Staple', 'MO': 'Staple', 'CL': 'Staple', 'KMB': 'Staple',
    'GIS': 'Staple', 'K': 'Staple', 'HSY': 'Staple', 'SYY': 'Staple', 'STZ': 'Staple',
    'KHC': 'Staple', 'KR': 'Staple', 'EL': 'Staple', 'CHD': 'Staple', 'CLX': 'Staple',
    'KDP': 'Staple', 'TAP': 'Staple', 'ADM': 'Staple', 'BG': 'Staple', 'MNST': 'Staple',
    # Industrials
    'CAT': 'Indust', 'GE': 'Indust', 'RTX': 'Indust', 'HON': 'Indust', 'UNP': 'Indust',
    'BA': 'Indust', 'DE': 'Indust', 'LMT': 'Indust', 'UPS': 'Indust', 'MMM': 'Indust',
    'GD': 'Indust', 'NOC': 'Indust', 'CSX': 'Indust', 'NSC': 'Indust', 'WM': 'Indust',
    'EMR': 'Indust', 'ETN': 'Indust', 'ITW': 'Indust', 'PH': 'Indust', 'ROK': 'Indust',
    'FDX': 'Indust', 'CARR': 'Indust', 'TT': 'Indust', 'PCAR': 'Indust', 'FAST': 'Indust',
    # Materials
    'LIN': 'Mater', 'APD': 'Mater', 'SHW': 'Mater', 'FCX': 'Mater', 'ECL': 'Mater',
    'NEM': 'Mater', 'NUE': 'Mater', 'DOW': 'Mater', 'DD': 'Mater', 'VMC': 'Mater',
    'CTVA': 'Mater', 'PPG': 'Mater', 'MLM': 'Mater', 'IP': 'Mater', 'PKG': 'Mater',
    'ALB': 'Mater', 'GOLD': 'Mater', 'FMC': 'Mater', 'CF': 'Mater', 'MOS': 'Mater',
    # Utilities
    'NEE': 'Util', 'SO': 'Util', 'DUK': 'Util', 'CEG': 'Util', 'SRE': 'Util',
    'AEP': 'Util', 'D': 'Util', 'PCG': 'Util', 'EXC': 'Util', 'XEL': 'Util',
    'ED': 'Util', 'WEC': 'Util', 'ES': 'Util', 'AWK': 'Util', 'DTE': 'Util',
    # Real Estate
    'PLD': 'REIT', 'AMT': 'REIT', 'EQIX': 'REIT', 'SPG': 'REIT', 'PSA': 'REIT',
    'O': 'REIT', 'WELL': 'REIT', 'DLR': 'REIT', 'CCI': 'REIT', 'AVB': 'REIT',
    'CBRE': 'REIT', 'SBAC': 'REIT', 'WY': 'REIT', 'EQR': 'REIT', 'VTR': 'REIT',
    # Communication Services
    'META': 'Comm', 'GOOGL': 'Comm', 'GOOG': 'Comm', 'NFLX': 'Comm', 'DIS': 'Comm',
    'T': 'Comm', 'VZ': 'Comm', 'CMCSA': 'Comm', 'TMUS': 'Comm', 'CHTR': 'Comm',
    'EA': 'Comm', 'TTWO': 'Comm', 'RBLX': 'Comm', 'PARA': 'Comm', 'WBD': 'Comm',
    'MTCH': 'Comm', 'LYV': 'Comm', 'OMC': 'Comm', 'IPG': 'Comm', 'FOXA': 'Comm',
    # IT Services & Software
    'EPAM': 'Tech', 'ALGN': 'Health',
}

# Persistent sector cache file
SECTOR_CACHE_FILE = os.path.join('.', 'sector_cache.json')

def _load_sector_cache() -> dict:
    """Load sector cache from file"""
    try:
        if os.path.exists(SECTOR_CACHE_FILE):
            with open(SECTOR_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except:
        pass
    return {}

def _save_sector_cache(cache: dict):
    """Save sector cache to file"""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(SECTOR_CACHE_FILE), exist_ok=True)
        with open(SECTOR_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving sector cache: {e}")

# Load cache at startup
_sector_cache = _load_sector_cache()

def get_sector(ticker: str) -> str:
    """Get sector for a ticker, auto-fetch from yfinance if not in SECTOR_MAP"""
    global _sector_cache
    
    # Check static map first
    if ticker in SECTOR_MAP:
        return SECTOR_MAP[ticker]
    
    # Check persistent cache
    if ticker in _sector_cache:
        return _sector_cache[ticker]
    
    # Fetch from yfinance and save to file
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        sector = info.get('sector', '')
        
        # Map sector to short code
        sector_short_map = {
            'Technology': 'Tech',
            'Information Technology': 'Tech',
            'Healthcare': 'Health',
            'Health Care': 'Health',
            'Financials': 'Fin',
            'Financial Services': 'Fin',
            'Consumer Discretionary': 'Cons',
            'Consumer Cyclical': 'Cons',
            'Consumer Staples': 'Staple',
            'Consumer Defensive': 'Staple',
            'Energy': 'Energy',
            'Industrials': 'Indust',
            'Materials': 'Mater',
            'Basic Materials': 'Mater',
            'Utilities': 'Util',
            'Real Estate': 'REIT',
            'Communication Services': 'Comm',
        }
        
        short_sector = sector_short_map.get(sector, sector[:5] if sector else '-')
        
        # Save to cache and persist to file
        _sector_cache[ticker] = short_sector
        _save_sector_cache(_sector_cache)
        print(f"✅ Cached sector for {ticker}: {short_sector}")
        
        return short_sector
    except Exception as e:
        print(f"Error fetching sector for {ticker}: {e}")
        _sector_cache[ticker] = '-'
        _save_sector_cache(_sector_cache)
        return '-'


def calculate_rsi(series, period=14):
    delta = series.diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_trend(df):
    if len(df) < 50: return 50, "Neutral", 0
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    
    # Calculate MAs if not present (though we calc them before calling this)
    ma20 = curr['MA20']
    ma50 = curr['MA50']
    ma200 = curr['MA200']
    price = curr['Close']
    rsi = curr['RSI']
    
    score = 50
    signal = "Neutral"
    
    # Simple Trend Logic
    if price > ma20 > ma50 > ma200:
        score = 90
        signal = "Strong Buy"
    elif ma20 > ma50 and (prev['MA20'] <= prev['MA50'] or price > ma20):
        score = 80
        signal = "Buy (Golden Cross)"
    elif price < ma20 < ma50:
        score = 30
        signal = "Sell (Downtrend)"
    elif rsi > 75:
        score -= 10
        signal = "Overbought"
        
    return score, signal, rsi

@app.route('/')
def index():
    return render_template('index.html')

# Load Stock List for Suffix Mapping
# Load Stock List and Ticker Map
try:
    # Load the verified ticker map
    map_df = pd.read_csv('ticker_to_yahoo_map.csv', dtype=str)
    # Create dictionary: ticker -> yahoo_ticker
    TICKER_TO_YAHOO_MAP = dict(zip(map_df['ticker'], map_df['yahoo_ticker']))
    print(f"Loaded {len(TICKER_TO_YAHOO_MAP)} verified ticker mappings.")
except Exception as e:
    print(f"Error loading ticker map: {e}")
    TICKER_TO_YAHOO_MAP = {}



@app.route('/api/kr/recommendations')
def get_kr_recommendations():
    try:
        csv_path = 'recommendation_history.csv'
        if not os.path.exists(csv_path):
            return jsonify({'error': 'Recommendation history not found'}), 404
            
        df = pd.read_csv(csv_path)
        
        # Convert to list of dicts
        recommendations = df.to_dict(orient='records')
        
        # Get unique dates for filtering
        dates = sorted(df['recommendation_date'].unique().tolist(), reverse=True)
        
        return jsonify({
            'dates': dates,
            'data': recommendations
        })
        
    except Exception as e:
        print(f"Error reading recommendations: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kr/performance')
def get_kr_performance():
    try:
        csv_path = 'performance_report.csv'
        if not os.path.exists(csv_path):
            return jsonify({'error': 'Performance report not found'}), 404
            
        df = pd.read_csv(csv_path)
        
        # Summary Stats
        summary = {
            'total_count': len(df),
            'avg_return': float(df['return'].mean()),
            'win_rate': float((df['return'] > 0).mean() * 100),
            'top_performers': df.sort_values('return', ascending=False).head(5).to_dict(orient='records')
        }
        
        return jsonify({
            'summary': summary,
            'data': df.to_dict(orient='records')
        })
        
    except Exception as e:
        print(f"Error reading performance: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kr/market-status')
def get_kr_market_status():
    try:
        # Simple Logic using KODEX 200 (069500) or Samsung Electronics (005930) as proxy
        prices_path = 'daily_prices.csv'
        if not os.path.exists(prices_path):
            return jsonify({'status': 'UNKNOWN', 'reason': 'No price data'}), 404
            
        # Optimization: Read file and filter
        # We'll use a simple approach for now.
        df = pd.read_csv(prices_path, dtype={'ticker': str})
        
        target_ticker = '069500'
        target_name = 'KODEX 200'
        
        market_df = df[df['ticker'] == target_ticker].copy()
        
        if market_df.empty:
            # Fallback to Samsung Electronics
            target_ticker = '005930'
            target_name = 'Samsung Elec'
            market_df = df[df['ticker'] == target_ticker].copy()
            
        if market_df.empty:
             return jsonify({'status': 'UNKNOWN', 'reason': 'Market proxy data not found'}), 404
             
        market_df['date'] = pd.to_datetime(market_df['date'])
        market_df = market_df.sort_values('date')
        
        if len(market_df) < 200:
             return jsonify({'status': 'NEUTRAL', 'reason': 'Insufficient data'}), 200
             
        # Calculate MAs
        market_df['MA20'] = market_df['current_price'].rolling(20).mean()
        market_df['MA50'] = market_df['current_price'].rolling(50).mean()
        market_df['MA200'] = market_df['current_price'].rolling(200).mean()
        
        last = market_df.iloc[-1]
        price = last['current_price']
        ma20 = last['MA20']
        ma50 = last['MA50']
        ma200 = last['MA200']
        
        status = "NEUTRAL"
        score = 50
        
        if price > ma200 and ma20 > ma50:
            status = "RISK_ON"
            score = 80
        elif price < ma200 and ma20 < ma50:
            status = "RISK_OFF"
            score = 20
            
        return jsonify({
            'status': status,
            'score': score,
            'current_price': float(price),
            'ma200': float(ma200),
            'date': last['date'].strftime('%Y-%m-%d'),
            'symbol': target_ticker,
            'name': target_name
        })

    except Exception as e:
        print(f"Error checking market status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/kr/market-data')
def get_kr_market_data():
    """한국 시장 실시간 데이터 (KOSPI 주요 종목 + 지수)"""
    try:
        kr_data_path = os.path.join('.', 'kr_market_data.json')
        if not os.path.exists(kr_data_path):
            # 데이터 없으면 실시간 수집
            try:
                from kr_market_collector import KRMarketDataCollector
                collector = KRMarketDataCollector()
                data = collector.collect()
                return jsonify(data)
            except Exception as e:
                return jsonify({'error': f'한국 시장 데이터를 수집할 수 없습니다: {str(e)}'}), 500
        
        with open(kr_data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        print(f"Error getting KR market data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/portfolio')
def get_portfolio_data():
    try:
        target_date = request.args.get('date')
        
        if target_date:
            # --- Historical Data Mode ---
            csv_path = os.path.join(os.path.dirname(__file__), 'recommendation_history.csv')
            if not os.path.exists(csv_path):
                return jsonify({'error': 'History not found'}), 404
                
            df = pd.read_csv(csv_path, dtype={'ticker': str})
            
            # Filter by date
            df = df[df['recommendation_date'] == target_date]
            
            # Sort by Score
            top_holdings_df = df.sort_values(by='final_investment_score', ascending=False).head(10)
            
            # Define top_picks for later use (style box)
            top_picks = top_holdings_df
            
            # Fetch Real-time Prices for these tickers
            tickers = top_holdings_df['ticker'].tolist()
            current_prices = {}
            
            if tickers:
                yf_tickers = []
                ticker_map = {}
                
                for t in tickers:
                    t_padded = str(t).zfill(6)
                    yf_t = TICKER_TO_YAHOO_MAP.get(t_padded, f"{t_padded}.KS")
                    yf_tickers.append(yf_t)
                    ticker_map[yf_t] = t_padded

                try:
                    # Batch download
                    price_data = yf.download(yf_tickers, period='1d', interval='1m', progress=False, threads=True)
                    if not price_data.empty:
                        price_data = price_data.ffill()
                        
                        # Extract Close prices
                        if 'Close' in price_data.columns:
                            closes = price_data['Close']
                            for yf_t, orig_t in ticker_map.items():
                                try:
                                    if isinstance(closes, pd.DataFrame) and yf_t in closes.columns:
                                        val = closes[yf_t].iloc[-1]
                                        current_prices[orig_t] = float(val) if not pd.isna(val) else 0
                                    elif isinstance(closes, pd.Series) and closes.name == yf_t:
                                         val = closes.iloc[-1]
                                         current_prices[orig_t] = float(val) if not pd.isna(val) else 0
                                except:
                                    current_prices[orig_t] = 0
                except Exception as e:
                    print(f"Error fetching historical prices: {e}")

            top_holdings = []
            for _, row in top_holdings_df.iterrows():
                t_str = str(row['ticker']).zfill(6)
                rec_price = float(row['current_price'])
                cur_price = current_prices.get(t_str, 0)
                return_pct = ((cur_price - rec_price) / rec_price * 100) if rec_price > 0 else 0.0
                
                top_holdings.append({
                    'ticker': t_str,
                    'name': row['name'],
                    'price': cur_price, # Real-time price
                    'recommendation_price': rec_price, # Historical price at rec time
                    'return_pct': return_pct, # Return %
                    'score': float(row['final_investment_score']),
                    'grade': row['investment_grade'],
                    'wave': row.get('wave_stage', 'N/A'),
                    'sd_stage': 'N/A', # Not in history file
                    'inst_trend': 'N/A', # Not in history file
                    'ytd': 0 # Not in history file, or calculate diff?
                })
                
            # Calculate simple stats for history view
            key_stats = {
                'qtd_return': f"{top_holdings_df['final_investment_score'].mean():.1f}" if not top_holdings_df.empty else "0.0",
                'ytd_return': str(len(top_holdings_df)),
                'one_year_return': "N/A",
                'div_yield': "N/A",
                'expense_ratio': 'N/A'
            }
            
            holdings_distribution = [] # Skip for history

        else:
            # --- Current Live Data Mode ---
            # Read the analysis results CSV
            csv_path = os.path.join(os.path.dirname(__file__), 'wave_transition_analysis_results.csv')
            if not os.path.exists(csv_path):
                 # Fallback to mock data if file doesn't exist
                print("CSV file not found, using mock data")
                return jsonify({
                    'key_stats': {
                        'qtd_return': '+5.2%',
                        'ytd_return': '+12.8%',
                        'one_year_return': '+15.4%',
                        'div_yield': '2.1%',
                        'expense_ratio': '0.45%'
                    },
                    'holdings_distribution': [
                        {'label': 'Equity', 'value': 65, 'color': '#3b82f6'},
                        {'label': 'Fixed Income', 'value': 25, 'color': '#10b981'},
                        {'label': 'Cash', 'value': 10, 'color': '#6b7280'}
                    ],
                    'top_holdings': [],
                    'style_box': {
                        'large_value': 15, 'large_core': 20, 'large_growth': 15,
                        'mid_value': 10, 'mid_core': 15, 'mid_growth': 10,
                        'small_value': 5, 'small_core': 5, 'small_growth': 5
                    }
                })
    
            df = pd.read_csv(csv_path, dtype={'ticker': str})
            
            # --- Key Stats (Calculated from Top Recommendations) ---
            # Filter for S and A grade stocks for "Portfolio" stats
            top_picks = df[df['investment_grade'].isin(['S급 (즉시 매수)', 'A급 (적극 매수)'])]
            
            avg_score = top_picks['final_investment_score'].mean() if not top_picks.empty else 0
            avg_return_potential = top_picks['price_change_6m'].mean() * 100 if not top_picks.empty else 0 # Using 6m price change as proxy for momentum/potential
            avg_div_yield = top_picks['div_yield'].mean() if not top_picks.empty else 0
            
            key_stats = {
                'qtd_return': f"{avg_score:.1f}", # Re-purposing label for Score
                'ytd_return': f"{len(top_picks)}", # Count of Top Picks
                'one_year_return': f"{avg_return_potential:.1f}%", # Momentum
                'div_yield': f"{avg_div_yield:.1f}%",
                'expense_ratio': 'N/A' # Not applicable
            }
    
            # --- Holdings Distribution (Market Allocation) ---
            market_counts = top_picks['market'].value_counts()
            holdings_distribution = []
            colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']
            for i, (market, count) in enumerate(market_counts.items()):
                holdings_distribution.append({
                    'label': market,
                    'value': int(count),
                    'color': colors[i % len(colors)]
                })
                
            # --- Top Holdings Table (AI Recommendations) ---
            # Sort by Score
            top_holdings_df = top_picks.sort_values(by='final_investment_score', ascending=False).head(10)
            top_holdings = []
            for _, row in top_holdings_df.iterrows():
                rec_price = float(row['current_price'])
                cur_price = float(row['current_price']) # For live data, rec_price == cur_price initially
                # However, if we want real-time updates to reflect changes since analysis, we might need real-time fetch here too?
                # For now, live data implies "just analyzed", so return is 0%. 
                # BUT, if the analysis was done hours ago, prices might have moved.
                # The user asks for "rec price vs current price". 
                # In live mode, 'current_price' in CSV is the price AT ANALYSIS TIME.
                # We are NOT fetching real-time prices for the live table currently (except via the separate update loop in JS?).
                # Wait, the JS updateRealtimePrices fetches new prices.
                # So the initial load might show 0%, and then JS updates it?
                # Actually, let's just set it to 0.0 for now, as rec_price == price in this context.
                # OR, if we want to be fancy, we could fetch real-time here too. 
                # Given the user's request context (historical view), 0% is correct for "just now".
                
                top_holdings.append({
                    'ticker': str(row['ticker']).zfill(6),
                    'name': row['name'],
                    'price': cur_price,
                    'recommendation_price': rec_price, # Add Rec. Price
                    'return_pct': 0.0, # Initially 0 for live data
                    'score': float(row['final_investment_score']),
                    'grade': row['investment_grade'],
                    'wave': row.get('wave_stage', 'N/A'),
                    'sd_stage': row.get('supply_demand_stage', 'N/A'),
                    'inst_trend': row.get('institutional_trend', 'N/A'),
                    'ytd': float(row['price_change_20d']) * 100 # Using 20d change as proxy
                })

        # --- Performance Data ---
        performance_data = []
        perf_csv_path = 'performance_report.csv'
        if os.path.exists(perf_csv_path):
            perf_df = pd.read_csv(perf_csv_path)
            # Get top 5 recent performers
            recent_perf = perf_df.sort_values('rec_date', ascending=False).head(10)
            for _, row in recent_perf.iterrows():
                performance_data.append({
                    'ticker': row['ticker'],
                    'name': row['name'],
                    'return': f"{row['return']:.1f}%",
                    'date': row['rec_date'],
                    'days': row['days']
                })

        # --- Style Box (Approximation) ---
        style_counts = {
            'large_value': 0, 'large_core': 0, 'large_growth': 0,
            'mid_value': 0, 'mid_core': 0, 'mid_growth': 0,
            'small_value': 0, 'small_core': 0, 'small_growth': 0
        }
        
        total_style_count = 0
        
        for _, row in top_picks.iterrows():
            # Size
            market = row.get('market', 'KOSPI') # Default to KOSPI if missing
            is_large = market == 'KOSPI'
            
            # Style
            pbr = row.get('pbr', 1.5)
            if pd.isna(pbr): pbr = 1.5
            
            style_suffix = '_core'
            if pbr < 1.0: style_suffix = '_value'
            elif pbr > 2.5: style_suffix = '_growth'
            
            size_prefix = 'large' if is_large else 'small'
            
            key = f"{size_prefix}{style_suffix}"
            if key in style_counts:
                style_counts[key] += 1
                total_style_count += 1

        # Convert counts to percentages
        style_box = {}
        if total_style_count > 0:
            for k, v in style_counts.items():
                style_box[k] = round((v / total_style_count) * 100, 1)
        else:
             style_box = {k: 0 for k in style_counts}

        # Get latest date from the dataframe if available
        latest_date = None
        if 'current_date' in df.columns and not df.empty:
            latest_date = df['current_date'].iloc[0]
        elif 'recommendation_date' in df.columns and not df.empty:
             latest_date = df['recommendation_date'].max()

        # --- Market Indices ---
        market_indices = []
        indices_map = {
            '^DJI': 'Dow Jones',
            '^GSPC': 'S&P 500',
            '^IXIC': 'NASDAQ',
            '^RUT': 'Russell 2000',
            '^VIX': 'VIX',
            'GC=F': 'Gold',
            'CL=F': 'Crude Oil',
            'BTC-USD': 'Bitcoin',
            '^TNX': '10Y Treasury',
            'DX-Y.NYB': 'Dollar Index',
            'KRW=X': 'USD/KRW'
        }
        
        try:
            tickers_list = list(indices_map.keys())
            # Fetch data
            idx_data = yf.download(tickers_list, period='5d', progress=False, threads=True)
            
            # Process data
            if not idx_data.empty:
                # Handle MultiIndex columns if multiple tickers
                # If single ticker, columns are simple. But we requested list, so likely MultiIndex if >1
                # yfinance behavior depends on version, but usually MultiIndex (Price, Ticker) for multiple
                
                closes = idx_data['Close']
                
                for ticker, name in indices_map.items():
                    try:
                        # Get series for this ticker
                        if isinstance(closes, pd.DataFrame) and ticker in closes.columns:
                            series = closes[ticker].dropna()
                        elif isinstance(closes, pd.Series) and closes.name == ticker:
                            series = closes.dropna()
                        else:
                            # Fallback or skip
                            continue
                            
                        if len(series) >= 2:
                            current_val = series.iloc[-1]
                            prev_val = series.iloc[-2]
                            change = current_val - prev_val
                            change_pct = (change / prev_val) * 100
                            
                            market_indices.append({
                                'name': name,
                                'price': f"{current_val:,.2f}",
                                'change': f"{change:,.2f}",
                                'change_pct': change_pct,
                                'color': 'red' if change >= 0 else 'blue' # Red for up in Korea
                            })
                        elif len(series) == 1:
                             market_indices.append({
                                'name': name,
                                'price': f"{series.iloc[-1]:,.2f}",
                                'change': "0.00",
                                'change_pct': 0.0,
                                'color': 'gray'
                            })
                    except Exception as e:
                        print(f"Error processing index {ticker}: {e}")
                        
        except Exception as e:
            print(f"Error fetching market indices: {e}")

        data = {
            'key_stats': key_stats, # Keeping for backward compatibility if needed, or remove? Plan said replace section.
            'market_indices': market_indices, # New field
            'holdings_distribution': holdings_distribution,
            'top_holdings': top_holdings,
            'style_box': style_box,
            'performance': performance_data,
            'latest_date': latest_date
        }
        return jsonify(data)
    except Exception as e:
        print(f"Error getting portfolio data: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/portfolio')
def get_us_portfolio_data():
    """US Market Portfolio Data - Market Indices"""
    try:
        import time
        market_indices = []
        
        # US Market Indices
        indices_map = {
            '^DJI': 'Dow Jones',
            '^GSPC': 'S&P 500',
            '^IXIC': 'NASDAQ',
            '^RUT': 'Russell 2000',
            '^VIX': 'VIX',
            'GC=F': 'Gold',
            'CL=F': 'Crude Oil',
            'BTC-USD': 'Bitcoin',
            '^TNX': '10Y Treasury',
            'DX-Y.NYB': 'Dollar Index',
            'KRW=X': 'USD/KRW'
        }
        
        # Fetch each ticker individually with error handling
        for ticker, name in indices_map.items():
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period='5d')
                
                if not hist.empty and len(hist) >= 2:
                    current_val = float(hist['Close'].iloc[-1])
                    prev_val = float(hist['Close'].iloc[-2])
                    change = current_val - prev_val
                    change_pct = (change / prev_val) * 100
                    
                    market_indices.append({
                        'name': name,
                        'price': f"{current_val:,.2f}",
                        'change': f"{change:+,.2f}",
                        'change_pct': round(change_pct, 2),
                        'color': 'green' if change >= 0 else 'red'
                    })
                elif not hist.empty:
                    current_val = float(hist['Close'].iloc[-1])
                    market_indices.append({
                        'name': name,
                        'price': f"{current_val:,.2f}",
                        'change': "0.00",
                        'change_pct': 0,
                        'color': 'gray'
                    })
            except Exception as e:
                print(f"Error fetching {ticker} ({name}): {e}")

        return jsonify({
            'market_indices': market_indices,
            'top_holdings': [],
            'style_box': {}
        })
        
    except Exception as e:
        print(f"Error getting US portfolio data: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/smart-money')
def get_us_smart_money():
    """Get Smart Money Picks with performance tracking (Cached via DB)"""
    try:
        import math
        import json
        
        # 1. DB에서 가장 최근 날짜의 캐시된 데이터 조회
        from daily_db import get_conn, get_today_kst
        picks_with_perf = []
        last_updated = ""
        
        if DAILY_DB_ENABLED:
            with get_conn() as conn:
                latest = conn.execute("SELECT MAX(date) FROM smart_money_daily").fetchone()[0]
                if latest:
                    rows = conn.execute("SELECT * FROM smart_money_daily WHERE date = ? ORDER BY score DESC", (latest,)).fetchall()
                    for r in rows:
                        d = dict(r)
                        picks_with_perf.append({
                            'ticker': d.get('ticker'),
                            'name': d.get('name'),
                            'sector': d.get('sector'),
                            'final_score': d.get('score'),
                            'composite_score': d.get('score'),
                            'grade': d.get('grade'),
                            'current_price': d.get('price'),
                            'price_at_rec': d.get('price'),
                            'change_since_rec': d.get('price_change_1d'),
                            'volume_stage': 'N/A',
                            'smart_money_flow': d.get('smart_money_flow', 'N/A'),
                            'target_upside': 0,
                        })
                    last_updated = latest
        
        # 2. DB가 비어있으면 백엔드 CSV 파일 매핑 (yf_download 생략)
        if not picks_with_perf:
            csv_path = os.path.join('.', 'smart_money_picks_v2.csv')
            if not os.path.exists(csv_path):
                csv_path = os.path.join('.', 'smart_money_picks.csv')
                
            if not os.path.exists(csv_path):
                return jsonify({
                    'status': 'Generating',
                    'message': '캐시 생성 중...',
                    'last_updated': last_updated,
                    'top_picks': [],
                    'summary': {'total_analyzed': 0, 'avg_score': 0}
                }), 200
                
            df = pd.read_csv(csv_path)
            last_updated = "CSV Fallback"
            
            # Top 30개만 잘라서 전송 (데이터 폭주 방지)
            limit = int(request.args.get('limit', 30))
            if limit > 0:
                df = df.head(limit)
                
            for _, row in df.iterrows():
                ticker = row['ticker']
                rec_price = row.get('current_price', 0) if 'current_price' in row else 0
                
                # 안전한 get 처리 (KeyError 방지)
                def _safe_get(r, col, default=0):
                    return r[col] if col in r and pd.notna(r[col]) else default
                    
                picks_with_perf.append({
                    'ticker': ticker,
                    'name': _safe_get(row, 'name', ticker),
                    'sector': get_sector(ticker),
                    'final_score': _safe_get(row, 'composite_score', 0),
                    'composite_score': _safe_get(row, 'composite_score', 0),
                    'grade': _safe_get(row, 'grade', 'N/A'),
                    'target_upside': _safe_get(row, 'target_upside', 0),
                    'target_buy_price': _safe_get(row, 'target_buy_price', 0),
                    'target_sell_price': _safe_get(row, 'target_sell_price', 0),
                    'ai_recommendation': _safe_get(row, 'ai_recommendation', '분석 중'),
                    'current_price': round(rec_price, 2) if rec_price else 0,
                    'price_at_rec': round(rec_price, 2) if rec_price else 0,
                    'change_since_rec': 0,
                    'category': _safe_get(row, 'category', 'N/A'),
                    'volume_stage': _safe_get(row, 'volume_stage', 'N/A'),
                    'insider_score': _safe_get(row, 'insider_score', 0),
                    'avg_surprise': _safe_get(row, 'avg_surprise', 0),
                })

        # 모두 응답 반환
        return jsonify({
            'status': 'OK',
            'last_updated': last_updated,
            'top_picks': picks_with_perf,
            'summary': {
                'total_analyzed': len(picks_with_perf),
                'avg_score': round(sum(p['final_score'] for p in picks_with_perf) / max(1, len(picks_with_perf)), 1)
            }
        })
        
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"Error getting smart money picks: {e}")
        return jsonify({'error': str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# 📊 Wall Street 7섹션 Stock Detail Report API
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/api/stock_detail/<ticker>')
def get_stock_detail(ticker: str):
    """
    특정 종목의 Wall Street 리서치 애널리스트 스타일 7섹션 리포트 반환.

    우선순위:
      1. stock_reports.json 캐시 (screener 실행 후 저장된 데이터)
      2. ai_summaries.json  (GeminiGenerator가 생성한 텍스트 요약)
      3. 실시간 yfinance 데이터로 즉시 생성

    Returns:
      {
        ticker, company_name, total_score, letter_grade, grade_label,
        section1_overview, section2_financials, section3_valuation,
        section4_news_catalyst, section5_risks, section6_scenarios,
        section7_verdict
      }
    """
    ticker = ticker.upper().strip()

    try:
        # ── 1. 캐시 JSON 조회 ─────────────────────────────────
        report_cache_path = os.path.join(DATA_DIR, 'stock_reports.json')
        cached_report = None
        if os.path.exists(report_cache_path):
            try:
                with open(report_cache_path, 'r', encoding='utf-8') as f:
                    all_reports = json.load(f)
                cached_report = all_reports.get(ticker)
            except Exception as e:
                print(f"⚠️ Error reading stock_reports.json: {e}")

        # ── 2. AI 요약 주입 ─────────────────────────────────
        ai_summary_ko = ''
        ai_summary_en = ''
        ai_path = os.path.join(DATA_DIR, 'ai_summaries.json')
        if os.path.exists(ai_path):
            try:
                with open(ai_path, 'r', encoding='utf-8') as f:
                    ai_summaries = json.load(f)
                ticker_summary = ai_summaries.get(ticker, {})
                ai_summary_ko = ticker_summary.get('summary_ko', ticker_summary.get('summary', ''))
                ai_summary_en = ticker_summary.get('summary_en', '')
            except Exception as e:
                print(f"⚠️ Error reading ai_summaries.json: {e}")

        if cached_report:
            # 캐시 히트 — AI 요약만 최신으로 교체
            cached_report['section4_news_catalyst']['ai_catalyst_summary_ko'] = ai_summary_ko
            cached_report['section4_news_catalyst']['ai_catalyst_summary_en'] = ai_summary_en
            cached_report['cache_hit'] = True
            return jsonify(cached_report)

        # ── 3. 실시간 즉시 생성 (캐시 미스) ─────────────────
        print(f"🔍 Generating real-time report for {ticker}...")

        try:
            from smart_money_screener_v2 import WallStreetScreener, _sector_benchmark
        except ImportError as e:
            return jsonify({'error': f'Screener import failed: {e}'}), 500

        screener = WallStreetScreener(data_dir=DATA_DIR)

        # SPY 로드 (상대강도 계산용)
        try:
            spy = yf.Ticker("SPY")
            screener.spy_data = spy.history(period="3mo")
        except Exception:
            screener.spy_data = None

        # yfinance 데이터
        try:
            info = yf.Ticker(ticker).info or {}
        except Exception:
            info = {}

        # 더미 row (volume/13F 데이터 없는 경우 기본값 사용)
        dummy_row = pd.Series({
            'supply_demand_score': 50,
            'institutional_score': 50,
        })

        # 5개 섹션 점수 계산
        fin_score, fin_detail = screener.get_financial_score(info)
        val_score, val_detail = screener.get_valuation_score(info)
        tech_score, tech_detail = screener.get_technical_score(ticker)
        sm_score, sm_detail = screener.get_smart_money_score(dummy_row, ticker)
        analyst_score, analyst_detail = screener.get_analyst_score(info)

        total_score = round(fin_score + val_score + tech_score + sm_score + analyst_score, 1)
        letter, grade_label = WallStreetScreener.score_to_grade(total_score)

        # 7섹션 리포트
        report = screener.build_report_dict(
            ticker, info, dummy_row,
            fin_detail, val_detail, tech_detail, sm_detail, analyst_detail,
            total_score, letter, grade_label
        )

        # 뉴스 수집 (실시간)
        try:
            import xml.etree.ElementTree as ET
            news_url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
            resp = requests.get(news_url, timeout=5)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                news_items = []
                for item in root.findall('.//item')[:5]:
                    news_items.append({
                        'title': item.find('title').text if item.find('title') is not None else '',
                        'published': item.find('pubDate').text if item.find('pubDate') is not None else '',
                        'link': item.find('link').text if item.find('link') is not None else '',
                    })
                report['section4_news_catalyst']['news_items'] = news_items
        except Exception as e:
            print(f"⚠️ News fetch failed for {ticker}: {e}")

        # AI 요약 주입
        report['section4_news_catalyst']['ai_catalyst_summary_ko'] = ai_summary_ko
        report['section4_news_catalyst']['ai_catalyst_summary_en'] = ai_summary_en
        report['cache_hit'] = False

        return jsonify(report)

    except Exception as e:
        print(f"❌ Error in get_stock_detail({ticker}): {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/stock_detail/<ticker>/refresh', methods=['POST'])
def refresh_stock_detail(ticker: str):
    """
    특정 종목의 AI 요약을 강제 재생성 (Gemini 호출).
    POST /api/stock_detail/AAPL/refresh
    Body: {"lang": "ko"}  (optional, default: both)
    """
    ticker = ticker.upper().strip()
    try:
        from ai_summary_generator import NewsCollector, GeminiGenerator

        nc = NewsCollector()
        gen = GeminiGenerator()

        news = nc.get_news(ticker)
        data = {'ticker': ticker, 'composite_score': None, 'grade': None}

        # 캐시에서 점수 가져오기
        report_cache_path = os.path.join(DATA_DIR, 'stock_reports.json')
        if os.path.exists(report_cache_path):
            try:
                with open(report_cache_path, 'r', encoding='utf-8') as f:
                    all_reports = json.load(f)
                cached = all_reports.get(ticker, {})
                data['composite_score'] = cached.get('total_score')
                data['grade'] = cached.get('letter_grade')
            except Exception:
                pass

        import time
        summary_ko = gen.generate(ticker, data, news, lang='ko')
        time.sleep(1)
        summary_en = gen.generate(ticker, data, news, lang='en')

        # ai_summaries.json 업데이트
        ai_path = os.path.join(DATA_DIR, 'ai_summaries.json')
        ai_summaries = {}
        if os.path.exists(ai_path):
            try:
                with open(ai_path, 'r', encoding='utf-8') as f:
                    ai_summaries = json.load(f)
            except Exception:
                pass

        ai_summaries[ticker] = {
            'summary': summary_ko,
            'summary_ko': summary_ko,
            'summary_en': summary_en,
            'updated': datetime.now().isoformat(),
            'news_count': len(news),
        }

        with open(ai_path, 'w', encoding='utf-8') as f:
            json.dump(ai_summaries, f, ensure_ascii=False, indent=2)

        return jsonify({
            'ticker': ticker,
            'summary_ko': summary_ko,
            'summary_en': summary_en,
            'news_count': len(news),
            'updated': ai_summaries[ticker]['updated'],
        })

    except Exception as e:
        print(f"❌ Error refreshing summary for {ticker}: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/us/etf-flows')
def get_us_etf_flows():
    """Get ETF Fund Flow Analysis"""
    try:
        csv_path = os.path.join('.', 'us_etf_flows.csv')
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'ETF flows not found. Run analyze_etf_flows.py first.'}), 404
        
        df = pd.read_csv(csv_path)
        
        # Fix missing columns from analyze_etf_flows.py
        if 'name' not in df.columns:
            df['name'] = df['ticker']
        if 'category' not in df.columns:
            df['category'] = 'ETF'
            # Assign basic categories based on known tickers
            broad_tickers = ['SPY', 'QQQ', 'DIA', 'IWM']
            df.loc[df['ticker'].isin(broad_tickers), 'category'] = 'Broad Market'
            df.loc[df['ticker'].str.startswith('XL'), 'category'] = 'Sector'
        
        # Calculate market sentiment
        broad_market = df[df['category'] == 'Broad Market']
        broad_score = round(broad_market['flow_score'].mean(), 1) if not broad_market.empty else 50
        
        # Sector summary
        sector_flows = df[df['category'] == 'Sector'].to_dict(orient='records')
        
        # Top inflows and outflows
        top_inflows = df.nlargest(5, 'flow_score').to_dict(orient='records')
        top_outflows = df.nsmallest(5, 'flow_score').to_dict(orient='records')
        
        # Load AI analysis
        ai_analysis_text = ""
        ai_path = os.path.join('.', 'etf_flow_analysis.json')
        if os.path.exists(ai_path):
            try:
                with open(ai_path, 'r', encoding='utf-8') as f:
                    ai_data = json.load(f)
                    ai_analysis_text = ai_data.get('ai_analysis', '')
            except Exception as e:
                print(f"Error loading ETF AI analysis: {e}")

        return jsonify({
            'market_sentiment_score': broad_score,
            'sector_flows': sector_flows,
            'top_inflows': top_inflows,
            'top_outflows': top_outflows,
            'all_etfs': df.to_dict(orient='records'),
            'ai_analysis': ai_analysis_text
        })
        
    except Exception as e:
        print(f"Error getting ETF flows: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/stock-chart/<ticker>')
def get_us_stock_chart(ticker):
    """Get US stock chart data (OHLC) for candlestick chart"""
    try:
        # Get period from query params (default: 1y)
        period = request.args.get('period', '1y')
        valid_periods = ['1mo', '3mo', '6mo', '1y', '2y', '5y', 'max']
        if period not in valid_periods:
            period = '1y'
        
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)
        
        if hist.empty:
            return jsonify({'error': f'No data found for {ticker}'}), 404
        
        # Format for Lightweight Charts
        candles = []
        for date, row in hist.iterrows():
            candles.append({
                'time': int(date.timestamp()),
                'open': round(row['Open'], 2),
                'high': round(row['High'], 2),
                'low': round(row['Low'], 2),
                'close': round(row['Close'], 2)
            })
        
        return jsonify({
            'ticker': ticker,
            'period': period,
            'candles': candles
        })
        
    except Exception as e:
        print(f"Error getting US stock chart for {ticker}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/history-dates')
def get_us_history_dates():
    """Get list of available historical analysis dates"""
    try:
        history_dir = os.path.join('.', 'history')
        
        if not os.path.exists(history_dir):
            return jsonify({'dates': []})
        
        dates = []
        for f in os.listdir(history_dir):
            if f.startswith('picks_') and f.endswith('.json'):
                date_str = f[6:-5]  # Extract date from filename
                dates.append(date_str)
        
        dates.sort(reverse=True)  # Most recent first
        
        return jsonify({
            'dates': dates,
            'count': len(dates)
        })
        
    except Exception as e:
        print(f"Error getting history dates: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/history/<date>')
def get_us_history_by_date(date):
    """Get picks from a specific historical date with current performance"""
    try:
        import json
        import math
        
        history_file = os.path.join('.', 'history', f'picks_{date}.json')
        
        if not os.path.exists(history_file):
            return jsonify({'error': f'No analysis found for {date}'}), 404
        
        with open(history_file, 'r', encoding='utf-8') as f:
            snapshot = json.load(f)
        
        # Get current prices individually for better reliability
        tickers = [p['ticker'] for p in snapshot['picks']]
        current_prices = {}
        
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period='5d')
                if not hist.empty:
                    current_prices[ticker] = round(float(hist['Close'].dropna().iloc[-1]), 2)
            except Exception as e:
                print(f"Error fetching price for {ticker}: {e}")
        
        # Add performance data
        picks_with_perf = []
        for pick in snapshot['picks']:
            ticker = pick['ticker']
            price_at_rec = pick.get('price_at_analysis', 0) or 0
            current_price = current_prices.get(ticker, price_at_rec) or price_at_rec
            
            if isinstance(price_at_rec, float) and math.isnan(price_at_rec):
                price_at_rec = 0
            if isinstance(current_price, float) and math.isnan(current_price):
                current_price = price_at_rec
            
            if price_at_rec > 0:
                change_pct = ((current_price / price_at_rec) - 1) * 100
            else:
                change_pct = 0
            
            if isinstance(change_pct, float) and math.isnan(change_pct):
                change_pct = 0
            
            picks_with_perf.append({
                **pick,
                'sector': get_sector(ticker),
                'current_price': round(current_price, 2),
                'price_at_rec': round(price_at_rec, 2),
                'change_since_rec': round(change_pct, 2)
            })
        
        # Calculate average performance
        changes = [p['change_since_rec'] for p in picks_with_perf if p['price_at_rec'] > 0]
        avg_perf = round(sum(changes) / len(changes), 2) if changes else 0
        
        return jsonify({
            'analysis_date': snapshot.get('analysis_date', date),
            'analysis_timestamp': snapshot.get('analysis_timestamp', ''),
            'top_picks': picks_with_perf,
            'summary': {
                'total': len(picks_with_perf),
                'avg_performance': avg_perf
            }
        })
        
    except Exception as e:
        print(f"Error getting history for {date}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/macro-analysis')
def get_us_macro_analysis():
    """Get macro market analysis with live indicators + cached AI predictions"""
    try:
        import json
        
        # Get language and model preference
        lang = request.args.get('lang', 'ko')
        model = request.args.get('model', 'gemini')  # 'gemini' or 'gpt'
        
        # === LIVE MACRO INDICATORS ===
        macro_tickers = {
            'VIX': '^VIX',
            'DXY': 'DX-Y.NYB',
            'GOLD': 'GC=F',
            'OIL': 'CL=F',
            'BTC': 'BTC-USD',
            'ETH': 'ETH-USD',
            '10Y_Yield': '^TNX',
            '2Y_Yield': '^IRX',
            'SPY': 'SPY',
            'QQQ': 'QQQ',
            'USD/KRW': 'KRW=X'
        }
        
        macro_indicators = {}
        
        # === LOAD CACHED INDICATORS FIRST (for all 30+ indicators) ===
        # Determine which file to load based on model and language
        if model == 'gpt':
            if lang == 'en':
                analysis_path = os.path.join('.', 'macro_analysis_gpt_en.json')
            else:
                analysis_path = os.path.join('.', 'macro_analysis_gpt.json')
            # Fallback to gemini if GPT file doesn't exist
            if not os.path.exists(analysis_path):
                if lang == 'en':
                    analysis_path = os.path.join('.', 'macro_analysis_en.json')
                else:
                    analysis_path = os.path.join('.', 'macro_analysis.json')
        else:  # gemini (default)
            if lang == 'en':
                analysis_path = os.path.join('.', 'macro_analysis_en.json')
            else:
                analysis_path = os.path.join('.', 'macro_analysis.json')
        
        if not os.path.exists(analysis_path):
            analysis_path = os.path.join('.', 'macro_analysis.json')
        
        ai_analysis = "AI 분석을 로드할 수 없습니다. macro_analyzer.py를 실행하세요."
        
        if os.path.exists(analysis_path):
            with open(analysis_path, 'r', encoding='utf-8') as f:
                cached = json.load(f)
                ai_analysis = cached.get('ai_analysis', ai_analysis)
                # Start with cached indicators
                macro_indicators = cached.get('macro_indicators', {})
        
        # === UPDATE KEY INDICATORS WITH LIVE DATA ===
        live_tickers = {
            'VIX': '^VIX',
            'SPY': 'SPY',
            'QQQ': 'QQQ',
            'BTC': 'BTC-USD',
            'GOLD': 'GC=F',
            'USD/KRW': 'KRW=X'
        }
        
        try:
            import time as t
            for name, ticker in live_tickers.items():
                try:
                    stock = yf.Ticker(ticker)
                    hist = stock.history(period='5d')
                    
                    if not hist.empty and len(hist) >= 2:
                        current = float(hist['Close'].iloc[-1])
                        prev = float(hist['Close'].iloc[-2])
                        change = current - prev
                        change_pct = (change / prev) * 100 if prev != 0 else 0
                        
                        macro_indicators[name] = {
                            'current': round(current, 2),
                            'change_1d': round(change_pct, 2)
                        }
                    t.sleep(0.3)
                except Exception as e:
                    print(f"Error fetching live {name}: {e}")
        except Exception as e:
            print(f"Error in live data loop: {e}")
        
        return jsonify({
            'macro_indicators': macro_indicators,
            'ai_analysis': ai_analysis,
            'model': model,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Error getting macro analysis: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/economic-calendar', methods=['GET'])
def get_economic_calendar():
    try:
        translated_file = 'weekly_calendar_ko.json'
        
        # 1. 이미 번역된 캐시 파일이 있으면 바로 반환
        if os.path.exists(translated_file):
            with open(translated_file, 'r', encoding='utf-8') as f:
                return jsonify(json.load(f))
                
        # 2. 캐시가 없으면 원본을 로드해서 번역 시작
        with open('weekly_calendar.json', 'r', encoding='utf-8') as f:
            data = json.load(f)

        api_key = os.getenv('GOOGLE_API_KEY')
        if not api_key:
            return jsonify(data)
            
        import requests
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

        # 3. 캘린더 이벤트 설명 추가 및 한글화 (애널리스트 톤)
        for idx, event in enumerate(data.get('events', [])):
            prompt = f'''
너는 월가 매크로 경제 애널리스트다.
다음 미국 경제 일정(지표)에 대해 짧게 1~2문장으로 한글로 객관적인 해석을 제공하라.
이 지표의 실제(Actual) 수치가 예상(Forecast)을 상회하거나 하회할 때 주식시장에 미칠 영향을 데이터 기반으로 간결하게 설명하라. 감정적이거나 비유적인 표현은 절대 사용하지 마라.

지표 이름: {event.get('event', '')}
예상치/실제치: {event.get('description', '')}
영향도: {event.get('impact', '')}
'''
            payload = {"contents": [{"parts": [{"text": prompt}]}]}
            try:
                resp = requests.post(f"{url}?key={api_key}", json=payload)
                if resp.status_code == 200:
                    resp_data = resp.json()
                    analyst_desc = resp_data['candidates'][0]['content']['parts'][0]['text']
                    data['events'][idx]['description'] = f"{event.get('description', '')}\n\n[Analyst Comment]: {analyst_desc}"
            except Exception as e:
                pass # 에러 시 원문 유지

        # 4. 뉴스 한글 번역
        for idx, news in enumerate(data.get('news_feed', [])):
            prompt = f"다음 영어 뉴스 제목을 한국어로 자연스럽게 번역해줘. 오직 번역된 제목문장만 출력해:\n\n{news.get('title', '')}"
            payload = {"contents": [{"parts": [{"text": prompt}]}]}
            try:
                resp = requests.post(f"{url}?key={api_key}", json=payload)
                if resp.status_code == 200:
                    data['news_feed'][idx]['title'] = resp.json()['candidates'][0]['content']['parts'][0]['text'].strip()
            except Exception:
                pass

        # 5. 번역 완료 후 캐시 파일로 저장
        with open(translated_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return jsonify(data)
    except FileNotFoundError:
        return jsonify({
            'week_start': '',
            'week_end': '',
            'events': [],
            'news_feed': []
        })
    except Exception as e:
        print(f"Error reading calendar data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/sector-heatmap')
def get_us_sector_heatmap():
    """Get sector performance data for heatmap visualization"""
    try:
        import json
        
        # Load sector heatmap data
        heatmap_path = os.path.join('.', 'sector_heatmap.json')
        
        if not os.path.exists(heatmap_path):
            # Generate fresh data if not exists
            from us_market.sector_heatmap import SectorHeatmapCollector
            collector = SectorHeatmapCollector()
            data = collector.get_sector_performance('1d')
            return jsonify(data)
        
        with open(heatmap_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return jsonify(data)
        
    except Exception as e:
        print(f"Error getting sector heatmap: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/options-flow')
def get_us_options_flow():
    """Get options flow data"""
    try:
        import json
        
        # Load options flow data
        flow_path = os.path.join('.', 'options_flow.json')
        
        if not os.path.exists(flow_path):
            return jsonify({'error': 'Options flow data not found. Run options_flow.py first.'}), 404
        
        with open(flow_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return jsonify(data)
        
    except Exception as e:
        print(f"Error getting options flow: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/ai-summary/<ticker>')
def get_us_ai_summary(ticker):
    """Get AI-generated summary for a US stock"""
    try:
        import json
        
        # Get language preference
        lang = request.args.get('lang', 'ko')
        
        # Load AI summaries
        summary_path = os.path.join('.', 'ai_summaries.json')
        
        if not os.path.exists(summary_path):
            return jsonify({'error': 'AI summaries not found. Run ai_summary_generator.py first.'}), 404
        
        with open(summary_path, 'r', encoding='utf-8') as f:
            summaries = json.load(f)
        
        if ticker not in summaries:
            return jsonify({'error': f'Summary not found for {ticker}'}), 404
        
        summary_data = summaries[ticker]
        
        # Get summary in requested language (fallback to Korean if English not available)
        if lang == 'en':
            summary = summary_data.get('summary_en', summary_data.get('summary', ''))
        else:
            summary = summary_data.get('summary_ko', summary_data.get('summary', ''))
        
        return jsonify({
            'ticker': ticker,
            'summary': summary,
            'lang': lang,
            'news_count': summary_data.get('news_count', 0),
            'updated': summary_data.get('updated', '')
        })
        
    except Exception as e:
        print(f"Error getting AI summary for {ticker}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<ticker>')
def get_stock_info(ticker):
    ticker = str(ticker).zfill(6) # Ensure 6-digit format
    try:
        # 1. Get Metrics from Analysis Results
        metrics = {}
        analysis_path = 'wave_transition_analysis_results.csv'
        if os.path.exists(analysis_path):
            df = pd.read_csv(analysis_path, dtype={'ticker': str})
            df['ticker'] = df['ticker'].apply(lambda x: str(x).zfill(6))
            # Ensure ticker is string and padded if necessary
            stock_row = df[df['ticker'] == ticker]
            if not stock_row.empty:
                row = stock_row.iloc[0]
                metrics = {
                    'name': row['name'],
                    'score': float(row['final_investment_score']),
                    'grade': row['investment_grade'],
                    'wave_stage': row['wave_stage'],
                    'supply_demand': row['supply_demand_stage'],
                    'inst_trend': row.get('institutional_trend', 'N/A'),
                    'for_trend': row.get('foreign_trend', 'N/A'),
                    'sector': row['market']
                }

        # 2. Get Price History (Fetch 5Y from yfinance)
        price_history = []
        try:
            # Map ticker to Yahoo format
            yf_ticker = TICKER_TO_YAHOO_MAP.get(ticker)
            if not yf_ticker:
                yf_ticker = f"{ticker}.KS"
                
            stock = yf.Ticker(yf_ticker)
            hist = stock.history(period="5y")
            
            if not hist.empty:
                # Reset index to get Date column
                hist = hist.reset_index()
                
                # Convert to list of dicts
                for _, row in hist.iterrows():
                    # Handle different timezone/date formats
                    date_val = row['Date']
                    if hasattr(date_val, 'strftime'):
                        date_str = date_val.strftime('%Y-%m-%d')
                    else:
                        date_str = str(date_val).split(' ')[0]
                        
                    price_history.append({
                        'time': date_str,
                        'open': float(row['Open']),
                        'high': float(row['High']),
                        'low': float(row['Low']),
                        'close': float(row['Close']),
                        'volume': int(row['Volume'])
                    })
        except Exception as e:
            print(f"Error fetching history from yfinance for {ticker}: {e}")
            # Fallback to daily_prices.csv if yfinance fails
            prices_path = 'daily_prices.csv'
            if os.path.exists(prices_path):
                price_df = pd.read_csv(prices_path, dtype={'ticker': str})
                price_df['ticker'] = price_df['ticker'].apply(lambda x: str(x).zfill(6))
                stock_prices = price_df[price_df['ticker'] == ticker].copy()
                
                if 'date' in stock_prices.columns:
                    stock_prices['date'] = pd.to_datetime(stock_prices['date'])
                    stock_prices = stock_prices.sort_values('date')
                    
                    for _, row in stock_prices.iterrows():
                        price_history.append({
                            'time': row['date'].strftime('%Y-%m-%d'),
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['current_price'],
                            'volume': row['volume'] if 'volume' in row else 0
                        })

        # 3. Get AI Report Section
        ai_report_content = ""
        # Find latest report
        report_files = [f for f in os.listdir('.') if f.startswith('ai_analysis_report_') and f.endswith('.md')]
        if report_files:
            latest_report = sorted(report_files)[-1]
            with open(latest_report, 'r', encoding='utf-8') as f:
                full_report = f.read()
            
            import re
            # Pattern: ## 📌 .* \(Ticker\)
            pattern = re.compile(rf"## 📌 .* \({ticker}\)")
            match = pattern.search(full_report)
            
            if match:
                start_idx = match.start()
                next_match = re.search(r"## 📌 ", full_report[start_idx + 1:])
                if next_match:
                    end_idx = start_idx + 1 + next_match.start()
                    ai_report_content = full_report[start_idx:end_idx]
                else:
                    ai_report_content = full_report[start_idx:]

        return jsonify({
            'metrics': metrics,
            'price_history': price_history,
            'ai_report': ai_report_content
        })

    except Exception as e:
        print(f"Error getting stock detail for {ticker}: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/run-analysis', methods=['POST'])
def run_analysis():
    try:
        # Run analysis2.py and track_performance.py
        # We run them sequentially: analysis2.py -> track_performance.py
        # Using a thread or subprocess to avoid blocking
        
        def run_scripts():
            print("🚀 Starting Analysis...")
            try:
                # 1. Run Analysis
                subprocess.run(['python3', 'analysis2.py'], check=True)
                print("✅ Analysis Complete.")
                
                # 2. Run Performance Tracking
                subprocess.run(['python3', 'track_performance.py'], check=True)
                print("✅ Performance Tracking Complete.")
                
            except Exception as e:
                print(f"❌ Error running scripts: {e}")

        # Start in background thread
        thread = threading.Thread(target=run_scripts)
        thread.start()
        
        return jsonify({'status': 'started', 'message': 'Analysis started in background.'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/api/realtime-prices', methods=['POST'])
def get_realtime_prices():
    try:
        data = request.get_json()
        tickers = data.get('tickers', [])
        
        if not tickers:
            return jsonify({})
            
        # Add suffixes if missing (simple logic based on TICKER_SUFFIX_MAP)
        # We need to ensure TICKER_SUFFIX_MAP is available or re-load it if needed.
        # It is loaded at startup, so it should be available as a global.
        
        yf_tickers = []
        ticker_map = {} # yf_ticker -> original_ticker
        
        for t in tickers:
            # Ensure 6 digits (pad with zeros)
            t_padded = str(t).zfill(6)
            
            # Use the verified map
            yf_t = TICKER_TO_YAHOO_MAP.get(t_padded)
            
            if not yf_t:
                # Fallback if not in map (should be rare if map is complete)
                # Default to .KS
                yf_t = f"{t_padded}.KS"
                print(f"Warning: Ticker {t_padded} not found in map. Defaulting to {yf_t}")
            
            yf_tickers.append(yf_t)
            ticker_map[yf_t] = t # Map back to original input ticker for response
            
        # Fetch data in batch
        # period='1d' is enough to get current price and OHLC
        prices = {}
        
        print(f"DEBUG: Requesting {len(yf_tickers)} tickers from yfinance: {yf_tickers[:10]}...") # Log first 10
        
        # yfinance download
        df = yf.download(yf_tickers, period='1d', interval='1m', progress=False, threads=True)
        
        # Fill missing data (e.g. if a stock didn't trade in the last minute)
        if not df.empty:
            df = df.ffill()
        
        # Helper to extract data from a row
        def extract_ohlc(row):
            def safe_float(val):
                return float(val) if not pd.isna(val) else 0.0
                
            return {
                'current': safe_float(row['Close']),
                'open': safe_float(row['Open']),
                'high': safe_float(row['High']),
                'low': safe_float(row['Low']),
                # We can use the index (datetime) for the time, but for 1d bars in chart we usually need YYYY-MM-DD
                # However, for realtime updates on a daily candle, we just update the current day's candle.
                # Let's return the date string.
                'date': row.name.strftime('%Y-%m-%d') if hasattr(row, 'name') else datetime.now().strftime('%Y-%m-%d')
            }

        if len(yf_tickers) == 1:
            try:
                # Single ticker, df columns are simple
                last_row = df.iloc[-1]
                prices[tickers[0]] = extract_ohlc(last_row)
            except Exception as e:
                print(f"Error extracting single ticker data: {e}")
        else:
            # Multi-index columns
            try:
                last_row = df.iloc[-1]
                # last_row has MultiIndex (PriceType, Ticker)
                # We need to iterate over our requested tickers
                for yf_t in yf_tickers:
                    original_t = ticker_map.get(yf_t)
                    if original_t:
                        try:
                            # Extract data for this specific ticker
                            # We need to access cross-section or specific columns
                            # df['Close'][yf_t]
                            
                            # Handle NaN values
                            def safe_float(val):
                                return float(val) if not pd.isna(val) else 0.0

                            prices[original_t] = {
                                'current': safe_float(df['Close'][yf_t].iloc[-1]),
                                'open': safe_float(df['Open'][yf_t].iloc[-1]),
                                'high': safe_float(df['High'][yf_t].iloc[-1]),
                                'low': safe_float(df['Low'][yf_t].iloc[-1]),
                                'date': df.index[-1].strftime('%Y-%m-%d')
                            }
                        except Exception as inner_e:
                            # print(f"Error for {original_t}: {inner_e}")
                            pass
            except Exception as e:
                print(f"Error extracting multi ticker data: {e}")
                        
        return jsonify(prices)
        
    except Exception as e:
        print(f"Error fetching realtime prices: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/calendar')
def get_us_calendar():
    """Get Weekly Economic Calendar"""
    try:
        import json
        calendar_path = os.path.join('.', 'weekly_calendar.json')
        
        # If file doesn't exist, return empty
        if not os.path.exists(calendar_path):
            return jsonify({'events': [], 'message': 'Calendar data not available'}), 404
            
        with open(calendar_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        return jsonify(data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/technical-indicators/<ticker>')
def get_technical_indicators(ticker):
    """Get technical indicators (RSI, MACD, Bollinger Bands, Support/Resistance)"""
    try:
        import ta
        from ta.momentum import RSIIndicator
        from ta.trend import MACD
        from ta.volatility import BollingerBands
        
        period = request.args.get('period', '1y')
        
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)
        
        if hist.empty:
            return jsonify({'error': f'No data found for {ticker}'}), 404
        
        df = hist.reset_index()
        close = df['Close']
        high = df['High']
        low = df['Low']
        
        # RSI (14-period)
        rsi_indicator = RSIIndicator(close=close, window=14)
        df['rsi'] = rsi_indicator.rsi()
        
        # MACD (12, 26, 9)
        macd = MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        df['macd_line'] = macd.macd()
        df['signal_line'] = macd.macd_signal()
        df['macd_histogram'] = macd.macd_diff()
        
        # Bollinger Bands (20-period, 2 std)
        bb = BollingerBands(close=close, window=20, window_dev=2)
        df['bb_upper'] = bb.bollinger_hband()
        df['bb_middle'] = bb.bollinger_mavg()
        df['bb_lower'] = bb.bollinger_lband()
        
        # Support & Resistance detection (simple pivot-based)
        def find_support_resistance(df, window=20):
            supports = []
            resistances = []
            
            for i in range(window, len(df) - window):
                low_window = low.iloc[i-window:i+window+1]
                high_window = high.iloc[i-window:i+window+1]
                
                # Local minimum = Support
                if low.iloc[i] == low_window.min():
                    supports.append(float(low.iloc[i]))
                    
                # Local maximum = Resistance
                if high.iloc[i] == high_window.max():
                    resistances.append(float(high.iloc[i]))
            
            # Cluster and deduplicate (within 2% range)
            def cluster_levels(levels, threshold=0.02):
                if not levels:
                    return []
                levels = sorted(levels)
                clusters = []
                current_cluster = [levels[0]]
                
                for level in levels[1:]:
                    if (level - current_cluster[0]) / current_cluster[0] < threshold:
                        current_cluster.append(level)
                    else:
                        clusters.append(sum(current_cluster) / len(current_cluster))
                        current_cluster = [level]
                clusters.append(sum(current_cluster) / len(current_cluster))
                return [round(c, 2) for c in clusters[-5:]]  # Top 5 recent levels
            
            return cluster_levels(supports), cluster_levels(resistances)
        
        supports, resistances = find_support_resistance(df)
        
        # Prepare response
        def make_series(dates, values):
            result = []
            for date, val in zip(dates, values):
                if pd.notna(val):
                    result.append({
                        'time': int(date.timestamp()),
                        'value': round(float(val), 2)
                    })
            return result
        
        return jsonify({
            'ticker': ticker,
            'rsi': make_series(df['Date'], df['rsi']),
            'macd': {
                'macd_line': make_series(df['Date'], df['macd_line']),
                'signal_line': make_series(df['Date'], df['signal_line']),
                'histogram': make_series(df['Date'], df['macd_histogram'])
            },
            'bollinger': {
                'upper': make_series(df['Date'], df['bb_upper']),
                'middle': make_series(df['Date'], df['bb_middle']),
                'lower': make_series(df['Date'], df['bb_lower'])
            },
            'support_resistance': {
                'support': supports,
                'resistance': resistances
            }
        })
        
    except Exception as e:
        print(f"Error getting technical indicators for {ticker}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# =========================================================================
# 📊 보유 매트릭스 API - Holdings Matrix (전체 S&P 500)
# =========================================================================
@app.route('/api/us/holdings-matrix')
def get_holdings_matrix():
    try:
        import math
        picks_file = os.path.join(DATA_DIR, 'smart_money_picks_v2.csv')
        if not os.path.exists(picks_file):
            return jsonify({'error': 'smart_money_picks_v2.csv not found'}), 404

        df = pd.read_csv(picks_file, encoding='utf-8-sig')

        # 쿼리 파라미터
        sector_filter = request.args.get('sector', 'all')
        grade_filter  = request.args.get('grade', 'all')
        min_score     = float(request.args.get('min_score', 0))
        sort_by       = request.args.get('sort', 'score')
        page          = int(request.args.get('page', 1))
        per_page      = int(request.args.get('per_page', 50))
        search_query  = request.args.get('search', '').lower().strip()

        if search_query:
            per_page = 5000  # 검색 시 모든 결과 표시

        # 섹터 매핑 (volume_analysis에 sector 없으면 SECTOR_MAP 활용)
        def get_sector(ticker):
            # SECTOR_MAP is structured as {'AAPL': 'Tech', 'MSFT': 'Tech' ...}
            return SECTOR_MAP.get(ticker, 'Other')

        df['sector'] = df['ticker'].apply(get_sector)

        # 필터링
        if sector_filter != 'all':
            df = df[df['sector'] == sector_filter]
        if grade_filter != 'all':
            df = df[df['grade'].str.contains(grade_filter, na=False)]
        if min_score > 0:
            df = df[df['composite_score'] >= min_score]
            
        if search_query:
            # 영어 티커, 종목명 (영어/한글) 모두 검색 가능하도록
            df = df[
                df['ticker'].str.lower().str.contains(search_query, na=False) |
                df['name'].str.lower().str.contains(search_query, na=False)
            ]

        # 정렬
        sort_map = {
            'score': 'composite_score',
            'sd': 'sd_score',
            'inst': 'inst_score',
            'tech': 'tech_score',
            'upside': 'target_upside'
        }
        sort_col = sort_map.get(sort_by, 'composite_score')
        if sort_col in df.columns:
            df = df.sort_values(sort_col, ascending=False)

        # 섹터 × 등급 크로스탭
        grade_map = {'S': 'S급', 'A': 'A급', 'B': 'B급', 'C': 'C급', 'D': 'D급', 'F': 'F급'}
        def extract_grade_key(g):
            for k in ['S급', 'A급', 'B급', 'C급', 'D급', 'F급']:
                if k in str(g):
                    return k
            return '기타'
        df['grade_key'] = df['grade'].apply(extract_grade_key)

        sectors_list = sorted(df['sector'].unique().tolist())
        grades_list  = ['S급', 'A급', 'B급', 'C급', 'D급', 'F급']
        crosstab = {}
        for sec in sectors_list:
            crosstab[sec] = {}
            for grd in grades_list:
                cnt = len(df[(df['sector'] == sec) & (df['grade_key'] == grd)])
                crosstab[sec][grd] = cnt

        # 요약 통계
        summary = {
            'total': len(df),
            'avg_score': round(df['composite_score'].mean(), 1) if not df.empty else 0,
            'grade_counts': df['grade_key'].value_counts().to_dict(),
            'sector_counts': df['sector'].value_counts().to_dict(),
        }

        # 페이지네이션
        total_count = len(df)
        total_pages = math.ceil(total_count / per_page)
        start = (page - 1) * per_page
        end   = start + per_page
        page_df = df.iloc[start:end]

        stocks = []
        for _, row in page_df.iterrows():
            stocks.append({
                'ticker':          row.get('ticker', ''),
                'name':            row.get('name', ''),
                'sector':          row.get('sector', ''),
                'grade':           row.get('grade', ''),
                'grade_key':       row.get('grade_key', ''),
                'composite_score': float(row.get('composite_score', 0) or 0),
                'financial_score': float(row.get('financial_score', 0) or 0),
                'valuation_score': float(row.get('valuation_score', 0) or 0),
                'tech_score':      float(row.get('tech_score', 0) or 0),
                'smart_money_score': float(row.get('smart_money_score', 0) or 0),
                'analyst_score':   float(row.get('analyst_score', 0) or 0),
                'sd_score':        float(row.get('financial_score', 0) or 0),
                'inst_score':      float(row.get('valuation_score', 0) or 0),
                'fund_score':      float(row.get('smart_money_score', 0) or 0),
                'rs_score':        float(row.get('analyst_score', 0) or 0),
                'current_price':   float(row.get('current_price', 0) or 0),
                'target_upside':   float(row.get('upside_pct', row.get('target_upside', 0)) or 0),
                'upside_pct':      float(row.get('upside_pct', 0) or 0),
                'rsi':             float(row.get('rsi', 50) or 50),
                'recommendation':  row.get('recommendation', ''),
            })

        return jsonify({
            'stocks':      stocks,
            'summary':     summary,
            'crosstab':    crosstab,
            'sectors':     sectors_list,
            'grades':      grades_list,
            'pagination': {
                'page':        page,
                'per_page':    per_page,
                'total':       total_count,
                'total_pages': total_pages
            }
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# =========================================================================
# 📈 과거 수익률 API - Performance History
# =========================================================================
@app.route('/api/us/performance-history')
def get_performance_history():
    try:
        import yfinance as yf
        picks_file = os.path.join(DATA_DIR, 'smart_money_picks_v2.csv')
        if not os.path.exists(picks_file):
            return jsonify({'error': 'smart_money_picks_v2.csv not found'}), 404

        df = pd.read_csv(picks_file, encoding='utf-8-sig')
        period = request.args.get('period', '3mo')  # 1mo, 3mo, 6mo, 1y
        my_portfolio_str = request.args.get('my_portfolio', '')
        my_portfolio = []
        my_tickers = []
        if my_portfolio_str:
            import json
            from urllib.parse import unquote
            try:
                # Flask automatically unquotes request.args, so no need for unquote unless double encoded
                my_portfolio_json = unquote(my_portfolio_str)
                my_portfolio = json.loads(my_portfolio_json)
                my_tickers = [item.get('t', '').upper() for item in my_portfolio if item.get('t')]
                print("Successfully parsed my_portfolio:", my_portfolio)
                print("my_tickers:", my_tickers)
            except Exception as e:
                import traceback; traceback.print_exc()
                print("Error parsing my_portfolio:", e)

        # Top 20 종목
        df['composite_score'] = pd.to_numeric(df['composite_score'], errors='coerce').fillna(0)
        top_df = df.nlargest(20, 'composite_score')
        tickers = top_df['ticker'].tolist()
        
        all_tickers = list(set(tickers + my_tickers + ['SPY']))
        # Download all data in one request (huge performance boost)
        data = yf.download(all_tickers, period=period, progress=False)

        if data.empty or 'Close' not in data:
            return jsonify({'error': 'Failed to download data'}), 500
            
        close_prices = data['Close']
        if isinstance(close_prices, pd.Series):
            close_prices = pd.DataFrame(close_prices, columns=all_tickers)
            
        spy_col = 'SPY' if 'SPY' in close_prices.columns else None
        
        # Dates and Returns for SPY
        if spy_col and not close_prices[spy_col].dropna().empty:
            spy_closes = close_prices[spy_col].dropna()
            spy_start = float(spy_closes.iloc[0])
            spy_returns_full = ((spy_closes / spy_start) - 1) * 100
            spy_returns = [round(float(x), 2) for x in spy_returns_full.tolist()]
        else:
            spy_returns = []
            spy_returns_full = pd.Series(dtype=float)
            
        stock_results = []
        best_performer = None
        worst_performer = None
        
        for idx, row in top_df.iterrows():
            ticker = row['ticker']
            if ticker not in close_prices.columns:
                continue
                
            stock_series = close_prices[ticker].dropna()
            if len(stock_series) < 5:
                continue
                
            start_price = float(stock_series.iloc[0])
            end_price = float(stock_series.iloc[-1])
            if start_price == 0:
                continue
                
            total_return = (end_price / start_price - 1) * 100
            spy_ret_total = spy_returns[-1] if spy_returns else 0
            excess_return = total_return - spy_ret_total
            
            # 1개월 고점 대비 하락폭 계산 로직
            one_month_ago = stock_series.index[-1] - pd.Timedelta(days=30)
            recent_month_series = stock_series.loc[one_month_ago:]
            drop_from_high = 0
            if not recent_month_series.empty:
                one_month_high = recent_month_series.max()
                drop_from_high = (end_price / one_month_high - 1) * 100 if one_month_high > 0 else 0

            result = {
                'ticker': ticker,
                'name': row.get('name', ticker),
                'score': float(row.get('composite_score', 0)),
                'composite_score': float(row.get('composite_score', 0)),
                'grade': row.get('grade', row.get('letter_grade', '')),
                'letter_grade': row.get('letter_grade', row.get('grade', '')),
                'start_price': round(start_price, 2),
                'current_price': round(end_price, 2),
                'end_price': round(end_price, 2),
                'total_return': round(total_return, 2),
                'return_pct': round(total_return, 2),
                'excess_return': round(excess_return, 2),
                'drop_from_high': round(drop_from_high, 2),
                'drop_1m': round(drop_from_high, 2),  # 1개월 고점 대비 낙폭
            }
            stock_results.append(result)

            if best_performer is None or total_return > best_performer['total_return']:
                best_performer = result
            if worst_performer is None or total_return < worst_performer['total_return']:
                worst_performer = result

        if not stock_results:
            return jsonify({'error': 'No valid stock data found'}), 500
            
        # Calculate Average Portfolio Returns over time
        valid_tickers = [r['ticker'] for r in stock_results]
        port_prices = close_prices[valid_tickers].ffill()
        start_prices = port_prices.bfill().iloc[0]
        
        daily_returns = (port_prices / start_prices - 1) * 100
        port_daily_avg = daily_returns.mean(axis=1) # average return across 20 valid stocks
        
        # Sample to reduce series length (to ~30 pts)
        step = max(1, len(port_daily_avg) // 30)
        port_sampled = port_daily_avg.iloc[::step]
        port_sampled_returns = [round(float(x), 2) for x in port_sampled.tolist()]
        sampled_dates = [str(d.date()) for d in port_sampled.index]
        
        if spy_col and not spy_returns_full.empty:
            spy_sampled = spy_returns_full.reindex(port_sampled.index).ffill()
            sampled_spy_returns = [round(float(x), 2) for x in spy_sampled.tolist()]
        else:
            sampled_spy_returns = []

        # Calculate My Portfolio Returns using actual cost basis and quantity
        my_sampled_returns = []
        if my_portfolio:
            total_cost = sum(float(item.get('q', 0)) * float(item.get('p', 0)) for item in my_portfolio)
            valid_my_tickers = [item['t'].upper() for item in my_portfolio if item['t'].upper() in close_prices.columns]
            
            if total_cost > 0 and valid_my_tickers:
                daily_value = pd.Series(0.0, index=close_prices.index)
                for item in my_portfolio:
                    ticker = item['t'].upper()
                    qty = float(item.get('q', 0))
                    if qty > 0 and ticker in close_prices.columns:
                        daily_value += close_prices[ticker].ffill() * qty
                        
                my_daily_return = (daily_value / total_cost - 1) * 100
                my_sampled = my_daily_return.reindex(port_sampled.index).ffill()
                my_sampled_returns = [round(float(x), 2) for x in my_sampled.tolist()]

        returns = [r['total_return'] for r in stock_results]
        win_rate = round(sum(1 for r in returns if r > 0) / len(returns) * 100, 1) if returns else 0
        avg_return = round(sum(returns) / len(returns), 2) if returns else 0

        return jsonify({
            'period': period,
            'stocks': sorted(stock_results, key=lambda x: x['total_return'], reverse=True),
            'spy_returns': sampled_spy_returns,
            'portfolio_returns': port_sampled_returns,
            'my_portfolio_returns': my_sampled_returns,
            'spy_dates': sampled_dates,
            'summary': {
                'total_stocks': len(stock_results),
                'avg_return': avg_return,
                'win_rate': win_rate,
                'best': best_performer,
                'worst': worst_performer,
                'spy_return': round(spy_returns[-1], 2) if spy_returns else 0,
            }
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# =========================================================================
# ⚠️ 리스크 분석 API - Risk Analysis
# =========================================================================
@app.route('/api/us/risk-analysis')
def get_risk_analysis():
    try:
        picks_file = os.path.join(DATA_DIR, 'smart_money_picks_v2.csv')
        macro_file = os.path.join(DATA_DIR, 'macro_analysis.json')

        picks_df = pd.DataFrame()
        if os.path.exists(picks_file):
            picks_df = pd.read_csv(picks_file, encoding='utf-8-sig')

        # 섹터 배분
        def get_sector(ticker):
            for sector, tickers in SECTOR_MAP.items():
                if ticker in tickers:
                    return sector
            return 'Other'

        if not picks_df.empty:
            picks_df['sector'] = picks_df['ticker'].apply(get_sector)
            
            # 클라이언트에서 넘겨준 파라미터 확인 (내 포트폴리오 연동)
            user_tickers_param = request.args.get('tickers', '')
            if user_tickers_param:
                user_tickers = [t.strip().upper() for t in user_tickers_param.split(',')]
                portfolio_df = picks_df[picks_df['ticker'].isin(user_tickers)]
            else:
                # 파라미터가 없으면 A/S급 기본 포트폴리오
                portfolio_df = picks_df[picks_df['grade'].str.contains('A급|S급', na=False)]
                if portfolio_df.empty:
                    portfolio_df = picks_df.head(50)
            
            # 파라미터가 있으나 picks_df에 검색된 종목이 없으면 빈 데이터프레임 방지
            if portfolio_df.empty and user_tickers_param:
                portfolio_df = pd.DataFrame({'sector': ['Other'], 'grade': ['C급'], 'target_upside': [0], 'composite_score': [50], 'ticker': user_tickers[0]})

            sector_dist = portfolio_df['sector'].value_counts().to_dict()
            total = sum(sector_dist.values())
            sector_pct = {k: round(v / total * 100, 1) for k, v in sector_dist.items()}

            # 집중도 HHI (허핀달-허시만 지수)
            hhi = sum((v/100)**2 for v in sector_pct.values()) * 10000
            concentration_level = '높음 🔴' if hhi > 2500 else ('중간 🟡' if hhi > 1500 else '낮음 🟢')

            # 종목별 점수 분포
            score_dist = {
                'S급': int(portfolio_df['grade'].str.contains('S급', na=False).sum()),
                'A급': int(portfolio_df['grade'].str.contains('A급', na=False).sum()),
                'B급': int(portfolio_df['grade'].str.contains('B급', na=False).sum()),
                'C급': int(portfolio_df['grade'].str.contains('C급', na=False).sum()),
                '기타': int(portfolio_df['grade'].str.contains('D급|F급', na=False).sum()),
            }
            avg_score    = round(float(portfolio_df['composite_score'].mean()), 1)
            avg_upside   = round(float(portfolio_df['target_upside'].mean()), 1) if 'target_upside' in portfolio_df.columns else 0
        else:
            sector_pct = {}
            score_dist = {}
            avg_score  = 0
            avg_upside = 0
            hhi = 0
            concentration_level = '알 수 없음'
        # 매크로 지표
        macro_indicators = {}
        if os.path.exists(macro_file):
            with open(macro_file, encoding='utf-8') as f:
                macro_data = json.load(f)
            macro_indicators = macro_data.get('macro_indicators', {})

        # 'value' 키로 읽기 (JSON 구조에 맞게 수정)
        vix_raw        = macro_indicators.get('VIX', {}).get('value', None)
        yield_raw      = macro_indicators.get('YieldSpread', {}).get('value', None)
        fear_greed_raw = macro_indicators.get('FearGreed', {}).get('value', None)

        # ── 실시간 yfinance 폴백 ──────────────────────
        try:
            import yfinance as yf

            # VIX 실시간 (^VIX)
            if vix_raw is None:
                _vix = yf.Ticker('^VIX')
                _vh = _vix.history(period='1d', interval='1m')
                if not _vh.empty:
                    vix_raw = float(_vh['Close'].iloc[-1])

            # 수익률 곡선 스프레드 실시간 (^TNX 10년 - ^IRX 3개월)
            if yield_raw is None:
                try:
                    _t10 = yf.Ticker('^TNX').history(period='1d')
                    _t2  = yf.Ticker('^TYX').history(period='1d')  # 30년 대신 2년은 ^IRX
                    _t2y = yf.Ticker('^IRX').history(period='1d')
                    t10 = float(_t10['Close'].iloc[-1]) / 10 if not _t10.empty else 4.5
                    t2  = float(_t2y['Close'].iloc[-1]) / 100 if not _t2y.empty else 4.3
                    yield_raw = round(t10 - t2, 2)
                except Exception:
                    yield_raw = 0.0

            # FearGreed - CNN API 시도
            if fear_greed_raw is None:
                try:
                    import urllib.request
                    req = urllib.request.Request(
                        'https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
                        headers={'User-Agent': 'Mozilla/5.0'}
                    )
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        fg_data = json.loads(resp.read())
                    fear_greed_raw = float(fg_data.get('fear_and_greed', {}).get('score', 50))
                except Exception:
                    # yfinance로 근사치 계산 (SPY 20일 모멘텀 기반)
                    try:
                        _spy = yf.Ticker('SPY').history(period='1mo')
                        if not _spy.empty and len(_spy) >= 5:
                            ret_20 = (_spy['Close'].iloc[-1] / _spy['Close'].iloc[0] - 1) * 100
                            fear_greed_raw = max(10, min(90, 50 + ret_20 * 3))
                        else:
                            fear_greed_raw = 50
                    except Exception:
                        fear_greed_raw = 50

        except Exception as yf_err:
            print(f'Warning: yfinance fallback failed: {yf_err}')

        # 최종값 설정 (None이면 기본값)
        vix          = float(vix_raw or 20)
        fear_greed   = float(fear_greed_raw or 50)
        yield_spread = float(yield_raw or 0)


        # 리스크 수준 판단
        vix_level   = '위험 🔴' if vix > 30 else ('주의 🟡' if vix > 20 else '안전 🟢')
        fg_level    = '과열 🔴' if fear_greed > 75 else ('탐욕 🟡' if fear_greed > 55 else ('중립 ⚪' if fear_greed > 40 else '공포 🟢'))
        curve_level = '역전 ⚠️' if yield_spread < 0 else '정상 ✅'

        # 변동성 점수 (0~100, 높을수록 위험)
        vix_score = min(100, max(0, (vix - 10) * 3))

        # 리스크 종합 점수
        risk_score = round((vix_score * 0.4 + (100 - fear_greed) * 0.3 + (50 if yield_spread < 0 else 20) * 0.3), 1)
        risk_level = '높음 🔴' if risk_score > 60 else ('중간 🟡' if risk_score > 40 else '낮음 🟢')


        # 내 포트폴리오 리스크 종목 (변동성 지표 기반)
        risk_stocks = []
        if not picks_df.empty and 'portfolio_df' in locals() and not portfolio_df.empty:
            top10 = portfolio_df.nlargest(50, 'composite_score').head(15)
            for _, row in top10.iterrows():
                def _safe_get(r, col, default=0):
                    val = r.get(col, default)
                    return float(val) if pd.notna(val) else default
                    
                risk_stocks.append({
                    'ticker':    row.get('ticker', ''),
                    'name':      row.get('name', ''),
                    'sector':    row.get('sector', ''),
                    'grade':     row.get('grade', ''),
                    'score':     _safe_get(row, 'composite_score', 0),
                    'sd_score':  _safe_get(row, 'sd_score', 50),
                    'inst_score': _safe_get(row, 'inst_score', 50),
                    'upside':    _safe_get(row, 'target_upside', 0),
                    'price':     _safe_get(row, 'current_price', 0),
                })

        return jsonify({
            'market_risk': {
                'vix':          float(vix or 20),
                'vix_level':    vix_level,
                'fear_greed':   float(fear_greed or 50),
                'fg_level':     fg_level,
                'yield_spread': float(yield_spread or 0),
                'curve_level':  curve_level,
                'risk_score':   risk_score,
                'risk_level':   risk_level,
            },
            'portfolio_risk': {
                'sector_pct':          sector_pct,
                'hhi':                 round(hhi, 0),
                'concentration_level': concentration_level,
                'avg_score':           avg_score,
                'avg_upside':          avg_upside,
                'grade_dist':          score_dist,
            },
            'stocks': risk_stocks,
            'timestamp': pd.Timestamp.now().isoformat(),
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/us/realtime-prices')
def get_my_portfolio_realtime_prices():
    """내 포트폴리오 보유 종목 현재가 일괄 조회"""
    try:
        tickers_raw = request.args.get('tickers', '')
        if not tickers_raw:
            return jsonify({'error': 'No tickers provided'}), 400

        tickers = [t.strip().upper() for t in tickers_raw.split(',') if t.strip()]
        if len(tickers) > 50:
            return jsonify({'error': 'Too many tickers (max 50)'}), 400

        result = {}
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period='5d')
                if not hist.empty:
                    last_close = float(hist['Close'].dropna().iloc[-1])
                    prev_close = float(hist['Close'].dropna().iloc[-2]) if len(hist) >= 2 else last_close
                    change_pct = ((last_close - prev_close) / prev_close * 100) if prev_close > 0 else 0
                    result[ticker] = {
                        'price': round(last_close, 2),
                        'prev_close': round(prev_close, 2),
                        'change_pct': round(change_pct, 2),
                        'name': stock.info.get('shortName', ticker) if hasattr(stock, 'info') else ticker,
                        'sector': get_sector(ticker),
                    }
                else:
                    result[ticker] = {'price': 0, 'prev_close': 0, 'change_pct': 0, 'name': ticker, 'sector': '-'}
            except Exception as e:
                print(f"Error fetching price for {ticker}: {e}")
                result[ticker] = {'price': 0, 'prev_close': 0, 'change_pct': 0, 'name': ticker, 'sector': '-'}

        return jsonify({'prices': result, 'count': len(result)})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/us/portfolio-diagnosis', methods=['POST'])
def get_portfolio_diagnosis():
    """내 포트폴리오 진단 AI 코멘트 (상세판 + 주요 뉴스)"""
    try:
        data = request.json
        if not data or 'portfolio' not in data:
            return jsonify({'error': 'No portfolio data provided'}), 400
            
        portfolio = data['portfolio']
        if not portfolio:
            return jsonify({'comment': '대장! 아직 포트폴리오에 종목이 없어! 종목을 추가해주면 내가 분석해줄게! 😊'})
            
        # 1. 포트폴리오 요약 문자열 생성 및 뉴스 수집
        summary_lines = []
        news_snippets = []
        
        for item in portfolio[:10]: # 최대 10종목까지만 제한 (API 비용 방지)
            ticker = item.get('ticker')
            ret = item.get('returnPct', 0)
            summary_lines.append(f"- {ticker}: 비중 {item.get('weight', 0)}%, 수익률 {ret}%")
            
            # 종목별 최신 뉴스 1~2개 가져오기
            try:
                stock_obj = yf.Ticker(ticker)
                stock_news = getattr(stock_obj, 'news', None)
                if callable(stock_news):
                    stock_news = stock_news()
                if stock_news and isinstance(stock_news, list) and len(stock_news) > 0:
                    first = stock_news[0]
                    title = first.get('title', '') if isinstance(first, dict) else ''
                    if title: news_snippets.append(f"[{ticker}] {title}")
            except Exception:
                pass
                
        port_desc = "\n".join(summary_lines)
        news_desc = "\n".join(news_snippets[:5]) # 뉴스는 최대 5개만
        
        # 2. Gemini 호출
        api_key = os.getenv('GOOGLE_API_KEY')
        if not api_key:
            return jsonify({'comment': '[🟢 Analyst Mode] API 키가 누락되어 분석을 진행할 수 없습니다.'})
            
        import datetime
        now_str = datetime.datetime.now().strftime('%y.%m.%d_%H:%M:%S')
        
        prompt = f'''너는 월가 리서치 애널리스트다.
포트폴리오 및 종목을 분석할 때 감정적 설명이나 비유를 사용하지 말고 데이터 기반 투자 리포트 형식으로 작성하라.

[포트폴리오 상태 요약]
총 수익률: {data.get('totalReturnPct', 0)}%
{port_desc}

[포트폴리오 관련 최근 뉴스 요약]
{news_desc}

다음 순서로 분석하라.
1. 포트폴리오 전체 개요 및 평가
- 자산 배분 비중 및 주요 수익 요인 분석

2. 개별 종목 개요
- 사업 모델, 주요 수익원, 시장 위치 중심 요약

3. 개별 종목 재무 및 밸류에이션 분석
- 성장성, 이익률, 부채 비율 및 상대 가치 평가

4. 최근 뉴스 및 투자 논리 변화 (어닝, 이슈 중심)
- 규제, 실적 변화, 자금 조달 이슈 확인

5. 투자 시나리오 
- 포트폴리오의 전체적인 Bull, Base, Bear 사례

6. 추가 편입 추천 1개 종목
- 현재 포트폴리오에 없는 추천 종목과 매수/매도 단가 명시

7. 종합 판단 등급 (추천 종목에 한하여)
- 등급 기준:
S = 강력한 매수 (구조적 성장 + 밸류 매력)
A = 매수
B = 중립
C = 약한 투자
D = 회피
F = 투자 부적합

작성 규칙:
0. ⚠️ 답변 첫 줄에는 무조건 다음 상태라인을 출력해: [🟢 Analyst Mode | {now_str}] (Asia/Seoul)
'''
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}]
        }
        resp = requests.post(f"{url}?key={api_key}", json=payload)
        
        if resp.status_code == 200:
            resp_data = resp.json()
            comment = resp_data['candidates'][0]['content']['parts'][0]['text']
        else:
            comment = f"[🟢 Analyst Mode | {now_str}] (Asia/Seoul)\n\n일시적인 서버 오류로 인해 AI 분석을 불러올 수 없습니다."
            
        return jsonify({'comment': comment})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/us/stock-scores')
def get_stock_scores():
    """종목별 스마트머니 점수 + 등급 조회"""
    try:
        tickers_param = request.args.get('tickers', '')
        if not tickers_param:
            return jsonify({'error': 'tickers parameter required'}), 400
        
        tickers = [t.strip().upper() for t in tickers_param.split(',')]
        
        # Load CSV
        csv_path = os.path.join('.', 'smart_money_picks_v2.csv')
        if not os.path.exists(csv_path):
            return jsonify({'error': 'Score data not found'}), 404
        
        df = pd.read_csv(csv_path)
        
        result = {}
        for ticker in tickers:
            row = df[df['ticker'] == ticker]
            if not row.empty:
                r = row.iloc[0]
                result[ticker] = {
                    'score': round(float(r.get('composite_score', 0)), 1),
                    'grade': str(r.get('grade', 'N/A')),
                    'rank': int(row.index[0]) + 1,
                    'total': len(df),
                }
            else:
                result[ticker] = {'score': 0, 'grade': '📊 분석 대상 외', 'rank': 0, 'total': len(df)}
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/us/ailey-question', methods=['POST'])
def ailey_question():
    """에일리에게 종목 관련 질문하기"""
    try:
        data = request.json
        ticker = data.get('ticker', '시장 전체')
        question = data.get('question', '')
        
        if not question:
            return jsonify({'error': 'No question provided'}), 400
        
        api_key = os.getenv('GOOGLE_API_KEY')
        if not api_key:
            return jsonify({'answer': '대장! API 키가 없어서 대답을 못 하겠어! 😢'})
        
        import datetime
        now_str = datetime.datetime.now().strftime('%y.%m.%d_%H:%M:%S')
        
        prompt = f'''너는 '에일리(Ailey)'라는 이름의 똑똑하지만 상냥한 초등학생 투자 천재야!
사용자(대장)가 {ticker}에 대해 이렇게 물어봤어: "{question}"

답변 규칙:
0. ⚠️ 답변 첫 줄에는 무조건: [🟢 Online Mode | {now_str}] (Asia/Seoul)
1. 한국어 반말로 친근하게 대답!
2. 이모지 많이 쓰기! (😊📈🚀💰 등)
3. 초등학생 비유(날씨/놀이터/소풍 등) 꼭 1개 이상!
4. 길고 자세하게 (최소 300자 이상)!
5. 출처가 확실하지 않은 건 "에일리가 아는 선에선~" 이라고 먼저 말할 것.
'''
        
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        resp = requests.post(f"{url}?key={api_key}", json=payload, timeout=30)
        
        if resp.status_code == 200:
            answer = resp.json()['candidates'][0]['content']['parts'][0]['text']
            return jsonify({'answer': answer, 'ticker': ticker})
        else:
            return jsonify({'answer': f'[🟢 Online Mode | {now_str}] (Asia/Seoul)\n\n대장! 서버가 좀 이상해! 나중에 다시 물어봐줘! 😭'})
    
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


import threading
import subprocess

@app.route('/api/admin/trigger-update', methods=['POST'])
def trigger_update():
    """웹에서 '전체 데이터 최신화' 버튼을 눌렀을 때 백그라운드로 스크리너를 실행합니다."""
    def run_screener_bg():
        try:
            print("🚀 [Background] 백그라운드 스크리닝 시작...")
            # 1. 한국 시장 수집
            subprocess.run(['python', 'kr_market_collector.py'], check=False)
            # 2. 미국 시장 전체 분석
            subprocess.run(['python', 'smart_money_screener_v2.py', '--top', '50'], check=False)
            print("✅ [Background] 백그라운드 데이터 최신화 완료!")
        except Exception as e:
            print(f"❌ [Background] 업데이트 중 에러 발생: {e}")

    # 백그라운드 스레드로 실행 (응답은 즉시 반환)
    bg_thread = threading.Thread(target=run_screener_bg)
    bg_thread.start()
    
    return jsonify({"message": "✅ 백그라운드에서 데이터를 최신화하고 있어 대장! (약 5~10분 소요)"})

@app.route('/api/us/batch-prices')
def get_batch_prices():
    """가상 투자 탭용: 특정 티커들의 현재가를 빠르게 반환"""
    tickers_param = request.args.get('tickers', '')
    tickers = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]
    if not tickers:
        return jsonify({'prices': {}, 'error': 'No tickers provided'})
    prices = {}
    try:
        import math
        data = yf.download(tickers, period='2d', progress=False, auto_adjust=True)
        if not data.empty:
            closes = data['Close'] if 'Close' in data else data
            for ticker in tickers:
                try:
                    if isinstance(closes, pd.DataFrame) and ticker in closes.columns:
                        val = closes[ticker].dropna().iloc[-1]
                    elif isinstance(closes, pd.Series):
                        val = closes.dropna().iloc[-1]
                    else:
                        val = 0
                    prices[ticker] = round(float(val), 2) if not (isinstance(val, float) and math.isnan(val)) else 0
                except Exception:
                    prices[ticker] = 0
    except Exception as e:
        print(f'batch-prices error: {e}')
        for t in tickers:
            prices[t] = 0
    return jsonify({'prices': prices})


@app.route('/api/us/s-grade-picks')
def get_s_grade_picks():
    """가상 투자 탭용: S등급 종목 목록 반환 (stock_reports.json에서 설명 포함)"""
    try:
        csv_path = os.path.join(DATA_DIR, 'smart_money_picks_v2.csv')
        if not os.path.exists(csv_path):
            return jsonify({'picks': [], 'error': 'CSV not found'}), 404
        df = pd.read_csv(csv_path)
        s_df = df[df['letter_grade'] == 'S'].copy()

        # stock_reports.json에서 비즈니스 요약 로드
        reports = {}
        reports_path = os.path.join(DATA_DIR, 'stock_reports.json')
        if os.path.exists(reports_path):
            with open(reports_path, 'r', encoding='utf-8') as f:
                reports = json.load(f)

        picks = []
        for _, row in s_df.iterrows():
            ticker = row['ticker']
            rpt = reports.get(ticker, {})
            overview = rpt.get('section1_overview', {})
            picks.append({
                'ticker': ticker,
                'name': row.get('name', ticker),
                'sector': row.get('sector', get_sector(ticker)),
                'composite_score': float(row.get('composite_score', 0)),
                'letter_grade': row.get('letter_grade', 'S'),
                'grade': row.get('grade', ''),
                'current_price': float(row.get('current_price', 0) or 0),
                'upside_pct': float(row.get('upside_pct', 0) or 0),
                'target_buy_price': float(row.get('target_buy_price', 0) or 0),
                'target_sell_price': float(row.get('target_sell_price', 0) or 0),
                'revenue_growth_pct': float(row.get('revenue_growth_pct', 0) or 0),
                'operating_margin_pct': float(row.get('operating_margin_pct', 0) or 0),
                'pe_ratio': str(row.get('pe_ratio', 'N/A')),
                'recommendation': row.get('recommendation', ''),
                'business_summary': overview.get('business_summary', '') or '',
                'industry': overview.get('industry', '') or '',
                'market_cap_b': overview.get('market_cap_b', 0) or 0,
            })
        return jsonify({'picks': picks, 'count': len(picks)})
    except Exception as e:
        print(f's-grade-picks error: {e}')
        return jsonify({'picks': [], 'error': str(e)}), 500


# ─────────────────────────────────────────────────────────────
# 히스토리 조회 API (프론트엔드용)
# ─────────────────────────────────────────────────────────────

@app.route('/api/history/portfolio')
def api_portfolio_history():
    """포트폴리오 일별 수익률 히스토리"""
    if not DAILY_DB_ENABLED:
        return jsonify({'error': 'DB disabled'}), 503
    try:
        ticker = request.args.get('ticker')
        days   = int(request.args.get('days', 30))
        data   = get_portfolio_history(ticker=ticker, days=days)
        return jsonify({'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/smart-money')
def api_smart_money_history():
    """Smart Money 종목 히스토리"""
    if not DAILY_DB_ENABLED:
        return jsonify({'error': 'DB disabled'}), 503
    try:
        ticker = request.args.get('ticker')
        days   = int(request.args.get('days', 30))
        data   = get_smart_money_history(ticker=ticker, days=days)
        return jsonify({'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/market')
def api_market_history():
    """시장 지수 히스토리"""
    if not DAILY_DB_ENABLED:
        return jsonify({'error': 'DB disabled'}), 503
    try:
        symbol = request.args.get('symbol')
        days   = int(request.args.get('days', 90))
        data   = get_market_history(symbol=symbol, days=days)
        return jsonify({'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/ailey')
def api_ailey_history():
    """Ailey 분석 히스토리"""
    if not DAILY_DB_ENABLED:
        return jsonify({'error': 'DB disabled'}), 503
    try:
        ticker        = request.args.get('ticker')
        analysis_type = request.args.get('type')
        days          = int(request.args.get('days', 30))
        data          = get_ailey_history(ticker=ticker, analysis_type=analysis_type, days=days)
        return jsonify({'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/stats')
def api_history_stats():
    """DB 저장 현황 통계"""
    if not DAILY_DB_ENABLED:
        return jsonify({'error': 'DB disabled'}), 503
    try:
        return jsonify(get_db_stats())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/portfolio/save', methods=['POST'])
def api_save_portfolio_snapshot():
    """
    프론트엔드에서 포트폴리오 스냅샷 저장 요청
    Body: {holdings: [{ticker, name, quantity, avg_price, current_price, ...}]}
    """
    if not DAILY_DB_ENABLED:
        return jsonify({'error': 'DB disabled'}), 503
    try:
        holdings = request.json.get('holdings', [])
        if not holdings:
            return jsonify({'error': 'holdings empty'}), 400
        save_portfolio_snapshot(holdings)
        return jsonify({'ok': True, 'saved': len(holdings)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/health')
def health_check():
    """배포 후 상태 확인용 엔드포인트"""
    status = {
        'status': 'ok',
        'message': 'Healthy',
        'db_path': os.getenv('DB_PATH', 'default (local)'),
        'db_connected': False
    }
    
    if DAILY_DB_ENABLED:
        try:
            from daily_db import get_conn
            with get_conn() as conn:
                tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                table_names = [t[0] for t in tables]
                
                required_tables = ['portfolio_daily', 'smart_money_daily', 'market_indices_daily', 'ailey_analysis_daily']
                missing = [t for t in required_tables if t not in table_names]
                
                if missing:
                    status['status'] = 'error'
                    status['message'] = f'Missing tables: {missing}'
                    return jsonify(status), 500
                    
                status['db_connected'] = True
                status['tables'] = table_names
        except Exception as e:
            status['status'] = 'error'
            status['message'] = f'DB Connection Error: {str(e)}'
            return jsonify(status), 500
            
    return jsonify(status), 200


# ─────────────────────────────────────────────────────────────
# 앱 시작 시 DB 초기화 + 스케줄러 등록
# ─────────────────────────────────────────────────────────────
def _start_scheduler():
    if not SCHEDULER_ENABLED or not DAILY_DB_ENABLED:
        print("⚠️ 스케줄러 또는 DB 비활성화 상태")
        return
    try:
        from daily_collector import run_daily_collection
        scheduler = BackgroundScheduler(timezone=ZoneInfo("Asia/Seoul"))
        # 매일 KST 00:05 실행 (장 마감 후 데이터 안정화)
        scheduler.add_job(
            func=run_daily_collection,
            trigger=CronTrigger(hour=0, minute=5, timezone=ZoneInfo("Asia/Seoul")),
            id="daily_collect",
            replace_existing=True,
        )
        scheduler.start()
        print("⏰ 스케줄러 시작: 매일 KST 00:05 자동 수집")
    except Exception as e:
        print(f"❌ 스케줄러 시작 실패: {e}")


if __name__ == '__main__':
    # DB 초기화
    if DAILY_DB_ENABLED:
        init_db()

    # 백그라운드 스케줄러 시작
    _start_scheduler()

    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_ENV', 'production') != 'production'
    print(f'Flask Server Starting on port {port} (debug={debug})...')
    app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=False)

