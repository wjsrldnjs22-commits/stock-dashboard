#!/usr/bin/env python3
import os, json, logging
import pandas as pd
import numpy as np
import yfinance as yf

logging.basicConfig(level=logging.INFO)

class PortfolioRiskAnalyzer:
    def analyze_portfolio(self, tickers):
        try:
            data = yf.download(tickers, period='6mo', progress=False)['Close']
            returns = data.pct_change().dropna()
            
            # Correlation
            corr = returns.corr()
            high_corr = []
            cols = corr.columns
            for i in range(len(cols)):
                for j in range(i+1, len(cols)):
                    if corr.iloc[i, j] > 0.8:
                        high_corr.append([cols[i], cols[j], round(corr.iloc[i, j], 2)])
            
            # Volatility
            cov = returns.cov() * 252
            weights = np.array([1/len(tickers)] * len(tickers))
            var = np.dot(weights.T, np.dot(cov, weights))
            vol = np.sqrt(var)
            
            result = {
                'volatility': round(vol * 100, 2),
                'high_correlations': high_corr,
                'matrix': corr.round(2).to_dict()
            }
            
            with open('portfolio_risk.json', 'w') as f:
                json.dump(result, f, indent=2)
            logging.info(f"Risk Analysis: Volatility {vol*100:.1f}%")
            
        except Exception as e:
            logging.error(f"Error: {e}")

if __name__ == "__main__":
    # Example
    PortfolioRiskAnalyzer().analyze_portfolio(['AAPL', 'NVDA', 'MSFT', 'GOOGL'])
