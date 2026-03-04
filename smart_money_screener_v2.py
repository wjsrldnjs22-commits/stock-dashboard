#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Smart Money Screener v3.0 — Wall Street Research Analyst Edition
채점 방식: 절대점수 기반 (백분위 폐기)
섹션별 가중치:
  ① 재무분석     30pt  (매출성장률, EPS성장률, 영업이익률, D/E비율, FCF)
  ② 밸류에이션   25pt  (PER, PS, EV/EBITDA, 업종 대비 비교)
  ③ 기술적분석   20pt  (RSI, MACD, 이평선 배열, 거래량)
  ④ 스마트머니   15pt  (13F 기관축적, 거래량이상, S&P 대비 RS)
  ⑤ 애널리스트   10pt  (컨센서스, 목표가 괴리율)

등급 기준 (절대점수):
  S ≥ 65  : 강력 매수 (구조적 성장 + 밸류 매력)
  A ≥ 55  : 매수
  B ≥ 45  : 중립
  C ≥ 30  : 약한 투자
  D ≥ 15  : 회피
  F  < 15 : 투자 부적합
"""

import os
import pandas as pd
import numpy as np
import yfinance as yf
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 업종별 밸류에이션 기준치 (PER / PS / EV/EBITDA)
# ─────────────────────────────────────────────
SECTOR_VALUATION_BENCHMARKS = {
    'Technology':              {'pe': 28, 'ps': 6.0,  'ev_ebitda': 22},
    'Information Technology':  {'pe': 28, 'ps': 6.0,  'ev_ebitda': 22},
    'Healthcare':              {'pe': 22, 'ps': 4.0,  'ev_ebitda': 16},
    'Health Care':             {'pe': 22, 'ps': 4.0,  'ev_ebitda': 16},
    'Financials':              {'pe': 14, 'ps': 2.5,  'ev_ebitda': 12},
    'Financial Services':      {'pe': 14, 'ps': 2.5,  'ev_ebitda': 12},
    'Consumer Discretionary':  {'pe': 25, 'ps': 1.5,  'ev_ebitda': 14},
    'Consumer Cyclical':       {'pe': 25, 'ps': 1.5,  'ev_ebitda': 14},
    'Consumer Staples':        {'pe': 20, 'ps': 1.0,  'ev_ebitda': 13},
    'Consumer Defensive':      {'pe': 20, 'ps': 1.0,  'ev_ebitda': 13},
    'Energy':                  {'pe': 12, 'ps': 1.0,  'ev_ebitda': 7},
    'Industrials':             {'pe': 20, 'ps': 1.5,  'ev_ebitda': 14},
    'Materials':               {'pe': 16, 'ps': 1.5,  'ev_ebitda': 11},
    'Basic Materials':         {'pe': 16, 'ps': 1.5,  'ev_ebitda': 11},
    'Utilities':               {'pe': 18, 'ps': 2.5,  'ev_ebitda': 12},
    'Real Estate':             {'pe': 30, 'ps': 5.0,  'ev_ebitda': 20},
    'Communication Services':  {'pe': 22, 'ps': 3.5,  'ev_ebitda': 14},
    'DEFAULT':                 {'pe': 20, 'ps': 3.0,  'ev_ebitda': 14},
}


def _sector_benchmark(sector: str) -> dict:
    return SECTOR_VALUATION_BENCHMARKS.get(sector, SECTOR_VALUATION_BENCHMARKS['DEFAULT'])


class WallStreetScreener:
    """
    Wall Street Research Analyst Style Screener v3.0
    - 절대점수 등급 (S/A/B/C/D/F)
    - 7섹션 리포트 생성
    - yfinance 기반 실시간 데이터
    """

    def __init__(self, data_dir: str = '.'):
        self.data_dir = data_dir
        self.output_file = os.path.join(data_dir, 'smart_money_picks_v2.csv')
        self.volume_df = None
        self.holdings_df = None
        self.spy_data = None

    # ══════════════════════════════════════════
    # DATA LOADING
    # ══════════════════════════════════════════
    def load_data(self) -> bool:
        try:
            vol_file = os.path.join(self.data_dir, 'us_volume_analysis.csv')
            if os.path.exists(vol_file):
                self.volume_df = pd.read_csv(vol_file)
                logger.info(f"✅ Volume analysis: {len(self.volume_df)} stocks")
            else:
                logger.warning("⚠️ Volume analysis not found")
                return False

            holdings_file = os.path.join(self.data_dir, 'us_13f_holdings.csv')
            if os.path.exists(holdings_file):
                self.holdings_df = pd.read_csv(holdings_file)
                logger.info(f"✅ 13F holdings: {len(self.holdings_df)} stocks")
            else:
                logger.warning("⚠️ 13F holdings not found")
                return False

            logger.info("📈 Loading SPY benchmark...")
            spy = yf.Ticker("SPY")
            self.spy_data = spy.history(period="3mo")
            return True
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return False

    # ══════════════════════════════════════════
    # ① 재무 분석 — 30pt
    # ══════════════════════════════════════════
    def get_financial_score(self, info: dict) -> Tuple[float, dict]:
        """
        매출성장률(YoY)  : +10pt max
        EPS성장률        : +8pt  max
        영업이익률       : +7pt  max
        D/E 비율         : +5pt  max (낮을수록 좋음)
        FCF / 시총       : +0   (보너스 없음, 감산만)
        총계             : 30pt
        """
        score = 0.0
        details = {}

        # ── 매출 성장률 ──────────────────────────
        rev_growth = info.get('revenueGrowth') or 0.0
        details['revenue_growth_pct'] = round(rev_growth * 100, 1)
        if rev_growth >= 0.30:    score += 10
        elif rev_growth >= 0.20:  score += 8
        elif rev_growth >= 0.10:  score += 6
        elif rev_growth >= 0.05:  score += 4
        elif rev_growth >= 0.0:   score += 2
        else:                     score += max(-5, rev_growth * 20)  # 감산 최대 -5

        # ── EPS 성장률 ───────────────────────────
        eps_growth = info.get('earningsGrowth') or 0.0
        details['eps_growth_pct'] = round(eps_growth * 100, 1)
        if eps_growth >= 0.30:    score += 8
        elif eps_growth >= 0.20:  score += 7
        elif eps_growth >= 0.10:  score += 5
        elif eps_growth >= 0.0:   score += 3
        else:                     score += max(-4, eps_growth * 15)

        # ── 영업이익률 ───────────────────────────
        op_margin = info.get('operatingMargins') or 0.0
        details['operating_margin_pct'] = round(op_margin * 100, 1)
        if op_margin >= 0.30:    score += 7
        elif op_margin >= 0.20:  score += 6
        elif op_margin >= 0.10:  score += 4
        elif op_margin >= 0.0:   score += 2
        else:                    score += max(-3, op_margin * 10)

        # ── D/E 비율 (yfinance는 %로 제공: 150 = 1.5배) ──
        de_raw = info.get('debtToEquity') or 0.0
        de_ratio = de_raw / 100.0 if de_raw > 5 else de_raw  # %→배수 자동 변환
        details['debt_to_equity'] = round(de_ratio, 2)
        if de_ratio <= 0.0:        score += 5
        elif de_ratio <= 0.5:      score += 5
        elif de_ratio <= 1.0:      score += 4
        elif de_ratio <= 2.0:      score += 2
        elif de_ratio <= 3.0:      score += 0
        else:                      score -= 2

        # ── FCF (여유현금흐름이 양수인지) ────────
        fcf = info.get('freeCashflow') or 0
        details['free_cashflow_b'] = round(fcf / 1e9, 2) if fcf else 0
        if fcf < 0:
            score -= 2  # FCF 마이너스 페널티

        score = max(0.0, min(30.0, score))
        details['financial_score'] = round(score, 1)
        return score, details

    # ══════════════════════════════════════════
    # ② 밸류에이션 — 25pt
    # ══════════════════════════════════════════
    def get_valuation_score(self, info: dict) -> Tuple[float, dict]:
        """
        PER vs 업종 평균  : +8pt max
        PS  vs 업종 평균  : +8pt max
        EV/EBITDA vs 업종 : +9pt max
        총계              : 25pt
        """
        score = 0.0
        details = {}
        sector = info.get('sector', 'DEFAULT')
        benchmark = _sector_benchmark(sector)
        details['sector'] = sector

        # ── 고성장 할인: 매출이나 EPS 성장률이 높으면 밴드 확대 ──
        rev_growth = info.get('revenueGrowth') or 0.0
        growth_mult = 1.5 if rev_growth >= 0.20 else (1.2 if rev_growth >= 0.10 else 1.0)
        details['growth_premium_applied'] = growth_mult

        # ── PER ──────────────────────────────────
        pe = info.get('trailingPE') or info.get('forwardPE') or 0.0
        pe = max(0.0, pe)
        details['pe_ratio'] = round(pe, 2) if pe else 'N/A'
        sector_pe = benchmark['pe'] * growth_mult
        if 0 < pe <= sector_pe * 0.6:     score += 8   # 업종 대비 40% 이상 저평가
        elif 0 < pe <= sector_pe * 0.8:   score += 6
        elif 0 < pe <= sector_pe * 1.0:   score += 4   # 업종 평균 이하
        elif 0 < pe <= sector_pe * 1.3:   score += 2
        elif pe > sector_pe * 2.0:        score -= 2   # 업종 대비 2배 이상 고평가
        elif pe <= 0:                      score -= 1   # 적자 or 데이터 없음

        # ── PS ───────────────────────────────────
        ps = info.get('priceToSalesTrailing12Months') or 0.0
        details['ps_ratio'] = round(ps, 2) if ps else 'N/A'
        sector_ps = benchmark['ps'] * growth_mult
        if 0 < ps <= sector_ps * 0.5:     score += 8
        elif 0 < ps <= sector_ps * 0.8:   score += 6
        elif 0 < ps <= sector_ps * 1.0:   score += 4
        elif 0 < ps <= sector_ps * 1.5:   score += 2
        elif ps > sector_ps * 2.5:        score -= 2
        elif ps <= 0:                      score -= 0

        # ── EV/EBITDA ────────────────────────────
        ev = info.get('enterpriseValue') or 0
        ebitda = info.get('ebitda') or 0
        ev_ebitda = (ev / ebitda) if ebitda and ebitda > 0 else 0.0
        details['ev_ebitda'] = round(ev_ebitda, 2) if ev_ebitda else 'N/A'
        sector_ev = benchmark['ev_ebitda'] * growth_mult
        if 0 < ev_ebitda <= sector_ev * 0.6:   score += 9
        elif 0 < ev_ebitda <= sector_ev * 0.8:  score += 7
        elif 0 < ev_ebitda <= sector_ev * 1.0:  score += 5
        elif 0 < ev_ebitda <= sector_ev * 1.3:  score += 2
        elif ev_ebitda > sector_ev * 2.0:       score -= 2
        elif ev_ebitda <= 0:                    score += 0

        # -- 고성장 할인: 매출 성장률 20%+ 이면 PER/PS 기준을 1.5x까지 허용 --
        rev_growth = info.get('revenueGrowth') or 0.0
        growth_mult = 1.5 if rev_growth >= 0.20 else (1.3 if rev_growth >= 0.10 else 1.0)

        score = max(0.0, min(25.0, score))
        details['valuation_score'] = round(score, 1)
        details['growth_premium'] = growth_mult
        return score, details

    # ══════════════════════════════════════════
    # ③ 기술적 분석 — 20pt
    # ══════════════════════════════════════════
    def get_technical_score(self, ticker: str) -> Tuple[float, dict]:
        """
        RSI 상태          : +5pt max
        MACD 크로스       : +7pt max
        이평선 배열       : +5pt max
        Golden/Death Cross: +3pt max (보너스/페널티)
        총계              : 20pt
        """
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="6mo")
            if len(hist) < 50:
                return 10.0, self._default_technical()

            close = hist['Close']
            volume = hist['Volume']

            # RSI
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = (100 - (100 / (1 + rs))).iloc[-1]

            # MACD
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd = ema12 - ema26
            signal = macd.ewm(span=9, adjust=False).mean()
            macd_hist = (macd - signal).iloc[-1]
            macd_prev = (macd - signal).iloc[-2]
            macd_val = macd.iloc[-1]

            # 이평선
            ma20 = close.rolling(20).mean().iloc[-1]
            ma50 = close.rolling(50).mean().iloc[-1]
            ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else ma50
            price = close.iloc[-1]

            # Golden/Death Cross
            ma50_prev = close.rolling(50).mean().iloc[-5] if len(close) >= 55 else ma50
            ma200_prev = close.rolling(200).mean().iloc[-5] if len(close) >= 205 else ma200

            score = 0.0

            # RSI contribution (5pt)
            if 40 <= rsi <= 60:    score += 4
            elif rsi < 30:         score += 5   # oversold bounce potential
            elif rsi < 40:         score += 3
            elif rsi > 80:         score -= 1
            elif rsi > 70:         score += 2   # still trending, minor risk

            # MACD (7pt)
            if macd_hist > 0 and macd_prev <= 0:   score += 7  # 상승 크로스
            elif macd_hist > 0 and macd_val > 0:   score += 5  # 상승 구간 유지
            elif macd_hist > 0:                     score += 3
            elif macd_hist < 0 and macd_prev >= 0: score -= 3  # 하락 크로스
            elif macd_hist < 0:                     score -= 1

            # MA 배열 (5pt)
            if price > ma20 > ma50 > ma200:   score += 5   # 완전 정배열
            elif price > ma20 > ma50:          score += 3
            elif price > ma50:                 score += 1
            elif price < ma20 < ma50:          score -= 2   # 역배열

            # Golden/Death Cross 보너스 (3pt)
            if ma50 > ma200 and ma50_prev <= ma200_prev:   score += 3
            elif ma50 < ma200 and ma50_prev >= ma200_prev: score -= 3

            score = max(0.0, min(20.0, score))
            details = {
                'rsi': round(rsi, 1),
                'macd_histogram': round(macd_hist, 4),
                'ma20': round(ma20, 2),
                'ma50': round(ma50, 2),
                'ma200': round(ma200, 2),
                'price': round(price, 2),
                'ma_signal': 'Bullish' if price > ma20 > ma50 else ('Bearish' if price < ma20 < ma50 else 'Neutral'),
                'cross_signal': 'Golden Cross' if (ma50 > ma200 and ma50_prev <= ma200_prev) else
                                ('Death Cross' if (ma50 < ma200 and ma50_prev >= ma200_prev) else 'None'),
                'technical_score': round(score, 1)
            }
            return score, details
        except Exception:
            return 10.0, self._default_technical()

    def _default_technical(self) -> dict:
        return {
            'rsi': 50, 'macd_histogram': 0, 'ma20': 0, 'ma50': 0, 'ma200': 0,
            'price': 0, 'ma_signal': 'Unknown', 'cross_signal': 'None', 'technical_score': 10
        }

    # ══════════════════════════════════════════
    # ④ 스마트머니 / RS — 15pt
    # ══════════════════════════════════════════
    def get_smart_money_score(self, row: pd.Series, ticker: str) -> Tuple[float, dict]:
        """
        공급/수요(거래량) : +7pt max
        13F 기관 축적     : +5pt max
        S&P 대비 RS(20d)  : +3pt max
        총계              : 15pt
        """
        score = 0.0
        details = {}

        # 공급/수요 점수 (0~100 → 0~7점 선형 변환)
        sd_score = float(row.get('supply_demand_score', 50) or 50)
        sd_pts = round((sd_score - 50) / 50 * 7, 2)  # 50이면 0, 100이면 +7, 0이면 -7
        sd_pts = max(-3.0, min(7.0, sd_pts))
        score += sd_pts
        details['supply_demand_score'] = sd_score

        # 13F 기관 축적 (0~100 → 0~5점)
        inst_score = float(row.get('institutional_score', 50) or 50)
        inst_pts = round((inst_score - 50) / 50 * 5, 2)
        inst_pts = max(-2.0, min(5.0, inst_pts))
        score += inst_pts
        details['institutional_score'] = inst_score

        # 상대강도 vs S&P 500
        try:
            if self.spy_data is not None and len(self.spy_data) >= 21:
                stock_hist = yf.Ticker(ticker).history(period="3mo")
                if len(stock_hist) >= 21:
                    stock_ret = (stock_hist['Close'].iloc[-1] / stock_hist['Close'].iloc[-21] - 1) * 100
                    spy_ret = (self.spy_data['Close'].iloc[-1] / self.spy_data['Close'].iloc[-21] - 1) * 100
                    rs_20d = stock_ret - spy_ret
                    details['rs_vs_spy_20d'] = round(rs_20d, 1)
                    if rs_20d > 10:    score += 3
                    elif rs_20d > 5:   score += 2
                    elif rs_20d > 0:   score += 1
                    elif rs_20d < -10: score -= 2
                    elif rs_20d < -5:  score -= 1
                else:
                    details['rs_vs_spy_20d'] = 0
            else:
                details['rs_vs_spy_20d'] = 0
        except Exception:
            details['rs_vs_spy_20d'] = 0

        score = max(0.0, min(15.0, score))
        details['smart_money_score'] = round(score, 1)
        return score, details

    # ══════════════════════════════════════════
    # ⑤ 애널리스트 컨센서스 — 10pt
    # ══════════════════════════════════════════
    def get_analyst_score(self, info: dict) -> Tuple[float, dict]:
        """
        컨센서스 추천     : +5pt max
        목표가 대비 상승여력: +5pt max
        총계              : 10pt
        """
        score = 0.0
        details = {}

        current_price = info.get('currentPrice') or info.get('regularMarketPrice') or 0
        target_price = info.get('targetMeanPrice') or 0
        recommendation = info.get('recommendationKey', 'none') or 'none'
        num_analysts = info.get('numberOfAnalystOpinions') or 0

        upside = ((target_price / current_price) - 1) * 100 if current_price > 0 and target_price > 0 else 0

        details['current_price'] = round(current_price, 2)
        details['target_price'] = round(target_price, 2) if target_price else 'N/A'
        details['upside_pct'] = round(upside, 1)
        details['recommendation'] = recommendation
        details['num_analysts'] = num_analysts
        details['company_name'] = info.get('longName') or info.get('shortName') or ''

        # 추천 점수 (5pt)
        rec_map = {'strongBuy': 5, 'buy': 4, 'hold': 2, 'underperform': 0, 'sell': -1, 'strongSell': -2}
        score += rec_map.get(recommendation, 1)

        # 목표가 상승여력 (5pt)
        if upside >= 40:    score += 5
        elif upside >= 25:  score += 4
        elif upside >= 15:  score += 3
        elif upside >= 5:   score += 2
        elif upside > 0:    score += 1
        elif upside < -15:  score -= 2
        elif upside < -5:   score -= 1

        score = max(0.0, min(10.0, score))
        details['analyst_score'] = round(score, 1)
        return score, details

    # ══════════════════════════════════════════
    # 최종 등급 계산
    # ══════════════════════════════════════════
    @staticmethod
    def score_to_grade(total: float) -> Tuple[str, str]:
        """절대점수 → 등급 변환 (현실에 맞는 우상향 커브)"""
        if total >= 65:   return 'S', '🔥 S급 (강력 매수)'
        elif total >= 55: return 'A', '🌟 A급 (매수)'
        elif total >= 45: return 'B', '📈 B급 (중립)'
        elif total >= 35: return 'C', '📊 C급 (약한 투자)'
        elif total >= 25: return 'D', '⚠️ D급 (회피)'
        else:             return 'F', '🚫 F급 (투자 부적합)'

    # ══════════════════════════════════════════
    # 7섹션 리포트 딕셔너리 생성
    # ══════════════════════════════════════════
    def build_report_dict(self, ticker: str, info: dict, row: pd.Series,
                          fin: dict, val: dict, tech: dict, sm: dict, analyst: dict,
                          total_score: float, letter: str, grade_label: str) -> dict:
        """
        flask_app.py의 /api/stock_detail/<ticker> 에서 사용할 7섹션 리포트
        """
        company = analyst.get('company_name', ticker)
        sector = val.get('sector', 'Unknown')

        report = {
            'ticker': ticker,
            'company_name': company,
            'total_score': total_score,
            'letter_grade': letter,
            'grade_label': grade_label,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),

            # ① 기업 개요
            'section1_overview': {
                'business_summary': info.get('longBusinessSummary', 'N/A')[:500] if info.get('longBusinessSummary') else 'N/A',
                'sector': sector,
                'industry': info.get('industry', 'N/A'),
                'employees': info.get('fullTimeEmployees', 'N/A'),
                'country': info.get('country', 'N/A'),
                'market_cap_b': round((info.get('marketCap') or 0) / 1e9, 1),
                'website': info.get('website', ''),
            },

            # ② 재무 분석
            'section2_financials': {
                'revenue_growth_pct': fin.get('revenue_growth_pct', 0),
                'eps_growth_pct': fin.get('eps_growth_pct', 0),
                'operating_margin_pct': fin.get('operating_margin_pct', 0),
                'profit_margin_pct': round((info.get('profitMargins') or 0) * 100, 1),
                'debt_to_equity': fin.get('debt_to_equity', 0),
                'free_cashflow_b': fin.get('free_cashflow_b', 0),
                'roe_pct': round((info.get('returnOnEquity') or 0) * 100, 1),
                'current_ratio': round(info.get('currentRatio') or 0, 2),
                'score': fin.get('financial_score', 0),
                'max_score': 30,
            },

            # ③ 밸류에이션
            'section3_valuation': {
                'pe_ratio': val.get('pe_ratio', 'N/A'),
                'forward_pe': round(info.get('forwardPE') or 0, 2) or 'N/A',
                'ps_ratio': val.get('ps_ratio', 'N/A'),
                'pb_ratio': round(info.get('priceToBook') or 0, 2) or 'N/A',
                'ev_ebitda': val.get('ev_ebitda', 'N/A'),
                'sector': sector,
                'sector_pe_benchmark': _sector_benchmark(sector)['pe'],
                'sector_ps_benchmark': _sector_benchmark(sector)['ps'],
                'sector_ev_ebitda_benchmark': _sector_benchmark(sector)['ev_ebitda'],
                'score': val.get('valuation_score', 0),
                'max_score': 25,
            },

            # ④ 최근 뉴스 & 투자 논리 변화 (ai_summary_generator가 채움)
            'section4_news_catalyst': {
                'news_items': [],   # NewsCollector 결과가 여기 들어옴
                'ai_catalyst_summary': '',  # GeminiGenerator가 채움
                'score': None,
            },

            # ⑤ 리스크 분석
            'section5_risks': {
                'beta': round(info.get('beta') or 1.0, 2),
                'short_percent': round((info.get('shortPercentOfFloat') or 0) * 100, 1),
                'insider_percent': round((info.get('heldPercentInsiders') or 0) * 100, 1),
                'institution_percent': round((info.get('heldPercentInstitutions') or 0) * 100, 1),
                'de_ratio': fin.get('debt_to_equity', 0),
                'fcf_status': 'Positive' if fin.get('free_cashflow_b', 0) > 0 else 'Negative',
                '52w_high': round(info.get('fiftyTwoWeekHigh') or 0, 2),
                '52w_low': round(info.get('fiftyTwoWeekLow') or 0, 2),
                'current_price': analyst.get('current_price', 0),
                'pct_off_high': round(
                    (analyst.get('current_price', 0) / info.get('fiftyTwoWeekHigh', 1) - 1) * 100, 1
                ) if info.get('fiftyTwoWeekHigh') else 'N/A',
            },

            # ⑥ 투자 시나리오 (AI가 생성, 초기값은 수식 기반)
            'section6_scenarios': {
                'bull_case': {
                    'target': round(analyst.get('current_price', 0) * 1.30, 2),
                    'description': '매출 가속화 + 마진 개선 + 밸류 리레이팅',
                },
                'base_case': {
                    'target': round(analyst.get('target_price', 0) or analyst.get('current_price', 0) * 1.12, 2),
                    'description': '컨센서스 목표가 기준',
                },
                'bear_case': {
                    'target': round(analyst.get('current_price', 0) * 0.80, 2),
                    'description': '매크로 역풍 + 실적 미스',
                },
            },

            # ⑦ 종합 판단
            'section7_verdict': {
                'total_score': total_score,
                'score_breakdown': {
                    'financial': fin.get('financial_score', 0),
                    'valuation': val.get('valuation_score', 0),
                    'technical': tech.get('technical_score', 0),
                    'smart_money': sm.get('smart_money_score', 0),
                    'analyst': analyst.get('analyst_score', 0),
                },
                'letter_grade': letter,
                'grade_label': grade_label,
                'buy_price': tech.get('ma50', 0) if tech.get('ma50', 0) > 0 else round(analyst.get('current_price', 0) * 0.97, 2),
                'sell_price': analyst.get('target_price', 0) if analyst.get('target_price', 0) and analyst.get('target_price', 0) != 'N/A'
                              else round(analyst.get('current_price', 0) * 1.15, 2),
                'target_upside_pct': analyst.get('upside_pct', 0),
                'rs_vs_spy_20d': sm.get('rs_vs_spy_20d', 0),
                'technical_signal': tech.get('ma_signal', 'Unknown'),
                'rsi': tech.get('rsi', 50),
            },
        }
        return report

    # ══════════════════════════════════════════
    # 메인 스크리닝
    # ══════════════════════════════════════════
    def analyze_ticker(self, ticker: str, row: pd.Series) -> Optional[dict]:
        """단일 종목 전체 분석"""
        try:
            info = yf.Ticker(ticker).info or {}
        except Exception:
            info = {}

        # ① 재무
        fin_score, fin_detail = self.get_financial_score(info)
        # ② 밸류에이션
        val_score, val_detail = self.get_valuation_score(info)
        # ③ 기술적
        tech_score, tech_detail = self.get_technical_score(ticker)
        # ④ 스마트머니
        sm_score, sm_detail = self.get_smart_money_score(row, ticker)
        # ⑤ 애널리스트
        analyst_score, analyst_detail = self.get_analyst_score(info)

        total_score = round(fin_score + val_score + tech_score + sm_score + analyst_score, 1)
        letter, grade_label = self.score_to_grade(total_score)

        # 7섹션 리포트 생성
        report = self.build_report_dict(
            ticker, info, row,
            fin_detail, val_detail, tech_detail, sm_detail, analyst_detail,
            total_score, letter, grade_label
        )

        # CSV용 결과 (플랫)
        current_price = analyst_detail.get('current_price', 0) or 0
        target_price = analyst_detail.get('target_price', 'N/A')
        ma50 = tech_detail.get('ma50', 0) or 0

        buy_price = ma50 if 0 < ma50 < current_price * 1.05 else current_price * 0.97
        sell_price = (target_price if isinstance(target_price, (int, float)) and target_price > current_price
                      else current_price * 1.15)

        csv_row = {
            'ticker': ticker,
            'name': analyst_detail.get('company_name', ticker),
            'composite_score': total_score,
            'grade': grade_label,
            'letter_grade': letter,
            'financial_score': round(fin_score, 1),
            'valuation_score': round(val_score, 1),
            'tech_score': round(tech_score, 1),
            'smart_money_score': round(sm_score, 1),
            'analyst_score': round(analyst_score, 1),
            # 세부 지표
            'revenue_growth_pct': fin_detail.get('revenue_growth_pct', 0),
            'eps_growth_pct': fin_detail.get('eps_growth_pct', 0),
            'operating_margin_pct': fin_detail.get('operating_margin_pct', 0),
            'debt_to_equity': fin_detail.get('debt_to_equity', 0),
            'pe_ratio': val_detail.get('pe_ratio', 'N/A'),
            'ps_ratio': val_detail.get('ps_ratio', 'N/A'),
            'ev_ebitda': val_detail.get('ev_ebitda', 'N/A'),
            'sector': val_detail.get('sector', ''),
            'rsi': tech_detail.get('rsi', 50),
            'ma_signal': tech_detail.get('ma_signal', 'Unknown'),
            'rs_vs_spy_20d': sm_detail.get('rs_vs_spy_20d', 0),
            'upside_pct': analyst_detail.get('upside_pct', 0),
            'recommendation': analyst_detail.get('recommendation', 'none'),
            'current_price': current_price,
            'target_buy_price': round(buy_price, 2),
            'target_sell_price': round(sell_price, 2),
            'report_json': report,   # 전체 리포트 (flask_app에서 사용)
        }
        return csv_row

    def run_screening(self) -> pd.DataFrame:
        logger.info("🔍 Running Wall Street Screener v3.0 (Absolute Score System)...")

        merged_df = pd.merge(
            self.volume_df,
            self.holdings_df,
            on='ticker',
            how='left',
            suffixes=('_vol', '_inst')
        )
        merged_df['institutional_score'] = merged_df['institutional_score'].fillna(50)
        merged_df['supply_demand_score'] = merged_df['supply_demand_score'].fillna(50)

        logger.info(f"📊 Processing {len(merged_df)} candidates...")
        results = []

        for _, row in tqdm(merged_df.iterrows(), total=len(merged_df), desc="WS Screening"):
            ticker = row['ticker']
            try:
                result = self.analyze_ticker(ticker, row)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning(f"⚠️ {ticker}: {e}")

        if not results:
            return pd.DataFrame()

        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('composite_score', ascending=False)
        results_df['rank'] = range(1, len(results_df) + 1)

        # 등급 분포 로깅
        grade_dist = results_df['letter_grade'].value_counts()
        logger.info(f"📊 Grade Distribution: {grade_dist.to_dict()}")

        return results_df

    def run(self) -> pd.DataFrame:
        logger.info("🚀 Starting Wall Street Screener v3.0...")
        if not self.load_data():
            logger.error("❌ Failed to load data")
            return pd.DataFrame()

        results_df = self.run_screening()

        if not results_df.empty:
            # report_json 컬럼은 별도 JSON 파일로 저장 (CSV에는 제외)
            report_data = {}
            if 'report_json' in results_df.columns:
                for _, row in results_df.iterrows():
                    report_data[row['ticker']] = row['report_json']
                results_df = results_df.drop(columns=['report_json'])

            results_df.to_csv(self.output_file, index=False)
            logger.info(f"✅ Saved CSV: {self.output_file}")

            # 리포트 JSON 저장
            report_json_path = os.path.join(self.data_dir, 'stock_reports.json')
            import json
            with open(report_json_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"✅ Saved Reports: {report_json_path}")

        return results_df


# ──────────────────────────────────────────────
# 하위 호환: 기존 코드가 EnhancedSmartMoneyScreener를 import하는 경우 대비
# ──────────────────────────────────────────────
EnhancedSmartMoneyScreener = WallStreetScreener


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Wall Street Screener v3.0')
    parser.add_argument('--dir', default='.')
    args = parser.parse_args()

    screener = WallStreetScreener(data_dir=args.dir)
    results = screener.run()

    if not results.empty:
        print(f"\n{'='*70}")
        print(f"{'Rank':<5} {'Ticker':<8} {'Grade':<5} {'Score':>6} {'Rev%':>6} {'PE':>6} {'RSI':>5} {'Upside':>7}")
        print(f"{'='*70}")
        for _, row in results.head(30).iterrows():
            print(
                f"{int(row['rank']):<5} {row['ticker']:<8} {row['letter_grade']:<5} "
                f"{row['composite_score']:>6.1f} {row.get('revenue_growth_pct', 0):>5.1f}% "
                f"{str(row.get('pe_ratio', 'N/A')):>6} {row.get('rsi', 50):>5.1f} "
                f"{row.get('upside_pct', 0):>6.1f}%"
            )
        print(f"{'='*70}")

        grade_dist = results['letter_grade'].value_counts()
        print(f"\n📊 Grade Distribution:")
        for g in ['S', 'A', 'B', 'C', 'D', 'F']:
            cnt = grade_dist.get(g, 0)
            bar = '█' * cnt
            print(f"  {g}: {cnt:>3}  {bar}")


if __name__ == "__main__":
    main()
