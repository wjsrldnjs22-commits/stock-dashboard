#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Stock Summary Generator v2.0
Wall Street Analyst 7-Section Report 형식으로 Gemini 호출.
- 종목 뉴스 수집 (Google News RSS)
- 실시간 yfinance 데이터 기반 프롬프트 생성
- 한국어/영어 순차 생성 (Rate-limit 안전)
- stock_reports.json 캐시와 연동
"""

import os
import json
import logging
import time
import requests
import yfinance as yf
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from typing import Dict, List, Optional

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
class NewsCollector:
    """Google News RSS에서 종목 최신 뉴스 수집"""

    def get_news(self, ticker: str, max_items: int = 5) -> List[dict]:
        news = []
        try:
            import xml.etree.ElementTree as ET
            url = (
                f"https://news.google.com/rss/search"
                f"?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
            )
            resp = requests.get(url, timeout=6)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                for item in root.findall('.//item')[:max_items]:
                    title_el = item.find('title')
                    date_el  = item.find('pubDate')
                    link_el  = item.find('link')
                    if title_el is not None:
                        news.append({
                            'title':     title_el.text or '',
                            'published': date_el.text  if date_el  is not None else '',
                            'link':      link_el.text  if link_el  is not None else '',
                        })
        except Exception as e:
            logger.warning(f"⚠️ News fetch error for {ticker}: {e}")
        return news


# ─────────────────────────────────────────────────────────────────────────────
class GeminiGenerator:
    """Gemini Flash를 통해 Wall Street 7섹션 리포트 생성"""

    def __init__(self):
        self.key = os.getenv('GOOGLE_API_KEY', '')
        self.url = (
            "https://generativelanguage.googleapis.com/v1beta"
            "/models/gemini-2.5-flash:generateContent"
        )

    def _build_prompt(self, ticker: str, data: dict, news: List[dict], lang: str) -> str:
        """7섹션 리포트 프롬프트 생성"""
        news_block = (
            "\n".join(f"- {n['title']}" for n in news)
            if news else "최근 뉴스 없음"
        )

        # 점수/등급 정보 (screener 결과가 있으면 포함)
        score_line = ''
        if data.get('total_score') is not None:
            score_line = (
                f"\n[ 정량 스코어: {data['total_score']}/100 | "
                f"등급: {data.get('letter_grade','?')} | "
                f"재무점수: {data.get('financial_score','?')}/30 | "
                f"밸류점수: {data.get('valuation_score','?')}/25 | "
                f"기술점수: {data.get('tech_score','?')}/20 ]\n"
            )

        # 핵심 재무 지표 블록
        fin_block = ''
        if data.get('revenue_growth_pct') is not None:
            fin_block = (
                f"\n[ 핵심 지표 ]\n"
                f"  매출성장률: {data.get('revenue_growth_pct', 'N/A')}%\n"
                f"  EPS성장률:  {data.get('eps_growth_pct', 'N/A')}%\n"
                f"  영업이익률: {data.get('operating_margin_pct', 'N/A')}%\n"
                f"  D/E 비율:   {data.get('debt_to_equity', 'N/A')}\n"
                f"  PER:        {data.get('pe_ratio', 'N/A')}\n"
                f"  PS 비율:    {data.get('ps_ratio', 'N/A')}\n"
                f"  EV/EBITDA:  {data.get('ev_ebitda', 'N/A')}\n"
                f"  RSI:        {data.get('rsi', 'N/A')}\n"
                f"  현재가:     ${data.get('current_price', 'N/A')}\n"
                f"  목표가:     ${data.get('target_price', 'N/A')} "
                f"(상승여력: {data.get('upside_pct', 'N/A')}%)\n"
            )

        if lang == 'ko':
            return f"""너는 월가 리서치 애널리스트다.
감정적 설명이나 비유를 사용하지 말고 데이터 기반 투자 리포트 형식으로 작성하라.
{score_line}
종목: {ticker}
최근 뉴스:
{news_block}
{fin_block}
다음 순서로 분석하라 (각 섹션 제목을 ## 헤더로 표시):

## 1. 기업 개요
- 사업모델 (주요 제품/서비스)
- 주요 수익원
- 시장 위치 (경쟁 환경 포함)

## 2. 재무 분석
- 매출 성장률 (YoY) 및 트렌드
- EPS 성장률 및 어닝 서프라이즈
- 영업이익률 (업계 대비)
- 부채 수준 (D/E 비율, 이자보상배율)
- 잉여현금흐름 (FCF) 상태

## 3. 밸류에이션
- PER (동종업계 평균 대비)
- PS 비율
- EV/EBITDA
- 동종업계 비교 종합 판단 (저평가/적정/고평가)

## 4. 최근 뉴스 및 투자 논리 변화
- 규제 리스크/변화
- 파트너십·인수합병 동향
- 최근 실적 변화 포인트
- 자금조달·자사주매입 동향

## 5. 리스크 분석
- 산업 리스크 (시장구조, 경쟁)
- 재무 리스크 (부채, 현금흐름)
- 정책/규제 리스크

## 6. 투자 시나리오
- Bull Case: (긍정적 시나리오 + 목표주가)
- Base Case: (기본 시나리오 + 목표주가)
- Bear Case: (부정적 시나리오 + 목표주가)

## 7. 종합 판단
(이 섹션에만 최종 등급을 표시하라)

등급:
S = 강력한 매수 (구조적 성장 + 밸류 매력)
A = 매수
B = 중립
C = 약한 투자
D = 회피
F = 투자 부적합

**최종 등급: [S/A/B/C/D/F 중 하나]**
근거: (2~3문장 핵심 요약)
"""
        else:
            return f"""You are a Wall Street Research Analyst.
Write a data-driven investment report without emotional language or metaphors.
{score_line}
Ticker: {ticker}
Recent News:
{news_block}
{fin_block}
Analyze in the following order (use ## headers for each section):

## 1. Company Overview
- Business model (key products/services)
- Primary revenue streams
- Market position (competitive landscape)

## 2. Financial Analysis
- Revenue growth (YoY) and trend
- EPS growth and earnings surprises
- Operating margin (vs. industry)
- Debt levels (D/E ratio, interest coverage)
- Free cash flow status

## 3. Valuation
- P/E ratio (vs. sector average)
- P/S ratio
- EV/EBITDA
- Peer comparison (undervalued / fair / overvalued)

## 4. Recent News & Thesis Changes
- Regulatory risks/changes
- M&A and partnership developments
- Key earnings changes
- Capital allocation (buybacks, funding)

## 5. Risk Analysis
- Industry risks (competition, structure)
- Financial risks (debt, cash flow)
- Policy/regulatory risks

## 6. Investment Scenarios
- Bull Case: (scenario + price target)
- Base Case: (scenario + price target)
- Bear Case: (scenario + price target)

## 7. Final Verdict
(Display the final rating ONLY in this section)

Rating Scale:
S = Strong Buy (structural growth + value)
A = Buy
B = Hold
C = Weak Hold
D = Avoid
F = Uninvestable

**Final Rating: [S/A/B/C/D/F]**
Rationale: (2-3 sentence summary)
"""

    def generate(self, ticker: str, data: dict, news: List[dict], lang: str = 'ko') -> str:
        if not self.key:
            return "No API Key configured"

        prompt = self._build_prompt(ticker, data, news, lang)

        for attempt in range(3):
            try:
                payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": 0.2,       # 분석 리포트는 낮은 온도
                        "maxOutputTokens": 2048,
                    }
                }
                resp = requests.post(
                    f"{self.url}?key={self.key}",
                    json=payload,
                    timeout=60
                )
                if resp.status_code == 200:
                    result = resp.json()
                    candidates = result.get('candidates', [])
                    if candidates:
                        parts = candidates[0].get('content', {}).get('parts', [])
                        if parts:
                            return parts[0].get('text', 'Empty response')
                    return "Analysis Failed: empty candidates"
                elif resp.status_code == 429:
                    wait_sec = 10 * (attempt + 1)
                    logger.warning(f"⏳ Rate limited for {ticker} ({lang}), waiting {wait_sec}s...")
                    time.sleep(wait_sec)
                    continue
                elif resp.status_code in (500, 503):
                    logger.warning(f"⚠️ Server error {resp.status_code} for {ticker}, retrying...")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"❌ API {resp.status_code} for {ticker}: {resp.text[:200]}")
                    return f"Analysis Failed: HTTP {resp.status_code}"
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Timeout for {ticker} ({lang}) attempt {attempt+1}")
                time.sleep(3)
            except Exception as e:
                logger.error(f"❌ Unexpected error for {ticker}: {e}")
                break

        return "Analysis Failed: max retries exceeded"


# ─────────────────────────────────────────────────────────────────────────────
class AIStockAnalyzer:
    """
    스크리너 결과 CSV를 읽어 상위 종목 AI 요약 생성.
    - stock_reports.json의 정량 데이터를 프롬프트에 주입
    - 기존 ai_summaries.json 보완 (실패/미생성 항목만 재실행)
    """

    def __init__(self, data_dir: str = '.'):
        self.data_dir = data_dir
        self.output = os.path.join(data_dir, 'ai_summaries.json')
        self.gen = GeminiGenerator()
        self.news = NewsCollector()

    def _load_report_data(self, ticker: str) -> dict:
        """stock_reports.json에서 정량 데이터 로드"""
        path = os.path.join(self.data_dir, 'stock_reports.json')
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    all_reports = json.load(f)
                report = all_reports.get(ticker, {})
                if report:
                    # 플랫 딕셔너리로 변환 (프롬프트 주입용)
                    s2 = report.get('section2_financials', {})
                    s3 = report.get('section3_valuation', {})
                    s7 = report.get('section7_verdict', {})
                    return {
                        'total_score': report.get('total_score'),
                        'letter_grade': report.get('letter_grade'),
                        'financial_score': s7.get('score_breakdown', {}).get('financial'),
                        'valuation_score': s7.get('score_breakdown', {}).get('valuation'),
                        'tech_score': s7.get('score_breakdown', {}).get('technical'),
                        'revenue_growth_pct': s2.get('revenue_growth_pct'),
                        'eps_growth_pct': s2.get('eps_growth_pct'),
                        'operating_margin_pct': s2.get('operating_margin_pct'),
                        'debt_to_equity': s2.get('debt_to_equity'),
                        'pe_ratio': s3.get('pe_ratio'),
                        'ps_ratio': s3.get('ps_ratio'),
                        'ev_ebitda': s3.get('ev_ebitda'),
                        'rsi': s7.get('rsi'),
                        'current_price': s7.get('score_breakdown') and report.get('section5_risks', {}).get('current_price'),
                        'target_price': s7.get('sell_price'),
                        'upside_pct': s7.get('target_upside_pct'),
                    }
            except Exception as e:
                logger.warning(f"⚠️ report data load error for {ticker}: {e}")
        return {}

    def _should_regenerate(self, ticker: str, existing: dict) -> bool:
        """재생성이 필요한지 판단"""
        summary = existing.get('summary', '')
        if not summary:
            return True
        if summary in ('Analysis Failed', 'No API Key configured', 'Analysis Failed: max retries exceeded'):
            return True
        if 'Analysis Failed' in summary:
            return True
        # 7일 이상 지난 경우 재생성
        import datetime
        updated = existing.get('updated', '')
        if updated:
            try:
                updated_dt = datetime.datetime.fromisoformat(updated)
                age_days = (datetime.datetime.now() - updated_dt).days
                if age_days >= 7:
                    return True
            except Exception:
                pass
        return False

    def run(self, top_n: int = 20, force: bool = False):
        """상위 종목 AI 요약 생성/갱신"""
        csv_path = os.path.join(self.data_dir, 'smart_money_picks_v2.csv')
        if not os.path.exists(csv_path):
            logger.error(f"❌ {csv_path} not found. Run screener first.")
            return

        df = pd.read_csv(csv_path).head(top_n)
        logger.info(f"📋 Processing {len(df)} tickers for AI summary...")

        # 기존 결과 로드
        results: dict = {}
        if os.path.exists(self.output):
            try:
                with open(self.output, encoding='utf-8') as f:
                    results = json.load(f)
            except Exception:
                results = {}

        processed = 0
        for _, row in tqdm(df.iterrows(), total=len(df), desc="AI Summary"):
            ticker = row['ticker']
            existing = results.get(ticker, {})

            if not force and not self._should_regenerate(ticker, existing):
                continue

            logger.info(f"🤖 Generating summary for {ticker}...")

            # 정량 데이터 준비 (stock_reports.json 우선, CSV fallback)
            report_data = self._load_report_data(ticker)
            if not report_data:
                report_data = {
                    'total_score': row.get('composite_score'),
                    'letter_grade': row.get('letter_grade', row.get('grade', '')),
                    'revenue_growth_pct': row.get('revenue_growth_pct'),
                    'eps_growth_pct': row.get('eps_growth_pct'),
                    'operating_margin_pct': row.get('operating_margin_pct'),
                    'debt_to_equity': row.get('debt_to_equity'),
                    'pe_ratio': row.get('pe_ratio'),
                    'ps_ratio': row.get('ps_ratio'),
                    'ev_ebitda': row.get('ev_ebitda'),
                    'rsi': row.get('rsi'),
                    'current_price': row.get('current_price'),
                    'upside_pct': row.get('upside_pct') or row.get('target_upside'),
                }

            # 뉴스 수집
            news = self.news.get_news(ticker)

            # 한국어 생성
            summary_ko = self.gen.generate(ticker, report_data, news, lang='ko')
            time.sleep(2)  # Rate limit 방지

            # 영어 생성
            summary_en = self.gen.generate(ticker, report_data, news, lang='en')
            time.sleep(2)

            import datetime
            results[ticker] = {
                'summary':    summary_ko,   # 하위 호환용 (기본=한국어)
                'summary_ko': summary_ko,
                'summary_en': summary_en,
                'news_count': len(news),
                'score': report_data.get('total_score'),
                'grade': report_data.get('letter_grade'),
                'updated': datetime.datetime.now().isoformat(),
            }
            processed += 1

            # 중간 저장 (5건마다)
            if processed % 5 == 0:
                with open(self.output, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                logger.info(f"💾 Intermediate save: {processed} processed")

        # 최종 저장
        with open(self.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ Saved {len(results)} summaries → {self.output}")
        logger.info(f"✅ Generated/updated: {processed} summaries")

    def generate_single(self, ticker: str) -> dict:
        """단일 종목 즉시 생성 (flask /api/stock_detail/refresh 에서 호출)"""
        ticker = ticker.upper().strip()
        report_data = self._load_report_data(ticker)
        news = self.news.get_news(ticker)

        summary_ko = self.gen.generate(ticker, report_data, news, lang='ko')
        time.sleep(1)
        summary_en = self.gen.generate(ticker, report_data, news, lang='en')

        import datetime
        result = {
            'summary': summary_ko,
            'summary_ko': summary_ko,
            'summary_en': summary_en,
            'news_count': len(news),
            'updated': datetime.datetime.now().isoformat(),
        }

        # 저장
        existing = {}
        if os.path.exists(self.output):
            try:
                with open(self.output, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
            except Exception:
                pass
        existing[ticker] = result
        with open(self.output, 'w', encoding='utf-8') as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)

        return result


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AI Stock Summary Generator v2.0")
    parser.add_argument('--top', type=int, default=20, help='Top N tickers from screener CSV')
    parser.add_argument('--force', action='store_true', help='Force regenerate all summaries')
    parser.add_argument('--ticker', type=str, default='', help='Single ticker to regenerate')
    args = parser.parse_args()

    analyzer = AIStockAnalyzer(data_dir='.')

    if args.ticker:
        logger.info(f"🎯 Single-ticker mode: {args.ticker.upper()}")
        result = analyzer.generate_single(args.ticker)
        print(f"\n{'='*60}")
        print(f"[KO]\n{result['summary_ko'][:800]}...")
        print(f"\n[EN]\n{result['summary_en'][:400]}...")
    else:
        analyzer.run(top_n=args.top, force=args.force)
