"""
daily_collector.py — 매일 자동 수집 스케줄러

APScheduler로 매일 KST 자정+5분에 실행:
1. 시장 지수 수집 (yfinance)
2. Smart Money 스냅샷
3. Ailey 일간 시장 분석
(포트폴리오는 브라우저에서 API 호출 시 저장)
"""

import os
import json
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf

from daily_db import (
    save_market_indices_snapshot,
    save_smart_money_snapshot,
    save_ailey_analysis,
    get_today_kst,
)

KST = ZoneInfo("Asia/Seoul")

# 수집할 지수 목록
INDICES_MAP = {
    "^GSPC":    "S&P 500",
    "^IXIC":    "NASDAQ",
    "^DJI":     "Dow Jones",
    "^RUT":     "Russell 2000",
    "^VIX":     "VIX",
    "GC=F":     "Gold",
    "CL=F":     "Crude Oil",
    "BTC-USD":  "Bitcoin",
    "^TNX":     "10Y Treasury",
    "DX-Y.NYB": "Dollar Index",
    "KRW=X":    "USD/KRW",
}


def collect_market_indices() -> bool:
    """시장 지수 일봉 수집 & DB 저장 — 배치 다운로드로 rate limit 방지"""
    import pandas as pd
    try:
        print("📡 시장 지수 배치 수집 중...")
        symbols = list(INDICES_MAP.keys())

        # 한 번에 모아 다운로드
        raw = yf.download(
            symbols,
            period="5d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if raw.empty:
            print("⚠️ yfinance 배치 다운로드 결과 없음")
            return False

        closes = raw["Close"]
        opens  = raw.get("Open",  closes)
        highs  = raw.get("High",  closes)
        lows   = raw.get("Low",   closes)
        vols   = raw.get("Volume", closes * 0)

        def _get_val(df, col):
            try:
                s = df[col].dropna() if isinstance(df, pd.DataFrame) else df.dropna()
                return float(s.iloc[-1]) if len(s) else 0.0
            except Exception:
                return 0.0

        rows = []
        for symbol, name in INDICES_MAP.items():
            try:
                if isinstance(closes, pd.DataFrame) and symbol not in closes.columns:
                    continue
                s_close = (closes[symbol] if isinstance(closes, pd.DataFrame) else closes).dropna()
                if len(s_close) < 1:
                    continue
                close      = float(s_close.iloc[-1])
                prev_close = float(s_close.iloc[-2]) if len(s_close) >= 2 else close
                chg_pct    = ((close - prev_close) / prev_close * 100) if prev_close else 0
                rows.append({
                    "symbol":     symbol,
                    "name":       name,
                    "open":       _get_val(opens, symbol),
                    "high":       _get_val(highs, symbol),
                    "low":        _get_val(lows,  symbol),
                    "close":      close,
                    "volume":     _get_val(vols,  symbol),
                    "change_pct": round(chg_pct, 4),
                })
            except Exception as e:
                print(f"  ⚠️ {symbol} 파싱 실패: {e}")

        if rows:
            save_market_indices_snapshot(rows)
            print(f"✅ 시장 지수 {len(rows)}개 저장 완료")
            return True
        return False
    except Exception as e:
        print(f"❌ 시장 지수 수집 에러: {e}")
        traceback.print_exc()
        return False


def collect_smart_money():
    """Smart Money CSV → DB 스냅샷"""
    try:
        import pandas as pd
        csv_path = os.path.join(os.path.dirname(__file__), "smart_money_picks_v2.csv")
        if not os.path.exists(csv_path):
            print("⚠️ smart_money_picks_v2.csv 없음, 스킵")
            return False

        df = pd.read_csv(csv_path)
        picks = []
        for _, row in df.iterrows():
            picks.append({
                "ticker":          str(row.get("ticker", "")),
                "name":            str(row.get("name", row.get("longName", ""))),
                "score":           float(row.get("smart_money_score", row.get("score", 0)) or 0),
                "grade":           str(row.get("grade", "")),
                "price":           float(row.get("currentPrice", row.get("price", 0)) or 0),
                "price_change_1d": float(row.get("price_change_1d", 0) or 0),
                "volume":          float(row.get("volume", 0) or 0),
                "sector":          str(row.get("sector", "")),
                "smart_money_flow": str(row.get("smart_money_flow", "")),
            })

        if picks:
            save_smart_money_snapshot(picks)
            print(f"✅ Smart Money {len(picks)}개 저장 완료")
        return True
    except Exception as e:
        print(f"❌ Smart Money 수집 에러: {e}")
        traceback.print_exc()
        return False


def collect_ailey_market_summary(gemini_api_key: str = None):
    """
    Ailey 일간 시장 요약 생성 & DB 저장
    (GEMINI_API_KEY 없으면 스킵)
    """
    key = gemini_api_key or os.getenv("GEMINI_API_KEY")
    if not key:
        print("⚠️ GEMINI_API_KEY 없음 — Ailey 일간 요약 스킵")
        return False

    try:
        import google.generativeai as genai
        genai.configure(api_key=key)

        today  = get_today_kst()
        now_kst = datetime.now(KST).strftime("%Y.%m.%d_%H:%M:%S")
        prompt = (
            f"오늘({today}) 미국 주식 시장 전체 상황을 한국어 반말로 요약해줘. "
            "초등학생도 이해할 수 있는 비유(날씨/교통/농사 등)를 사용하고, "
            "이모지🌡️📉🚀를 적극 활용해. "
            "형식: 첫 줄에 [🟢 Online Mode | {now_kst}] (Asia/Seoul), "
            "다음에 시장 요약(3~5문장), 주목할 섹터, 오늘의 한마디."
        )

        model    = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        text     = response.text or ""

        if text:
            save_ailey_analysis(
                response=text,
                analysis_type="market",
                prompt=prompt,
                model="gemini-1.5-flash",
                today=today,
            )
            print(f"✅ Ailey 일간 요약 저장 완료 ({today})")
            return True
    except Exception as e:
        print(f"❌ Ailey 시장 요약 에러: {e}")
        traceback.print_exc()
    return False


def run_daily_collection(gemini_api_key: str = None):
    """전체 일일 수집 실행 (스케줄러에서 호출)"""
    print(f"\n🕛 일일 데이터 수집 시작 — {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}")
    results = {
        "market_indices": collect_market_indices(),
        "smart_money":    collect_smart_money(),
        "ailey_summary":  collect_ailey_market_summary(gemini_api_key),
    }
    print(f"✅ 수집 완료: {results}\n")
    return results


if __name__ == "__main__":
    # 직접 실행해서 즉시 수집 테스트
    from daily_db import init_db
    init_db()
    run_daily_collection()
