"""
daily_db.py — 일일 데이터 누적 저장 모듈

4개 테이블:
1. portfolio_daily      — 포트폴리오 일별 수익률/평가금액
2. smart_money_daily    — Smart Money 종목 일별 점수/가격
3. market_indices_daily — 시장 지수 일봉 (S&P, NASDAQ, VIX...)
4. ailey_analysis_daily — Ailey AI 분석 결과 날짜별 누적
"""

import os
import sqlite3
import json
from datetime import datetime, date
from zoneinfo import ZoneInfo

# DB 파일 경로 (Render 영구 디스크 마운트 경로 환경변수 DB_PATH 로 덮어쓰기 가능)
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_data.db"))

KST = ZoneInfo("Asia/Seoul")

def get_today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # 동시 읽기/쓰기 안전
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

# ─────────────────────────────────────────
# DB 초기화 — 테이블 없으면 자동 생성
# ─────────────────────────────────────────
def init_db():
    with get_conn() as conn:
        conn.executescript("""
        -- 1. 포트폴리오 일별 스냅샷
        CREATE TABLE IF NOT EXISTS portfolio_daily (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,          -- YYYY-MM-DD (KST)
            ticker          TEXT NOT NULL,
            name            TEXT,
            quantity        REAL,
            avg_price       REAL,                   -- 매수단가 ($)
            current_price   REAL,                   -- 당일 종가 ($)
            market_value    REAL,                   -- 평가금액
            unrealized_pnl  REAL,                   -- 미실현 손익
            pnl_pct         REAL,                   -- 수익률 (%)
            total_invested  REAL,                   -- 총 포트폴리오 내 투자금
            UNIQUE(date, ticker)                    -- 날짜+티커 중복 방지
        );

        -- 2. Smart Money 종목 일별 추적
        CREATE TABLE IF NOT EXISTS smart_money_daily (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            ticker          TEXT NOT NULL,
            name            TEXT,
            score           REAL,                   -- Smart Money 점수
            grade           TEXT,                   -- S/A/B/C/D/F
            price           REAL,                   -- 당일 가격
            price_change_1d REAL,                   -- 1일 변화율 (%)
            volume          REAL,                   -- 거래량
            sector          TEXT,
            smart_money_flow TEXT,                  -- 자금 흐름 방향
            UNIQUE(date, ticker)
        );

        -- 3. 시장 지수 일봉
        CREATE TABLE IF NOT EXISTS market_indices_daily (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            symbol      TEXT NOT NULL,              -- ^GSPC, ^IXIC, ^VIX ...
            name        TEXT,                       -- S&P 500, NASDAQ ...
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      REAL,
            change_pct  REAL,                       -- 전일 대비 변화율 (%)
            UNIQUE(date, symbol)
        );

        -- 4. Ailey AI 분석 결과 누적
        CREATE TABLE IF NOT EXISTS ailey_analysis_daily (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            ticker          TEXT,                   -- 특정 종목 분석이면 티커 (없으면 NULL = 시장 분석)
            analysis_type   TEXT NOT NULL,          -- 'stock' | 'market' | 'portfolio' | 'chat'
            prompt          TEXT,                   -- 사용자 질문
            response        TEXT NOT NULL,          -- Ailey 답변 전문
            model           TEXT,                   -- 사용된 AI 모델
            created_at      TEXT DEFAULT (datetime('now'))
        );

        -- 인덱스 (빠른 조회를 위해)
        CREATE INDEX IF NOT EXISTS idx_portfolio_date   ON portfolio_daily(date);
        CREATE INDEX IF NOT EXISTS idx_smart_money_date ON smart_money_daily(date);
        CREATE INDEX IF NOT EXISTS idx_market_date      ON market_indices_daily(date);
        CREATE INDEX IF NOT EXISTS idx_ailey_date       ON ailey_analysis_daily(date);
        CREATE INDEX IF NOT EXISTS idx_ailey_ticker     ON ailey_analysis_daily(ticker);
        """)
    print(f"✅ DB 초기화 완료: {DB_PATH}")


# ─────────────────────────────────────────
# 1. 포트폴리오 저장
# ─────────────────────────────────────────
def save_portfolio_snapshot(holdings: list[dict], today: str = None):
    """
    holdings: [
        {ticker, name, quantity, avg_price, current_price,
         market_value, unrealized_pnl, pnl_pct, total_invested}
    ]
    """
    today = today or get_today_kst()
    rows = []
    for h in holdings:
        rows.append((
            today,
            h.get("ticker", ""),
            h.get("name", ""),
            h.get("quantity", 0),
            h.get("avg_price", 0),
            h.get("current_price", 0),
            h.get("market_value", 0),
            h.get("unrealized_pnl", 0),
            h.get("pnl_pct", 0),
            h.get("total_invested", 0),
        ))
    with get_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO portfolio_daily
            (date, ticker, name, quantity, avg_price, current_price,
             market_value, unrealized_pnl, pnl_pct, total_invested)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)
    print(f"💾 포트폴리오 {len(rows)}개 종목 저장 ({today})")


# ─────────────────────────────────────────
# 2. Smart Money 저장
# ─────────────────────────────────────────
def save_smart_money_snapshot(picks: list[dict], today: str = None):
    today = today or get_today_kst()
    rows = []
    for p in picks:
        rows.append((
            today,
            p.get("ticker", ""),
            p.get("name", ""),
            p.get("score", 0),
            p.get("grade", ""),
            p.get("price", 0),
            p.get("price_change_1d", 0),
            p.get("volume", 0),
            p.get("sector", ""),
            p.get("smart_money_flow", ""),
        ))
    with get_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO smart_money_daily
            (date, ticker, name, score, grade, price, price_change_1d,
             volume, sector, smart_money_flow)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)
    print(f"💾 Smart Money {len(rows)}개 종목 저장 ({today})")


# ─────────────────────────────────────────
# 3. 시장 지수 저장
# ─────────────────────────────────────────
def save_market_indices_snapshot(indices: list[dict], today: str = None):
    today = today or get_today_kst()
    rows = []
    for idx in indices:
        rows.append((
            today,
            idx.get("symbol", ""),
            idx.get("name", ""),
            idx.get("open", 0),
            idx.get("high", 0),
            idx.get("low", 0),
            idx.get("close", 0),
            idx.get("volume", 0),
            idx.get("change_pct", 0),
        ))
    with get_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO market_indices_daily
            (date, symbol, name, open, high, low, close, volume, change_pct)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, rows)
    print(f"💾 시장 지수 {len(rows)}개 저장 ({today})")


# ─────────────────────────────────────────
# 4. Ailey 분석 저장
# ─────────────────────────────────────────
def save_ailey_analysis(
    response: str,
    analysis_type: str = "chat",
    ticker: str = None,
    prompt: str = None,
    model: str = None,
    today: str = None,
):
    today = today or get_today_kst()
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO ailey_analysis_daily
            (date, ticker, analysis_type, prompt, response, model)
            VALUES (?,?,?,?,?,?)
        """, (today, ticker, analysis_type, prompt, response, model))
    print(f"💾 Ailey 분석 저장 ({analysis_type} / {ticker or 'market'} / {today})")


# ─────────────────────────────────────────
# 조회 함수들 (API용)
# ─────────────────────────────────────────
def get_portfolio_history(ticker: str = None, days: int = 30) -> list[dict]:
    """포트폴리오 히스토리 조회"""
    with get_conn() as conn:
        if ticker:
            rows = conn.execute("""
                SELECT * FROM portfolio_daily
                WHERE ticker = ?
                ORDER BY date DESC LIMIT ?
            """, (ticker, days)).fetchall()
        else:
            rows = conn.execute("""
                SELECT date,
                       SUM(market_value)    AS total_value,
                       SUM(unrealized_pnl)  AS total_pnl,
                       SUM(total_invested)  AS total_invested
                FROM portfolio_daily
                GROUP BY date
                ORDER BY date DESC LIMIT ?
            """, (days,)).fetchall()
        return [dict(r) for r in rows]


def get_smart_money_history(ticker: str = None, days: int = 30) -> list[dict]:
    """Smart Money 히스토리 조회"""
    with get_conn() as conn:
        if ticker:
            rows = conn.execute("""
                SELECT * FROM smart_money_daily
                WHERE ticker = ?
                ORDER BY date DESC LIMIT ?
            """, (ticker, days)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM smart_money_daily
                ORDER BY date DESC, score DESC LIMIT ?
            """, (days * 20,)).fetchall()
        return [dict(r) for r in rows]


def get_market_history(symbol: str = None, days: int = 90) -> list[dict]:
    """시장 지수 히스토리 조회"""
    with get_conn() as conn:
        if symbol:
            rows = conn.execute("""
                SELECT * FROM market_indices_daily
                WHERE symbol = ?
                ORDER BY date ASC LIMIT ?
            """, (symbol, days)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM market_indices_daily
                ORDER BY date DESC LIMIT ?
            """, (days * 10,)).fetchall()
        return [dict(r) for r in rows]


def get_ailey_history(ticker: str = None, analysis_type: str = None, days: int = 30) -> list[dict]:
    """Ailey 분석 히스토리 조회"""
    with get_conn() as conn:
        where_clauses = []
        params = []
        if ticker:
            where_clauses.append("ticker = ?")
            params.append(ticker)
        if analysis_type:
            where_clauses.append("analysis_type = ?")
            params.append(analysis_type)
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        params.append(days * 5)
        rows = conn.execute(f"""
            SELECT id, date, ticker, analysis_type, prompt,
                   substr(response, 1, 300) AS response_preview,
                   model, created_at
            FROM ailey_analysis_daily
            {where_sql}
            ORDER BY created_at DESC LIMIT ?
        """, params).fetchall()
        return [dict(r) for r in rows]


def get_db_stats() -> dict:
    """DB 현황 통계"""
    with get_conn() as conn:
        stats = {}
        for table in ["portfolio_daily", "smart_money_daily",
                      "market_indices_daily", "ailey_analysis_daily"]:
            row = conn.execute(f"""
                SELECT COUNT(*) AS cnt,
                       MIN(date) AS oldest,
                       MAX(date) AS latest
                FROM {table}
            """).fetchone()
            stats[table] = dict(row)
        return stats


if __name__ == "__main__":
    init_db()
    print(json.dumps(get_db_stats(), indent=2, ensure_ascii=False))
