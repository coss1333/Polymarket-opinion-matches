#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from rapidfuzz import fuzz, process
from dotenv import load_dotenv

# ----------------- Helpers -----------------

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = s.replace('–','-').replace('—','-').replace(' ',' ').replace('\u00A0',' ')
    s = re.sub(r'\bbtc\b', 'BTC', s, flags=re.I)
    s = re.sub(r'\beth\b', 'ETH', s, flags=re.I)
    s = re.sub(r'\busd\b', 'USD', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s)
    return s

def clean_for_match(s: str) -> str:
    s = norm_text(s)
    s = re.sub(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+\d{4}\b', '', s, flags=re.I)
    s = re.sub(r'\b\d{4}-\d{2}-\d{2}\b', '', s)
    s = s.replace('≥','>=').replace('≤','<=').replace('↗',' ').replace('↘',' ')
    s = re.sub(r'[|•·]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def similar(a: str, b: str) -> int:
    return fuzz.token_set_ratio(clean_for_match(a), clean_for_match(b))

def now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def ensure_out():
    os.makedirs("out", exist_ok=True)

def limit_rows(df: pd.DataFrame, max_rows: Optional[int]) -> pd.DataFrame:
    if max_rows is not None:
        return df.head(int(max_rows))
    return df

# ----------------- Polymarket -----------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_polymarket_markets(status: str = "active") -> List[Dict[str, Any]]:
    base = "https://gamma-api.polymarket.com"
    url = f"{base}/markets"
    params = {}
    if status and status.lower() in {"active", "closed", "resolved"}:
        params["status"] = status.lower()
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        markets = data.get("data") or data.get("markets") or data.get("result") or []
    else:
        markets = data
    if not isinstance(markets, list):
        markets = markets.get("data", [])
    return markets

def normalize_polymarket(markets: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for m in markets:
        title = m.get("question") or m.get("title") or m.get("name") or ""
        market_id = m.get("id") or m.get("market_id") or m.get("slug") or m.get("conditionId") or ""
        url = None
        slug = m.get("slug") or m.get("url_slug") or None
        if slug:
            url = f"https://polymarket.com/event/{slug}"
        else:
            url = "https://polymarket.com/"
        end_ts = m.get("endDate") or m.get("end_time") or m.get("expiry") or None
        try:
            if isinstance(end_ts, (int, float)):
                deadline = datetime.utcfromtimestamp(int(end_ts)).isoformat()
            elif isinstance(end_ts, str):
                deadline = end_ts
            else:
                deadline = None
        except Exception:
            deadline = None

        category = m.get("category") or m.get("topic") or None
        status = m.get("status") or None
        volume = m.get("volume") or m.get("liquidity") or None

        rows.append({
            "platform": "Polymarket",
            "title": norm_text(title),
            "market_id": market_id,
            "category": category,
            "status": status,
            "deadline": deadline,
            "volume": volume,
            "url": url
        })
    return pd.DataFrame(rows)

# ----------------- Opinion.trade -----------------

def _opinion_via_sdk(page=1, limit=20, status="ACTIVATED") -> List[Dict[str, Any]]:
    \"\"\"Fetch markets via the official SDK if installed.\"\"\"
    try:
        from opinion_clob_sdk.client import Client
        from opinion_clob_sdk.model import TopicStatusFilter
    except Exception:
        return []

    client = Client()
    all_rows: List[Dict[str, Any]] = []
    cur_page = 1
    while True:
        try:
            resp = client.get_markets(status=getattr(TopicStatusFilter, status) if status else None,
                                      page=cur_page, limit=limit)
            if getattr(resp, "errno", None) != 0:
                break
            items = getattr(getattr(resp, "result", None), "list", []) or []
            if not items:
                break
            all_rows.extend([dict(x) if hasattr(x, '__iter__') else x for x in items])
            cur_page += 1
            if cur_page > 200:
                break
        except Exception:
            break
    return all_rows

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def fetch_opinion_markets(use_sdk: bool = True, status: str = "ACTIVATED") -> List[Dict[str, Any]]:
    if use_sdk:
        items = _opinion_via_sdk(status=status)
        if items:
            return items
    # Placeholder for future REST usage if available publicly
    return []

def normalize_opinion(markets: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for m in markets:
        # m может быть объектом SDK с атрибутами — попробуем универсально
        get = (lambda k: (m.get(k) if isinstance(m, dict) else getattr(m, k, None)))
        title = get("market_title") or get("title") or get("question") or ""
        market_id = get("market_id") or get("id") or ""
        status = get("status") or None
        category = get("topic_type") or get("category") or None
        deadline = get("deadline") or get("end_time") or None
        url = f"https://app.opinion.trade/market/{market_id}" if market_id else "https://app.opinion.trade/"
        rows.append({
            "platform": "Opinion",
            "title": norm_text(title),
            "market_id": market_id,
            "category": category,
            "status": status,
            "deadline": deadline,
            "volume": get("volume") or None,
            "url": url
        })
    return pd.DataFrame(rows)

# ----------------- Matching -----------------

def match_markets(df_poly: pd.DataFrame, df_opi: pd.DataFrame, threshold: int = 86) -> pd.DataFrame:
    if df_poly.empty or df_opi.empty:
        return pd.DataFrame(columns=[
            "poly_title","poly_id","poly_url",
            "opinion_title","opinion_id","opinion_url",
            "similarity","is_exact"
        ])

    opinion_titles = df_opi["title"].tolist()
    opi_reset = df_opi.reset_index(drop=True)

    matches = []
    for _, row in df_poly.iterrows():
        title = row["title"]
        best = process.extract(
            clean_for_match(title),
            [clean_for_match(t) for t in opinion_titles],
            scorer=fuzz.token_set_ratio,
            limit=3
        )
        for _, score, idx in best:
            if score >= threshold:
                op_row = opi_reset.iloc[idx]
                matches.append({
                    "poly_title": row["title"],
                    "poly_id": row["market_id"],
                    "poly_url": row["url"],
                    "opinion_title": op_row["title"],
                    "opinion_id": op_row["market_id"],
                    "opinion_url": op_row["url"],
                    "similarity": int(score),
                    "is_exact": int(row["title"].strip().lower() == op_row["title"].strip().lower())
                })
    if not matches:
        return pd.DataFrame(columns=[
            "poly_title","poly_id","poly_url","opinion_title","opinion_id","opinion_url","similarity","is_exact"
        ])
    return pd.DataFrame(matches).drop_duplicates()

# ----------------- Main -----------------

def main():
    load_dotenv()
    ensure_out()

    poly_status = os.getenv("POLYMARKET_STATUS", "active")
    sim_threshold = int(os.getenv("SIMILARITY_THRESHOLD", "86"))
    max_rows = int(os.getenv("MAX_MARKETS_PER_PLATFORM")) if os.getenv("MAX_MARKETS_PER_PLATFORM") else None
    opinion_use_sdk = os.getenv("OPINION_USE_SDK", "true").lower() == "true"

    print(f"[{now_iso()}] Fetching Polymarket (status={poly_status}) ...")
    poly_raw = fetch_polymarket_markets(status=poly_status)
    df_poly = normalize_polymarket(poly_raw)
    df_poly = limit_rows(df_poly, max_rows)
    poly_path = os.path.join("out", "polymarket_markets.xlsx")
    df_poly.to_excel(poly_path, index=False)
    print(f"Saved {poly_path} ({len(df_poly)} rows)")

    print(f"[{now_iso()}] Fetching Opinion.trade (SDK={opinion_use_sdk}) ...")
    opi_raw = fetch_opinion_markets(use_sdk=opinion_use_sdk, status="ACTIVATED")
    df_opi = normalize_opinion(opi_raw) if opi_raw else pd.DataFrame(columns=["platform","title","market_id","category","status","deadline","volume","url"])
    df_opi = limit_rows(df_opi, max_rows)
    opi_path = os.path.join("out", "opinion_markets.xlsx")
    df_opi.to_excel(opi_path, index=False)
    print(f"Saved {opi_path} ({len(df_opi)} rows)")

    print(f"[{now_iso()}] Matching ... threshold={sim_threshold}")
    df_matches = match_markets(df_poly, df_opi, threshold=sim_threshold)
    match_path = os.path.join("out", "matches.xlsx")
    df_matches.to_excel(match_path, index=False)
    print(f"Saved {match_path} ({len(df_matches)} rows)")

    # Also save CSVs
    df_poly.to_csv(os.path.join("out", "polymarket_markets.csv"), index=False)
    df_opi.to_csv(os.path.join("out", "opinion_markets.csv"), index=False)
    df_matches.to_csv(os.path.join("out", "matches.csv"), index=False)

    print("Done.")

if __name__ == "__main__":
    main()
