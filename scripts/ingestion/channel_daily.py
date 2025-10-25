# scripts/ingestion/channel_daily.py
# 總覽：
# - 串接資料庫、日期視窗計算與 YouTube Analytics 抓取，將頻道「日次」指標寫入事實表。
# - 流程：確保 dim_channel 存在 → 計算抓取視窗 → 呼叫 YA API 取數 → 整理並 upsert 至 fact_yta_channel_daily。
# - 提供 CLI/設定檔雙來源的頻道 ID 解析，並以 env 參數建立 DB/YA client 等相依資源。

from typing import Dict, Any, List
from scripts.db.db import make_engine, upsert_fact_channel_daily
from scripts.utils.dates import compute_window
from scripts.ingestion.dim_channel import ensure_dim_channel
from scripts.ingestion.ya_api import build_ya_client, query_channel_daily

def _resolve_channel_id(cli_channel_id: str | None, settings: dict) -> str:
    """
    解析頻道 ID：優先使用 CLI/函式參數指定，否則回退至設定 settings["CHANNEL_ID"]。
    
    參數：
    - cli_channel_id: 由命令列或上層呼叫傳入的頻道 ID（可能為 None）
    - settings: 組態字典，需包含 "CHANNEL_ID"
    
    回傳：
    - str：最終使用的頻道 ID
    """
    return cli_channel_id or settings["CHANNEL_ID"]

def ingest_channel_daily(channel_id: str, env: Dict[str, str]) -> None:
    """
    執行 channel × day 指標抓取並寫入 fact_yta_channel_daily。
    範圍：上次抓取日+1 或 頻道建立日 或 env.START_DATE 三者最大，到 today-0

    參數：
    - channel_id: 目標頻道 ID
    - env: 執行所需環境參數字典，需包含：
        - DB_URL: 資料庫連線字串
        - START_DATE: 預設開始日期（YYYY-MM-DD），作為視窗回退選項之一
        - 其他給 YA client 用的鍵值（例：YAAO_* 或 GOOGLE_*）

    流程：
    1) 確保 dim_channel 存在（可再擴充補齊標題、建立日等欄位）。
    2) 計算抓取視窗（由上次成功抓取點、頻道建立日、指定起始日三者決定）。
    3) 建立 YA client 並抓取日級資料。
    4) 轉換欄位型別、計算衍生值（如 subscribers_net）。
    5) upsert 到 fact_yta_channel_daily。
    """
    # 建立資料庫 Engine（依據 env["DB_URL"]）
    engine = make_engine(env["DB_URL"])

    # 1) 確保 dim_channel 存在（必要維度資料先就位；未來可延伸更新 title、started_on 等）
    ensure_dim_channel(engine, channel_id)

    # 2) 計算抓取窗口（start_date, end_date）
    # compute_window 會依據資料庫既有資料與 START_DATE 等規則返回實際要抓的區間
    start_date, end_date = compute_window(engine, channel_id, env["START_DATE"])
    if not start_date or not end_date:
        # 若計算無新日期，則直接結束（例如資料已最新）
        print("[ingest_channel_daily] No new dates to ingest. Done.")
        return

    print(f"[ingest_channel_daily] Fetching range: {start_date} ~ {end_date}")

    # 3) 建立 YT Analytics client 並查詢日級資料（維度 day）
    analytics = build_ya_client(env)
    records = query_channel_daily(analytics, channel_id, start_date, end_date)

    # 若 API 無回傳資料，則記錄訊息並結束
    if not records:
        print("[ingest_channel_daily] No data returned from YouTube Analytics.")
        return

    # 4) 準備 upsert rows：將回傳欄位轉為事實表欄位，並處理型別與缺值
    rows: List[dict] = []
    for r in records:
        # 個別先轉為 int，避免 None 或空字串造成錯誤
        subscribers_gained = int(r.get("subscribersGained") or 0)
        subscribers_lost = int(r.get("subscribersLost") or 0)
        rows.append({
            "channel_id": channel_id,
            "day": r["day"],
            "views": int(r.get("views") or 0),
            "estimatedMinutesWatched": int(r.get("estimatedMinutesWatched") or 0),
            "averageViewDuration": int(r.get("averageViewDuration") or 0),
            "averageViewPercentage": float(r.get("averageViewPercentage") or 0.0),
            "likes": int(r.get("likes") or 0),
            "dislikes": int(r.get("dislikes") or 0),
            "comments": int(r.get("comments") or 0),
            "shares": int(r.get("shares") or 0),
            "playlistStarts": int(r.get("playlistStarts") or 0),
            "viewsPerPlaylistStart": float(r.get("viewsPerPlaylistStart") or 0.0),
            "cardClicks": int(r.get("cardClicks") or 0),
            "cardTeaserClicks": int(r.get("cardTeaserClicks") or 0),
            "subscribersGained": subscribers_gained,
            "subscribersLost": subscribers_lost,
            # 衍生欄位：淨訂閱數
            "subscribers_net": subscribers_gained - subscribers_lost,
        })

    # 5) 寫入 DB（upsert 以避免重複，並更新既有紀錄）
    upsert_fact_channel_daily(engine, rows)
    print(f"[ingest_channel_daily] Upserted {len(rows)} rows into fact_yta_channel_daily.")

# 本程式作用摘要：
# - _resolve_channel_id：決定使用的頻道 ID（優先 CLI/參數，其次設定）。
# - ingest_channel_daily：端到端流程，確保維度存在、計算抓取視窗、呼叫 YA、整形與 upsert。
# - 相依組件：make_engine/compute_window/build_ya_client/query_channel_daily/upsert_fact_channel_daily。