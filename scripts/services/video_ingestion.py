# scripts/services/video_ingestion.py
# 總覽：
# - dim_video 更新：run_fetch_videos 依頻道與日期抓取影片清單與詳情，分流為完整 upsert 或僅統計更新。
# - Analytics 視窗：run_top_videos 取 D-3~D-2 的 Top Videos，解析/補 meta 後批次 upsert 至 fact_yta_video_window。
# - 輔助與正規化：提供 YA 回傳表格解析、結果列組裝、補中繼資訊、型別正規化與批次 upsert。

import time
from typing import Optional, List, Dict, Any, Tuple, Callable
from datetime import datetime, date, timedelta
from decimal import Decimal
from scripts.ingestion.ya_api import get_analytics_client, query_reports  # 延遲匯入，避免硬依賴
from scripts.utils.dates import validate_date_str, default_dates_for_window_by_offset, valid_date_str
from scripts.utils.env import load_settings
from scripts.db.db import (
    get_engine,
    get_existing_videos,
    get_raw_cursor,
    upsert_dim_video_full,
    upsert_dim_video_stats_only,
)
from scripts.channel.ensure import ensure_dim_channel_exists
from scripts.youtube.playlists import fetch_channel_video_ids
from scripts.youtube.videos import fetch_videos_details_batch, _decide_video_type, fetch_video_meta_map
from scripts.models.fact_yta_video_window import KNOWN_FACT_WINDOW_COLS, UPSERT_SQL_FACT_WINDOW

# -------------------------------
# Public: fetch videos into dim_video
# -------------------------------
def run_fetch_videos(
    channel_id: str,
    max_results: int,
    published_after: Optional[str],
    published_before: Optional[str],
    settings: Dict[str, str],
) -> None:
    """
    依頻道與日期抓取影片清單與詳情，並寫入 dim_video（分為完整 upsert 與僅統計更新）。
    - 參數：
      channel_id：頻道 ID；max_results：每批呼叫 videos.list 的上限
      published_after/published_before：過濾頻道 uploads 的發佈時間（YYYY-MM-DD）
      settings：需提供 YPKG_API_KEY（YouTube Data API 金鑰）
    - 流程：
      1) 驗證日期參數格式
      2) 初始化 DB、確保 dim_channel 存在
      3) 取頻道 uploads 影片 IDs
      4) 查詢既有影片，判斷是否已做過 shorts_check
      5) videos.list 分批取詳情
      6) 分流 rows_full（完整 upsert）與 rows_stats（僅統計）並寫入
    - 輸出：透過 print 回報進度與摘要
    """
    # 1) 驗證日期（允許 None，若提供必須符合 YYYY-MM-DD）
    pa = validate_date_str(published_after)
    pb = validate_date_str(published_before)

    # 2) 初始化 DB 與確保頻道存在（若無則建立 dim_channel 基本資料）
    engine = get_engine()
    ch_payload = ensure_dim_channel_exists(engine, channel_id)
    print("[info] 確保 dim_channel：", {"channel_id": channel_id, "name": (ch_payload or {}).get("channel_name")})

    # 從設定取出 YouTube API Key（若未提供，fetch 將失敗——此處假設外部保證）
    yt_api_key = (settings.get("YPKG_API_KEY") or "").strip()

    # 3) 取得 video_ids（由 uploads 播放清單與日期條件過濾）
    print("[info] 取得頻道影片清單（uploads 播放清單）…")
    video_ids: List[str] = fetch_channel_video_ids(
        yt_api_key=yt_api_key,
        channel_id=channel_id,
        published_after=pa,
        published_before=pb,
    )
    print(f"[info] 取得 video_id 數量：{len(video_ids)}")
    if not video_ids:
        print("[info] 無影片可處理。")
        return

    # 4) 查詢既有影片（回傳 map：video_id -> 現存欄位，用於判斷是否只需更新統計）
    existing_map: Dict[str, Dict[str, Any]] = get_existing_videos(engine, video_ids)

    # 5) videos.list 分批抓詳情（以 max_results 為批次）
    print("[info] videos.list 分批抓取詳情…")

    # 用來儲存每支影片的詳情資料，key 為 video_id，value 為該影片的詳細資訊字典
    details_map: Dict[str, Dict[str, Any]] = {}

    # 依批次切片呼叫 API，避免超過每次上限
    for i in range(0, len(video_ids), max_results):
        batch = video_ids[i : i + max_results]

        # 呼叫批次查詢函式，向 YouTube Data API 的 videos.list 取回影片詳情
        batch_details = fetch_videos_details_batch(
            yt_api_key=yt_api_key,
            video_ids=batch,
        )

        # 將每支影片的詳情寫入 details_map（用 video_id 當 key）
        for d in batch_details:
            vid = d["video_id"]     # API 回傳的影片 ID（預期存在）
            details_map[vid] = d    # 若重複 key，後者覆蓋前者（正常不會發生）

    # 6) 準備 upsert rows（分為完整 upsert 與僅統計更新）
    rows_full: List[Dict[str, Any]] = []
    rows_stats: List[Dict[str, Any]] = []
    # - rows_full：首次或尚未做 shorts_check 的影片，需寫入完整欄位（meta + 統計）
    # - rows_stats：已做過 shorts_check 的既有影片，僅更新變動的統計欄位

    for vid in video_ids:
        d = details_map.get(vid)
        if not d:
            # 影片可能下架或權限受限，API 未返回；略過避免寫入不完整資料
            continue

        existed = existing_map.get(vid)
        if existed and int(existed.get("shorts_check", 0)) == 1:
            # 僅更新統計欄位
            rows_stats.append(
                {
                    "video_id": vid,
                    "view_count": d.get("view_count"),
                    "like_count": d.get("like_count"),
                    "comment_count": d.get("comment_count"),
                }
            )
        else:
            # 決定 video_type 與 is_short（使用新版邏輯 _decide_video_type）
            video_type = _decide_video_type(
                vid=vid,
                snippet=(d.get("raw") or {}).get("snippet"),
                live_details=(d.get("raw") or {}).get("liveStreamingDetails"),
                duration_sec=d.get("duration_sec"),
                published_at=d.get("published_at"),  # 已是 naive UTC
            )
            is_short = 1 if video_type == "shorts" else 0
            # 完整 upsert 所需欄位（含 meta 與統計）
            rows_full.append(
                {
                    "video_id": vid,
                    "channel_id": d.get("channel_id") or channel_id,
                    "video_title": d.get("video_title"),
                    "published_at": d.get("published_at"),
                    "duration_sec": d.get("duration_sec"),
                    "is_short": is_short,
                    "shorts_check": 1,
                    "video_type": video_type,
                    "view_count": d.get("view_count"),
                    "like_count": d.get("like_count"),
                    "comment_count": d.get("comment_count"),
                }
            )

    print(f"[info] 準備 upsert：full={len(rows_full)}, stats-only={len(rows_stats)}")

    # 7) 寫 DB（先 full 再 stats，避免舊資料覆蓋新 meta）
    if rows_full:
        upsert_dim_video_full(engine, rows_full)
    if rows_stats:
        upsert_dim_video_stats_only(engine, rows_stats)

    print(
        f"[success] 完成更新：總筆數={len(rows_full) + len(rows_stats)}, "
        f"完整寫入={len(rows_full)}, 僅統計={len(rows_stats)}"
    )

# -------------------------------
# Public: top videos into fact_yta_video_window
# -------------------------------
def _default_build_analytics_client(settings: Dict[str, str]):
    """
    預設工廠：依 settings 建立 YouTube Analytics API client。
    - 依據設定鍵：
      YAAO_TOKEN_PATH / YAAO_CREDENTIALS_PATH / YAAO_OAUTH_SCOPES / YAAO_OAUTH_PORT
    - 可於測試時以參數注入自定工廠取代
    """
    scopes = [s.strip() for s in (settings.get("YAAO_OAUTH_SCOPES") or "").split(",") if s.strip()]
    return get_analytics_client(
        token_path=settings.get("YAAO_TOKEN_PATH"),
        client_secret_path=settings.get("YAAO_CREDENTIALS_PATH"),
        scopes=scopes or None,
        oauth_port=int(settings.get("YAAO_OAUTH_PORT", "0") or 0),
    )

def _default_reports_query(analytics_client, **kwargs):
    """
    預設查詢：透過封裝的 query_reports 呼叫 Analytics。
    - 允許以 reports_query_fn 注入自定查詢以便測試或替換實作
    """
    return query_reports(analytics_client=analytics_client, **kwargs)

def _d3_d2_window(today: date | None = None):
    """
    回傳 D-3 ~ D-2 的日期區間（YYYY-MM-DD, YYYY-MM-DD）。
    - 用途：Top Videos 任務的固定預設視窗
    - today 可注入以便測試
    """
    today = today or date.today()
    start = today - timedelta(days=3)
    end = today - timedelta(days=2)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def run_top_videos(
    channel_id: str,
    start_date: Optional[str],
    end_date: Optional[str],
    from_offset: int,
    to_offset: int,
    metric: str,
    top_n: int,
    include_revenue: bool,
    settings: Dict[str, str],
    analytics_client_factory: Optional[Callable[[Dict[str, str]], Any]] = None,
    reports_query_fn: Optional[Callable[..., Dict[str, Any]]] = None,
):
    """
    取得頻道在指定區間內的 Top Videos，並 upsert 至 fact_yta_video_window。
    - 日期策略：固定使用 D-3 ~ D-2（覆寫輸入的 start_date/end_date）
    - 排序與指標：
      base_metrics 預設含 views/estimatedMinutesWatched/likes/comments/shares
      若 include_revenue 則加入 estimatedRevenue；排序欄位由 metric 指定
    - 可注入：
      analytics_client_factory：自定 client 建置；reports_query_fn：自定查詢
    - 流程：
      1) 產生日期區間、檢查排序欄位
      2) 建立 Analytics client
      3) 查 YA Reports（dimensions=['video']，依 metric 降冪排序）
      4) 將回傳表格解析為列物件、組裝結果列、補充影片 meta
      5) 正規化後批次 upsert 至 fact_yta_video_window
    """
    t0 = time.time()
    print("=== [top_videos] 任務開始 ===")
    print(f"[top_videos] 參數: channel_id={channel_id}, start_date={start_date}, end_date={end_date}, "
          f"from_offset={from_offset}, to_offset={to_offset}, metric={metric}, top_n={top_n}, "
          f"include_revenue={include_revenue}")

    # 1) 日期處理：固定使用 D-3 ~ D-2（忽略外部 start/end）
    start_date, end_date = _d3_d2_window()
    print(f"[top_videos] 使用預設優先的固定日期區間（D-3 ~ D-2）: {start_date} ~ {end_date}")

    # 2) 指標設定與檢查排序欄位是否受支援
    base_metrics = ["views", "estimatedMinutesWatched", "likes", "comments", "shares"]
    if include_revenue:
        base_metrics.append("estimatedRevenue")
    if metric not in base_metrics:
        raise ValueError(f"不支援的排序指標：{metric}，可用：{', '.join(base_metrics)}")
    print(f"[top_videos] 查詢指標(metrics): {base_metrics}，排序指標(metric): {metric}，top_n={top_n}")

    # 3) 建立 Analytics client（可注入替代）
    t_client = time.time()
    build_client = analytics_client_factory or _default_build_analytics_client
    print("[top_videos] 建立 YouTube Analytics client 中...")
    analytics = build_client(settings)
    print(f"[top_videos] Analytics client 建立完成，耗時 {time.time() - t_client:.2f}s")

    # 4) 查詢 Analytics（dimensions=['video']，sort 使用負號代表降冪）
    ids = f"channel=={channel_id}"
    query_fn = reports_query_fn or _default_reports_query
    print(f"[top_videos] 準備查詢 YA Reports: ids={ids}, date={start_date}~{end_date}, "
          f"dimensions=['video'], sort=-{metric}, max_results={top_n}")
    t_query = time.time()
    try:
        resp = query_fn(
            analytics_client=analytics,
            ids=ids,
            start_date=start_date,
            end_date=end_date,
            metrics=base_metrics,
            dimensions=["video"],
            sort=f"-{metric}",
            max_results=top_n,
            include_historical_channel_data=None,
            currency=None,
        )
    except Exception as e:
        import traceback
        print("[error] query_reports 發生例外：", repr(e))
        traceback.print_exc()
        print("[hint] 檢查：OAuth 憑證/授權、scopes、頻道擁有權、Analytics 開通狀態")
        raise
    print(f"[top_videos] YA 查詢完成，耗時 {time.time() - t_query:.2f}s")

    # 無資料則終止
    if not resp or not resp.get("rows"):
        headers = [h.get('name') for h in (resp.get('columnHeaders') or [])] if resp else []
        print(f"[info] 無資料（可能區間內無影片或權限不足）。headers={headers}, rows=0")
        print(f"=== [top_videos] 任務結束（無資料）耗時 {time.time() - t0:.2f}s ===")
        return
    else:
        headers = [h.get('name') for h in resp.get("columnHeaders", [])]
        print(f"[top_videos] YA 回傳欄位: {headers}，筆數: {len(resp.get('rows', []))}")

    # 5) 解析回傳表格、組裝結果列、補中繼資料
    t_parse = time.time()
    parsed = parse_analytics_table(resp)
    print(f"[top_videos] 解析完成：rows={len(parsed.get('rows', []))}，耗時 {time.time() - t_parse:.2f}s")

    t_build = time.time()
    result_rows, video_ids = build_top_video_rows(
        channel_id=channel_id, start_date=start_date, end_date=end_date, parsed_rows=parsed["rows"]
    )
    print(f"[top_videos] 組裝結果列完成：result_rows={len(result_rows)}，distinct_video_ids={len(set(video_ids))}，耗時 {time.time() - t_build:.2f}s")

    t_engine = time.time()
    print("[top_videos] 取得資料庫引擎中...")
    engine = get_engine()
    print(f"[top_videos] 資料庫引擎就緒，耗時 {time.time() - t_engine:.2f}s")

    t_enrich = time.time()
    print("[top_videos] 進行影片中繼資料 enrich 中（查 dim_video / dim_channel 等）...")
    before_len = len(result_rows)
    result_rows = enrich_with_video_meta(engine, result_rows, video_ids)
    after_len = len(result_rows)
    print(f"[top_videos] enrich 完成：原始 {before_len} 筆 -> enrich 後 {after_len} 筆，耗時 {time.time() - t_enrich:.2f}s")

    # 6) 正規化檢查並批次 upsert 至 fact_yta_video_window
    t_upsert = time.time()
    try:
        # 防呆：禁止非原子型別寫入定義欄位
        for i, row in enumerate(result_rows):
            for k, v in row.items():
                if k in KNOWN_FACT_WINDOW_COLS and isinstance(v, (dict, list)):
                    raise ValueError(f"Row {i} column {k} is non-atomic type: {type(v).__name__} -> {v}")
        print(f"[top_videos] 寫入 fact_yta_video_window 中，批次筆數={len(result_rows)} ...")
        affected = upsert_fact_yta_video_window_bulk(engine, result_rows)
        print(f"[success] 完成 top_videos：寫入/更新 {affected} 筆，耗時 {time.time() - t_upsert:.2f}s")
    except Exception as e:
        import traceback
        print(f"[error] 寫入 fact_yta_video_window 失敗：{e}")
        traceback.print_exc()
        raise

    print(f"=== [top_videos] 任務完成，總耗時 {time.time() - t0:.2f}s ===")

# -------------------------------
# Helpers
# -------------------------------
def parse_analytics_table(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    將 YA API 回傳的表格結構轉為 {'headers': [...], 'rows': [ {col: val, ...}, ... ]}。
    - 來源格式：columnHeaders: [{name: str, ...}], rows: [[val1, val2, ...], ...]
    - 回傳：headers（欄名列表）與 rows（字典列）
    """
    headers = [h["name"] for h in resp.get("columnHeaders", [])]
    rows: List[Dict[str, Any]] = []
    for raw in resp.get("rows", []):
        row = {}
        for i, val in enumerate(raw):
            row[headers[i]] = val
        rows.append(row)
    return {"headers": headers, "rows": rows}

def build_top_video_rows(channel_id: str, start_date: str, end_date: str, parsed_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    將 parse_analytics_table 的列轉為 fact_yta_video_window 的候選資料列，並蒐集 video_ids。
    - 行為：
      1) 基礎欄位：channel_id, video_id, start_date, end_date
      2) 先預留 video_title、video_published_at，待 enrich 時補
      3) 其他欄位（除了 video/videoTitle/videoPublishedAt）原樣帶入 payload
    - 回傳：result_rows 與 video_ids（供 enrich 查 meta）
    """
    result_rows: List[Dict[str, Any]] = []
    video_ids: List[str] = []
    for r in parsed_rows:
        video_id = r.get("video")
        if not video_id:
            continue
        video_ids.append(video_id)
        payload = {
            "channel_id": channel_id,
            "video_id": video_id,
            "start_date": start_date,
            "end_date": end_date,
            "video_title": None,
            "video_published_at": None,
        }
        for k, v in r.items():
            if k in ("video", "videoTitle", "videoPublishedAt"):
                continue
            payload[k] = v
        result_rows.append(payload)
    return result_rows, video_ids

def enrich_with_video_meta(engine, rows: List[Dict[str, Any]], video_ids: List[str]) -> List[Dict[str, Any]]:
    """
    以 dim_video 中的中繼資料補足 rows 的 video_title 與 video_published_at。
    - 行為：批次查詢 meta_map，逐列補入對應欄位
    - 若查無對應影片，保留 None
    """
    meta_map = fetch_video_meta_map(engine, video_ids)
    for row in rows:
        meta = meta_map.get(row["video_id"])
        if meta:
            row["video_title"] = meta.get("video_title")
            row["video_published_at"] = meta.get("published_at")
    return rows

# -------------------------------
# Normalize & UPSERT
# -------------------------------
def _to_date(v):
    """
    寬鬆轉換各型別為 date：
    - None -> None；date -> 原值；datetime -> date()；str -> fromisoformat
    - 其他型別拋 ValueError
    """
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return date.fromisoformat(v)
    raise ValueError(f"Invalid date: {v!r}")

def _to_int_or_none(v):
    """
    將輸入轉為 int；None 或空字串回傳 None；失敗回傳 None（容忍不合法數值）。
    - 用於 YA 指標欄位的安全轉換
    """
    if v is None or v == "":
        return None
    try:
        return int(v)
    except:
        return None

def _to_decimal_or_none(v):
    """
    將輸入轉為 Decimal；None 或空字串回傳 None；失敗回傳 None。
    - 用於 estimatedRevenue 等金額類欄位
    """
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except:
        return None

def _normalize_row_for_fact_window(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    將一列結果正規化為 fact_yta_video_window 可寫入的結構：
    - 必填檢查：channel_id、video_id、start_date、end_date
    - 型別轉換：日期欄位 -> date；published_at -> naive datetime
    - 指標欄位：views/likes/comments/... -> int or None；estimatedRevenue -> Decimal or None
    - 其餘未定義欄位併入 ext_metrics（保留原始鍵值）
    """
    r = dict(row)
    if not r.get("channel_id"): raise ValueError("channel_id required")
    if not r.get("video_id"): raise ValueError("video_id required")

    r["start_date"] = _to_date(r.get("start_date"))
    r["end_date"] = _to_date(r.get("end_date"))
    if r["start_date"] is None or r["end_date"] is None:
        raise ValueError("start_date/end_date required")

    # video_published_at 若是 ISO 字串，轉為 naive datetime；失敗則置 None
    if r.get("video_published_at") and isinstance(r["video_published_at"], str):
        try:
            r["video_published_at"] = datetime.fromisoformat(
                r["video_published_at"].replace("Z","+00:00")
            ).replace(tzinfo=None)
        except:
            r["video_published_at"] = None

    # 將常見整數型指標轉為 int 或 None
    for k in ("views","estimatedMinutesWatched","likes","comments","shares","subscribersGained","subscribersLost","watchTime"):
        r[k] = _to_int_or_none(r.get(k))
    # 金額型
    r["estimatedRevenue"] = _to_decimal_or_none(r.get("estimatedRevenue"))

    # 未定義欄位併入 ext_metrics（避免 schema 變更造成遺漏）
    ext = {}
    for k in list(r.keys()):
        if k not in KNOWN_FACT_WINDOW_COLS:
            ext[k] = r.pop(k)
    r["ext_metrics"] = ext or None
    return r

def upsert_fact_yta_video_window_bulk(engine, rows: List[Dict[str, Any]]) -> int:
    """
    將多列結果正規化後批次 upsert 至 fact_yta_video_window。
    - 步驟：
      1) 先透過 _normalize_row_for_fact_window 做欄位檢查與型別轉換
      2) 透過原生 cursor executemany 執行 UPSERT_SQL_FACT_WINDOW
      3) 回傳受影響列數（受資料庫驅動 rowcount 行為影響）
    """
    norm = [_normalize_row_for_fact_window(r) for r in rows]
    with engine.begin() as conn:
        cur = get_raw_cursor(conn)
        cur.executemany(UPSERT_SQL_FACT_WINDOW, norm)
        affected = cur.rowcount
    return affected