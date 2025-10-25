# scripts/youtube/videos.py
# 總覽：
# - fetch_videos_details_batch：批次呼叫 YouTube videos.list，解析常用欄位並回傳含 raw parts 的列表（不在此決定 video_type）。
# - _decide_video_type + 輔助函式：依直播狀態、時長與發佈時間，以及最終導向 URL（shorts/watch）判定影片型別。
# - _get_with_retry：對 requests.get 加上簡易重試與退避；_parse_*：處理 RFC3339 時間與 ISO 8601 時長解析；fetch_video_meta_map：從資料庫讀取影片基本資料映射。

import time
from typing import List, Dict, Any, Optional
import requests
from datetime import datetime, timezone
import re

# YouTube Data API v3 的 base URL
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# 當 response code 在此集合時，視為可重試（429: rate limit, 403: 部分情況, 5xx: 伺服器錯誤）
RETRY_STATUS = {429, 403, 500, 502, 503, 504}

# ISO 8601 Duration 解析用的正則：支援 PnDTnHnMnS（天、時、分、秒；時間部分以 T 開頭）
DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)

# 你的門檻日期：2024-10-15 00:00:00 UTC（naive UTC）
# 用途：在該日期前的短片，特定長度範圍（61–180 秒）可被視為 VOD（相容舊規則）
THRESHOLD_UTC_NAIVE = datetime(2024, 10, 15, 0, 0, 0)

# 一般性 User-Agent（避免被視為非典型用戶端），亦用於最終導向檢查
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; gpt-5/1.0; +https://example.com/bot)"
}

def _get_with_retry(url: str, params: Dict[str, Any], max_retries: int = 5, backoff_base: float = 0.8) -> Dict[str, Any]:
    """
    發送 GET 並在特定狀態碼時進行重試；以簡易指數退避控制等待時間。

    參數：
    - url：請求的 URL。
    - params：查詢參數。
    - max_retries：最大重試次數（不含首次），預設 5 次。
    - backoff_base：退避底數，實際等待為 (backoff_base ** i) * 2 + i*0.1。

    回傳：
    - 成功時回傳 JSON 解析後的字典。

    失敗：
    - 遇非可重試狀態碼：直接 raise_for_status。
    - 重試仍失敗：丟出 RuntimeError，內含最後一次錯誤狀態與訊息。
    """
    last_err = None
    for i in range(max_retries):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        # 在可重試狀態碼時，等待後再試
        if resp.status_code in RETRY_STATUS:
            sleep_s = (backoff_base ** i) * 2 + (i * 0.1)  # 簡單的指數退避 + 微抖動
            time.sleep(sleep_s)
            last_err = (resp.status_code, resp.text)
            continue
        # 其他狀態碼：視為不可重試，直接拋出
        resp.raise_for_status()
    # 用盡重試仍失敗
    raise RuntimeError(f"GET {url} failed after retries: {last_err}")

def _parse_rfc3339_to_naive_utc(s: Optional[str]) -> Optional[datetime]:
    """
    將 RFC3339（如 '2024-01-01T12:34:56Z'）轉成 naive UTC datetime。
    - 解析後會先轉為 tz-aware UTC，再去掉 tzinfo，以便與其他 naive UTC 做比較。
    """
    if not s:
        return None
    dt = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.replace(tzinfo=None)

def _parse_iso8601_duration_to_seconds(s: Optional[str]) -> Optional[int]:
    """
    將 ISO 8601 Duration（例如 'PT1H2M30S'、'PT45S'、'P1DT2H'）解析為秒數。
    - 僅支援天/時/分/秒四種單位，未出現的單位視為 0。
    - 無效格式回傳 None。
    """
    if not s:
        return None
    m = DURATION_RE.match(s)
    if not m:
        return None
    days = int(m.group("days") or 0)
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds

def _classify_by_url_simple(vid: str) -> str:
    """
    透過請求 YouTube 的短連結與 watch 連結，觀察最終導向（redirect 後的 URL）以判斷：
    - 若最終為 /shorts/<id> → 視為 'shorts'
    - 若最終為 /watch?... → 視為 'vod'
    - 否則回傳 'unknown'

    備註：
    - 此法需對 YouTube 發送 HTTP 請求，受網路與平台行為影響。
    - 允許 redirect，並以 requests 自動處理最終 URL。
    """
    def final_url(url: str) -> str:
        try:
            r = requests.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
            return r.url or ""
        except Exception:
            # 網路/超時等情況，視為未知
            return ""
    shorts_url = f"https://www.youtube.com/shorts/{vid}"
    su = final_url(shorts_url)
    if su.startswith("https://www.youtube.com/shorts/"):
        return "shorts"

    watch_url = f"https://www.youtube.com/watch?v={vid}"
    wu = final_url(watch_url)
    if wu.startswith("https://www.youtube.com/watch"):
        return "vod"

    return "unknown"

def _is_before_2024_10_15_utc(published_at_utc_naive: Optional[datetime]) -> bool:
    """
    判斷發佈時間是否早於 2024-10-15（以 naive UTC 比較）。
    - None 視為 False（無法判斷則不套用「歷史相容」規則）
    """
    if published_at_utc_naive is None:
        return False
    return published_at_utc_naive < THRESHOLD_UTC_NAIVE

def _decide_video_type(
    vid: str,
    snippet: Dict[str, Any],
    live_details: Dict[str, Any],
    duration_sec: Optional[int],
    published_at: Optional[datetime],  # 需為 naive UTC
) -> str:
    """
    綜合直播狀態、時長與時間門檻，以及最終導向 URL 來判定影片型別：
    - 'upcoming'：snippet.liveBroadcastContent == 'upcoming'
    - 'live'：snippet.liveBroadcastContent == 'live' 或 liveStreamingDetails 有 actualStartTime 且無 actualEndTime
    - 'live_replay'：存在 actualStartTime 且 actualEndTime
    - 'vod'：長度 > 180 秒；或 61–180 秒且發佈在 2024-10-15 前（歷史相容）
    - 其他短片：透過最終導向 URL 判斷 shorts/vod，否則 'unknown'

    注意：
    - 本函式目前未在 fetch_videos_details_batch 內直接使用，以保留解耦（抓取與分類分離）。
    """
    # 1) 先看 liveBroadcastContent
    lbc = (snippet.get("liveBroadcastContent") or "none").lower()
    if lbc == "upcoming":
        return "upcoming"
    if lbc == "live":
        return "live"

    # 2) live_replay / live（依 liveStreamingDetails）
    actual_start = (live_details or {}).get("actualStartTime")
    actual_end = (live_details or {}).get("actualEndTime")
    if actual_start:
        if actual_end:
            return "live_replay"
        else:
            return "live"

    # 3) 時長與歷史相容
    # 長於 180 秒視為一般 VOD
    if duration_sec is not None and duration_sec > 180:
        return "vod"
    # 61–180 秒且在門檻日期前，視為 VOD（舊規則）
    if duration_sec is not None and duration_sec > 60 and _is_before_2024_10_15_utc(published_at):
        return "vod"

    # 4) 其他短片：以 URL 導向判斷（/shorts/ vs /watch）
    return _classify_by_url_simple(vid)

def fetch_videos_details_batch(
    yt_api_key: str,
    video_ids: List[str],
) -> List[Dict[str, Any]]:
    """
    批次呼叫 YouTube videos.list 取得影片詳情，並回傳「已解析的常用欄位 + raw parts」。
    設計重點：不在此處判斷 video_type，以解耦抓取與分類。

    參數：
    - yt_api_key：YouTube Data API 的 API Key。
    - video_ids：影片 ID 清單（建議最多 50 筆，API 限制）。

    回傳（每支影片一筆 Dict）：
    - video_id：影片 ID
    - channel_id：頻道 ID（可能為 None）
    - video_title：標題（字串）
    - published_at：naive UTC 的 datetime（可能為 None）
    - duration_sec：秒數（int 或 None）
    - view_count / like_count / comment_count：整數或 None
    - raw：原始 parts 的打包字典（snippet/contentDetails/statistics/liveStreamingDetails/status）
    """
    # 1) 防呆：空清單直接回傳，避免送出空請求
    if not video_ids:
        return []

    # 2) 端點 URL
    url = f"{YOUTUBE_API_BASE}/videos"

    # 3) 查詢參數：一次要到所有必要 parts，以減少後續判斷再打 API 的需求
    params = {
        "part": "snippet,contentDetails,statistics,liveStreamingDetails,status",
        "id": ",".join(video_ids),  # 以逗號串接，最多 50
        "key": yt_api_key,
        "maxResults": 50,           # 與批次大小一致（雖非 videos.list 必要，但一致性佳）
    }

    # 4) 發送請求（含重試）
    data = _get_with_retry(url, params)

    # 5) 準備回傳容器
    rows: List[Dict[str, Any]] = []

    # 6) 取得 items（若無則空陣列）
    items = data.get("items") or []

    # 7) 逐筆解析
    for v in items:
        # 7.1 影片 ID
        vid: Optional[str] = v.get("id")

        # 7.2 各 part 的子物件（缺漏以空 dict 代替，避免 KeyError）
        snippet: Dict[str, Any] = v.get("snippet") or {}
        content: Dict[str, Any] = v.get("contentDetails") or {}
        stats: Dict[str, Any] = v.get("statistics") or {}
        live_details: Dict[str, Any] = v.get("liveStreamingDetails") or {}
        status: Dict[str, Any] = v.get("status") or {}

        # 7.3 基本欄位
        channel_id: Optional[str] = snippet.get("channelId")
        title: str = snippet.get("title") or ""

        # 7.4 時間與時長解析
        published_at = _parse_rfc3339_to_naive_utc(snippet.get("publishedAt"))
        duration_sec = _parse_iso8601_duration_to_seconds(content.get("duration"))

        # 7.5 數值欄位（穩健轉型，避免 int(None)）
        view_count: Optional[int] = (
            int(stats["viewCount"]) if "viewCount" in stats and stats.get("viewCount") is not None else None
        )
        like_count: Optional[int] = (
            int(stats["likeCount"]) if "likeCount" in stats and stats.get("likeCount") is not None else None
        )
        comment_count: Optional[int] = (
            int(stats["commentCount"]) if "commentCount" in stats and stats.get("commentCount") is not None else None
        )

        # 7.6 組裝輸出列（暫不決定 video_type；raw parts 供後續判斷使用）
        row: Dict[str, Any] = {
            "video_id": vid,
            "channel_id": channel_id,
            "video_title": title,
            "published_at": published_at,   # naive UTC
            "duration_sec": duration_sec,
            "view_count": view_count,
            "like_count": like_count,
            "comment_count": comment_count,
            "raw": {
                "snippet": snippet,
                "contentDetails": content,
                "statistics": stats,
                "liveStreamingDetails": live_details,
                "status": status,
            },
        }

        rows.append(row)

    # 8) 回傳整批結果
    return rows

def fetch_video_meta_map(engine, video_ids: List[str]) -> Dict[str, Dict]:
    """
    從資料庫 dim_video 中查詢指定 video_ids 的部分欄位，並回傳 dict 映射：
    - key：video_id
    - value：{ "video_title": str, "published_at": datetime }

    備註：
    - 使用裸 SQL 與 IN (...) 查詢；對大量 ID 時可考慮分批以避免 SQL 長度/執行效率問題。
    - engine 須為 SQLAlchemy Engine；透過 engine.begin() 取得連線與交易範圍。
    """
    if not video_ids:
        return {}
    # 依 video_ids 數量建立對應的 %s 佔位符，供 DB-API 驅動套入參數（避免 SQL injection）
    placeholders = ",".join(["%s"] * len(video_ids))
    sql = f"""
        SELECT video_id, video_title, published_at
        FROM dim_video
        WHERE video_id IN ({placeholders})
    """
    out: Dict[str, Dict] = {}
    # 使用 engine.begin() 建立交易範圍；確保資源釋放與錯誤時正確處理
    with engine.begin() as conn:
        # 取出 DB-API 游標，以便使用 .execute + fetchall（視驅動而定）
        cur = conn.connection.cursor()
        # 傳入 video_ids 作為參數列表，與 %s 佔位符對應
        cur.execute(sql, video_ids)
        # 逐筆將查詢結果組裝為 dict
        for vid, title, published_at in cur.fetchall():
            out[vid] = {"video_title": title, "published_at": published_at}
    return out