# scripts/youtube/playlists.py
# 總覽：
# - _get_uploads_playlist_id：查詢頻道的 uploads 播放清單 ID（channels.list -> contentDetails.relatedPlaylists.uploads）
# - fetch_channel_video_ids：以 uploads 清單遍歷 playlistItems，依日期區間過濾回傳 videoId 列表
# - 工具函式：_get_with_retry（含退避重試）、_rfc3339_day_start/_end（日界線轉換）、_parse_rfc3339（轉 naive UTC）

import time
from typing import List, Optional, Dict, Any, Iterable
import requests
from datetime import datetime, timezone

# YouTube Data API v3 的 base URL
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# 視為可重試的狀態碼（429: rate limit, 403: 某些暫時限制, 5xx: 伺服器錯誤）
RETRY_STATUS = {429, 403, 500, 502, 503, 504}

def _rfc3339_day_start(day_str: str) -> str:
    """
    將 YYYY-MM-DD 轉為該日 UTC 起始時間的 RFC3339 字串：YYYY-MM-DDT00:00:00Z
    - day_str：日期字串（例：'2025-01-31'）
    - 回傳：'2025-01-31T00:00:00Z'
    """
    # 先以 UTC 時區建立 aware datetime，再格式化輸出 RFC3339 字串
    dt = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _rfc3339_day_end(day_str: str) -> str:
    """
    將 YYYY-MM-DD 轉為該日 UTC 結束時間的 RFC3339 字串：YYYY-MM-DDT23:59:59Z
    - day_str：日期字串（例：'2025-01-31'）
    - 回傳：'2025-01-31T23:59:59Z'
    """
    dt = datetime.strptime(day_str, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_rfc3339(s: str) -> datetime:
    """
    將 RFC3339/ISO-8601 字串解析為 naive UTC datetime：
    - 輸入例：'2025-01-31T12:34:56Z' 或 '2025-01-31T12:34:56+00:00'
    - 流程：先解析為 aware，再轉為 UTC，最後去除 tzinfo 成為 naive UTC
    - 好處：便於與其他以 naive UTC 表示的時間做大小比較
    """
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def _get_with_retry(url: str, params: Dict[str, Any], max_retries: int = 5, backoff_base: float = 0.8) -> Dict[str, Any]:
    """
    封裝 requests.get，於特定狀態碼（RETRY_STATUS）時進行指數退避重試。
    - max_retries：最大嘗試次數（包含首次），此實作會跑最多 max_retries 次 requests
    - backoff：等待秒數 = (backoff_base ** i) * 2 + i*0.1（隨 i 遞增）
    - 成功回傳：resp.json() 字典
    - 失敗策略：遇到非可重試狀態碼直接 raise_for_status；重試用盡則丟 RuntimeError
    """
    last_err = None
    for i in range(max_retries):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in RETRY_STATUS:
            # 指數退避：嘗試次數越多，等待越久；外加少量線性項以避免完全一致
            sleep_s = (backoff_base ** i) * 2 + (i * 0.1)
            time.sleep(sleep_s)
            last_err = (resp.status_code, resp.text)
            continue
        # 其他錯誤直接拋出（例如 400/404 等用戶端錯誤）
        resp.raise_for_status()
    # 用盡重試仍失敗，將最後一次錯誤資訊納入訊息
    raise RuntimeError(f"GET {url} failed after retries: {last_err}")

def _get_uploads_playlist_id(yt_api_key: str, channel_id: str) -> str:
    """
    從 channels.list 查詢指定 channel 的 uploads 播放清單 ID（relatedPlaylists.uploads）。
    - yt_api_key：API Key
    - channel_id：目標頻道（UC 開頭）
    回傳：
    - uploads 播放清單 ID（通常 UU 開頭）
    例外：
    - 查無頻道或缺 uploads 欄位時，拋出 ValueError（利於明確處理）
    """
    # 端點：channels.list
    url = f"{YOUTUBE_API_BASE}/channels"
    # 僅需 contentDetails 才拿得到 relatedPlaylists.uploads
    params = {
        "part": "contentDetails",
        "id": channel_id,
        "key": yt_api_key,
        "maxResults": 1,
    }
    # 執行請求（含重試）
    data: Dict[str, Any] = _get_with_retry(url, params)

    # 主體資料在 items；若空，代表查無該頻道或權限問題
    items = data.get("items") or []
    if not items:
        raise ValueError(f"channel not found: {channel_id}")

    # 逐層取得 uploads 播放清單 ID
    uploads = (
        items[0].get("contentDetails") or {}
    ).get("relatedPlaylists", {}).get("uploads")

    # 未取得 uploads，屬異常情況
    if not uploads:
        raise ValueError(f"uploads playlist not found for channel: {channel_id}")

    return uploads

def fetch_channel_video_ids(
    yt_api_key: str,
    channel_id: str,
    published_after: Optional[str],
    published_before: Optional[str],
) -> List[str]:
    """
    以頻道的 uploads 播放清單為資料來源，列舉指定日期區間內的所有 videoId。
    - 日期過濾依據：playlistItems.snippet.publishedAt（加入 uploads 的時間，通常等於影片發布時間）
    - 區間策略：after 使用當日 00:00:00Z（含），before 使用當日 23:59:59Z（含）；任一端為 None 表示不限制

    參數：
    - yt_api_key：API Key
    - channel_id：頻道 ID
    - published_after：起日（YYYY-MM-DD 或 None）
    - published_before：迄日（YYYY-MM-DD 或 None）

    回傳：
    - 符合條件的 videoId 字串列表（依分頁完整遍歷）
    """
    # 先取得該頻道的 uploads 播放清單 ID
    uploads_id = _get_uploads_playlist_id(yt_api_key, channel_id)

    # 將 YYYY-MM-DD 轉為 RFC3339 字串，再解析為 naive UTC datetime 以利比較
    after_dt = _parse_rfc3339(_rfc3339_day_start(published_after)) if published_after else None
    before_dt = _parse_rfc3339(_rfc3339_day_end(published_before)) if published_before else None

    # playlistItems.list 端點：抓取 videoId 與 publishedAt（過濾用）
    url = f"{YOUTUBE_API_BASE}/playlistItems"
    params = {
        "part": "contentDetails,snippet",
        "playlistId": uploads_id,
        "key": yt_api_key,
        "maxResults": 50,  # 單頁最大值
    }

    ids: List[str] = []   # 收集通過過濾的 videoId
    page_count = 0        # 計數翻頁次數（可作為除錯或監控指標）

    # 逐頁拉取 uploads 清單
    while True:
        data = _get_with_retry(url, params)
        items = data.get("items") or []

        for it in items:
            cd = it.get("contentDetails") or {}
            sn = it.get("snippet") or {}

            vid = cd.get("videoId")
            pub = sn.get("publishedAt")  # RFC3339

            # 缺 videoId 的項目直接跳過（極少見）
            if not vid:
                continue

            # 若有設定區間邊界，則依 publishedAt 做時間過濾
            if (after_dt or before_dt) and pub:
                pub_dt = _parse_rfc3339(pub)

                # 起始邊界：pub_dt < after_dt 則排除
                if after_dt and pub_dt < after_dt:
                    continue
                # 結束邊界：pub_dt > before_dt 則排除
                if before_dt and pub_dt > before_dt:
                    continue

            # 通過過濾，加入結果
            ids.append(vid)

        # 處理分頁：有 nextPageToken 則續抓，否則結束
        page_token = data.get("nextPageToken")
        if not page_token:
            break

        params["pageToken"] = page_token
        page_count += 1

        # 輕量節流：避免突發高 QPS 觸發限速；如有更嚴 QPS 管理可提高此值
        time.sleep(0.05)

    return ids