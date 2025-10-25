# scripts/ingestion/ya_api.py
# 總覽：
# - 提供 YouTube Analytics v2 的用戶端建立與通用查詢接口（reports.query）。
# - 以低階通用方法（get_analytics_client、query_reports）支援高階便捷查詢（query_channel_daily）。
# - 與現有環境變數相容（build_ya_client），避免重複 OAuth 流程設定與參數分叉。
# - 回傳的欄位對應 fact_yta_channel_daily 等事實表的欄位，便於後續 ETL 寫入。

from typing import Iterable, Dict, Any, List, Optional
from googleapiclient.discovery import build
from scripts.run_probe import get_oauth_credentials  # 重用你已寫好的 OAuth 取得函式

# =========================
# 低階工廠與通用查詢接口
# =========================

def get_analytics_client(
    token_path: Optional[str],
    client_secret_path: Optional[str],
    scopes: Optional[List[str]],
    oauth_port: int = 0,
):
    """
    建立並回傳 YouTube Analytics v2 client。
    參數均為顯式傳入，利於測試與重用，也作為 build_ya_client 的底層實作。

    參數說明：
    - token_path: OAuth token 的儲存/讀取路徑，預設 credentials/analytics_oauth_token.json
    - client_secret_path: OAuth client secret 檔路徑，預設 credentials/analytics_oauth_client_secret.json
    - scopes: OAuth 權限範圍清單；若 None 則使用預設唯讀與營收唯讀兩個 scope
    - oauth_port: 本機 OAuth 驗證時啟動的回呼 port；0 表示隨機可用 port

    回傳：
    - googleapiclient.discovery.Resource：已授權的 youtubeAnalytics v2 客戶端
    """
    # scopes 可為 None（走預設），或使用者自定義的 List[str]
    if scopes is None:
        scopes = [
            "https://www.googleapis.com/auth/yt-analytics.readonly",
            "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
        ]
    # 透過既有的工具函式取得 OAuth 憑證，並使用 discovery.build 建立 client
    creds = get_oauth_credentials(
        scopes,
        token_path=token_path or "credentials/analytics_oauth_token.json",
        client_secret_path=client_secret_path or "credentials/analytics_oauth_client_secret.json",
        port=oauth_port or 0,
    )
    return build("youtubeAnalytics", "v2", credentials=creds)

def query_reports(
    analytics_client,
    *,
    ids: str,
    start_date: str,
    end_date: str,
    metrics: List[str] | str,
    dimensions: List[str] | str,
    sort: Optional[str] = None,
    max_results: Optional[int] = None,
    include_historical_channel_data: Optional[bool] = None,
    currency: Optional[str] = None,
) -> Dict[str, Any]:
    """
    通用的 reports 查詢封裝（對應 youtubeAnalytics.reports.query）。
    - 將 metrics、dimensions 支援 list 或逗號分隔字串，便於呼叫端彈性傳入。
    - 其餘參數按需映射至官方 API（sort, maxResults, includeHistoricalChannelData, currency）。

    參數說明：
    - analytics_client: 由 get_analytics_client/build_ya_client 建立之 client
    - ids: 目標資源識別，如 "channel==<CHANNEL_ID>"
    - start_date, end_date: 查詢日期區間（YYYY-MM-DD）
    - metrics: 指標（list 或逗號字串），如 ["views","likes"]
    - dimensions: 維度（list 或逗號字串），如 ["day"] 或 ["day","country"]
    - sort: 排序欄位（通常為維度或指標），例如 "day"
    - max_results: 最大返回筆數
    - include_historical_channel_data: 是否包含歷史頻道資料（官方參數名為 includeHistoricalChannelData）
    - currency: 貨幣代碼（若查詢營收相關指標）

    回傳：
    - dict：API 原始回傳（包含 columnHeaders、rows 等）
    """
    # 將 list/tuple 轉為逗號分隔，或原樣傳回字串；其他型別轉為字串
    def _to_csv(v):
        if v is None:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, (list, tuple)):
            return ",".join([str(x) for x in v])
        return str(v)

    # 組裝必要與可選參數，對應官方 API 命名
    params = {
        "ids": ids,
        "startDate": start_date,
        "endDate": end_date,
        "metrics": _to_csv(metrics),
        "dimensions": _to_csv(dimensions),
    }
    if sort:
        params["sort"] = sort
    if max_results is not None:
        params["maxResults"] = int(max_results)
    if include_historical_channel_data is not None:
        # 官方參數名稱使用駝峰：includeHistoricalChannelData
        params["includeHistoricalChannelData"] = bool(include_historical_channel_data)
    if currency:
        params["currency"] = currency

    # 直接呼叫官方 client 執行查詢
    return analytics_client.reports().query(**params).execute()

# =========================
# 高階便捷接口（向下重用低階）
# =========================

def build_ya_client(env: dict):
    """
    基於環境變數建立 YA 客戶端，與舊版接口相容；內部轉呼叫 get_analytics_client 避免邏輯分叉。

    讀取的環境鍵值：
    - YAAO_OAUTH_SCOPES: 逗號分隔的 scopes 字串（預設含 readonly 與 monetary.readonly）
    - YAAO_TOKEN_PATH / GOOGLE_TOKEN_PATH: OAuth token 檔案路徑（其一存在即可）
    - YAAO_CREDENTIALS_PATH / GOOGLE_CREDENTIALS_PATH: OAuth client secret 檔路徑（其一存在即可）
    - YAAO_OAUTH_PORT: 本機 OAuth 驗證回呼 port（可留空或 0 使用隨機可用 port）

    回傳：
    - 已授權的 youtubeAnalytics v2 client
    """
    # 解析 scopes 字串為陣列，並去除空白項
    scopes_str = env.get(
        "YAAO_OAUTH_SCOPES",
        "https://www.googleapis.com/auth/yt-analytics.readonly,https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    )
    scopes = [s.strip() for s in scopes_str.split(",") if s.strip()]
    # 兼容多種環境變數鍵，並提供預設路徑
    token_path = env.get("YAAO_TOKEN_PATH") or env.get("GOOGLE_TOKEN_PATH") or "credentials/analytics_oauth_token.json"
    client_path = env.get("YAAO_CREDENTIALS_PATH") or env.get("GOOGLE_CREDENTIALS_PATH") or "credentials/analytics_oauth_client_secret.json"
    port = int(env.get("YAAO_OAUTH_PORT", "0") or 0)

    # 統一走底層工廠函式建立 client
    return get_analytics_client(
        token_path=token_path,
        client_secret_path=client_path,
        scopes=scopes or None,
        oauth_port=port,
    )

def query_channel_daily(analytics, channel_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    拉取日次層級的頻道指標（維度：day），並整理為易於寫入 fact_yta_channel_daily 的結構。

    參數：
    - analytics: YA 客戶端（由 get_analytics_client/build_ya_client 取得）
    - channel_id: 目標頻道 ID
    - start_date, end_date: 查詢日期區間（YYYY-MM-DD）

    指標對應（部分與 fact_yta_channel_daily 欄位一一對應）：
    - views, estimatedMinutesWatched, averageViewDuration, averageViewPercentage,
      likes, dislikes, comments, shares, playlistStarts, viewsPerPlaylistStart,
      cardClicks, cardTeaserClicks, subscribersGained, subscribersLost

    回傳：
    - List[Dict[str, Any]]：每筆包含 day 與上述指標欄位的紀錄（欄位名與表欄位一致）
    """
    # 定義欲拉取的 metrics 清單，與事實表欄位對齊，便於後續直接 upsert
    metrics = [
        "views",
        "estimatedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
        "likes",
        "dislikes",
        "comments",
        "shares",
        "playlistStarts",
        "viewsPerPlaylistStart",
        "cardClicks",
        "cardTeaserClicks",
        "subscribersGained",
        "subscribersLost",
    ]
    # 呼叫通用查詢接口，維度為 day，按日排序，限制最大筆數（2000）
    resp = query_reports(
        analytics_client=analytics,
        ids=f"channel=={channel_id}",
        start_date=start_date,
        end_date=end_date,
        metrics=metrics,
        dimensions=["day"],
        sort="day",
        max_results=2000,
    )

    # 解析回傳表頭（columnHeaders）為欄位名稱，對應 rows 的順序
    cols = [h["name"] for h in resp.get("columnHeaders", [])]
    rows = resp.get("rows", []) or []

    # 將每一列轉為 dict，並且挑出與事實表對應的欄位，加入 day
    out: List[Dict[str, Any]] = []
    for r in rows:
        rec = dict(zip(cols, r))
        out.append({
            "day": rec.get("day"),
            "views": rec.get("views"),
            "estimatedMinutesWatched": rec.get("estimatedMinutesWatched"),
            "averageViewDuration": rec.get("averageViewDuration"),
            "averageViewPercentage": rec.get("averageViewPercentage"),
            "likes": rec.get("likes"),
            "dislikes": rec.get("dislikes"),
            "comments": rec.get("comments"),
            "shares": rec.get("shares"),
            "playlistStarts": rec.get("playlistStarts"),
            "viewsPerPlaylistStart": rec.get("viewsPerPlaylistStart"),
            "cardClicks": rec.get("cardClicks"),
            "cardTeaserClicks": rec.get("cardTeaserClicks"),
            "subscribersGained": rec.get("subscribersGained"),
            "subscribersLost": rec.get("subscribersLost"),
        })
    return out

# 本程式作用摘要：
# - get_analytics_client：以顯式參數建立 YA v2 client，統一 OAuth 與 build 流程。
# - query_reports：通用封裝 reports.query，處理參數轉換與選填項。
# - build_ya_client：從環境變數解析設定，向下呼叫 get_analytics_client（相容舊介面）。
# - query_channel_daily：拉取頻道日次指標（維度 day），輸出貼合事實表寫入的結構。