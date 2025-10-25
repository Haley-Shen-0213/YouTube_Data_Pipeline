# scripts/services/top_videos_query.py
# 總覽：
# - query_top_videos_from_ya：呼叫 YouTube Analytics 取得指定期間的 Top N 影片，支援回傳純 video_id 或含指標的詳細列表。
# - 僅做查詢與結果整理，不涉及資料庫存取或 upsert。
# - 具備排序指標檢核與可選收入欄位 include_revenue。

from typing import List, Dict, Any, Optional, Tuple
from scripts.ingestion.ya_api import query_reports

def query_top_videos_from_ya(
    analytics_client,
    channel_id: str,
    start_date: str,
    end_date: str,
    metric: str = "views",
    top_n: int = 10,
    include_revenue: bool = False,
    return_with_metrics: bool = False,
) -> List[str] | List[Dict[str, Any]]:
    """
    以 YouTube Analytics API 讀取 w_start ~ w_end 的 Top N 影片（依 metric 排序，預設 views）。
    - 僅讀取 YA 報表，不連 DB、不 upsert。
    - 回傳：
      - 預設：List[str]（video_id 列表，已依 metric desc 排序）
      - 若 return_with_metrics=True：List[dict]，包含 video_id 與指標值
    參數說明：
    - analytics_client：已授權的 YA API client
    - channel_id：頻道 ID（作為 ids=channel=={channel_id}）
    - start_date/end_date：查詢期間（YYYY-MM-DD）
    - metric：排序依據（限於 base_metrics 範圍）
    - top_n：取前 N 筆
    - include_revenue：是否把 estimatedRevenue 納入 metrics
    - return_with_metrics：是否回傳每支影片的指標明細
    """
    # 1) 準備 metrics 欄位（基本指標 + 可選收入）
    base_metrics = ["views", "estimatedMinutesWatched", "likes", "comments", "shares"]
    if include_revenue:
        base_metrics.append("estimatedRevenue")

    # 排序欄位檢核（需存在於 metrics 才能排序）
    if metric not in base_metrics:
        raise ValueError(f"不支援的排序指標：{metric}，可用：{', '.join(base_metrics)}")

    # 2) 查 YA 報表（以 video 為維度，依指定 metric 由高到低排序）
    ids = f"channel=={channel_id}"
    resp = query_reports(
        analytics_client=analytics_client,
        ids=ids,
        start_date=start_date,
        end_date=end_date,
        metrics=base_metrics,
        dimensions=["video"],
        sort=f"-{metric}",
        max_results=top_n,
    )

    # 解析回傳表格結構：headers 與 rows
    rows = resp.get("rows") or []
    headers = [h.get("name") for h in (resp.get("columnHeaders") or [])]
    # 常見欄位：video, views, estimatedMinutesWatched, likes, comments, shares, (optional) estimatedRevenue
    idx = {h: i for i, h in enumerate(headers or [])}

    def _get(row, col):
        """
        安全取值：從單列（list）中依欄名取得值。
        - 若欄位不存在或超界，回傳 None
        """
        i = idx.get(col)
        return row[i] if i is not None and i < len(row) else None

    # 若無資料，回傳空列表
    if not rows:
        return []

    # 預設僅回傳已排序的 video_id 列表（YA 已依 metric desc 排序）
    if not return_with_metrics:
        return [_get(r, "video") for r in rows if _get(r, "video")]

    # 回傳包含主要指標的 dict 列表（保留 base_metrics 的欄位）
    out: List[Dict[str, Any]] = []
    for r in rows:
        video_id = _get(r, "video")
        if not video_id:
            continue
        rec: Dict[str, Any] = {"video_id": video_id}
        for m in base_metrics:
            rec[m] = _get(r, m)
        out.append(rec)
    return out