# scripts/services/playlist_update.py
# 總覽：
# - run_update_playlists：一次更新三個播放清單（熱門 Shorts、熱門 VOD、近期熱門），支援 dry-run 與變更限額。
# - YouTube API 介面：列出、刪除、插入播放清單項目，內建重試與節流。
# - 輔助：時間視窗解析、名單差異計算、從設定讀取播放清單 ID、批次操作包裝與指數退避。

from __future__ import annotations

import time
import datetime as dt
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Iterable, Any, Set
from scripts.db.db import query_top_shorts, query_top_vods, query_poe327, query_new_vods, query_hot_videos
from scripts.youtube.client import get_youtube_data_client, call_with_retries
from scripts.ingestion.ya_api import build_ya_client
from scripts.services.top_videos_query import query_top_videos_from_ya

def run_update_playlists(
    channel_id: str,
    dry_run: bool = False,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    max_changes_per_playlist: Optional[int] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    一次更新三個播放清單：
      1) 最熱門 Shorts（shorts 前 20）
      2) 最熱門影片（VOD 前 10）
      3) 近期熱門（D-9 ~ D-2 期間 views 前 10，清空後依序重建）
    - 參數：
      channel_id：目標頻道 ID
      dry_run：是否僅產生計畫而不實際呼叫 YouTube API
      window_start/window_end：指定視窗（YYYY-MM-DD），未提供則預設 D-9~D-2（台北時區）
      max_changes_per_playlist：每個播放清單最大允許新增/刪除數量（None 表示不限制）
      settings：配置來源，需含三個播放清單 ID 與 API 憑證設定
    - 回傳：包含各清單的 before/target/add/remove 與操作耗時、API 計數等
    """
    started_at = time.time()

    # 0) 從 settings 讀取播放清單 ID（必要）
    pl_shorts, pl_vods, pl_recent, p1_poe327, pl_new_vods = _get_playlists_from_settings(settings)

    # 1) 決定 window（未提供則用預設 D-9~D-2）
    w_start, w_end = _resolve_window(window_start, window_end, tz="Asia/Taipei")

    # ==========================================
    # [NEW] 時間判斷邏輯 (UTC+8)
    # ==========================================
    tz_taipei = timezone(timedelta(hours=8))
    now = datetime.now(tz_taipei)
    
    # 判斷是否為每日更新時段 (00:00 <= 現在時間 < 00:30)
    # 若 dry_run=True 且想測試邏輯，可暫時註解掉時間限制，但正式環境建議保留
    do_daily_update = (now.hour == 0 and 0 <= now.minute < 30)

    print(f"[update_playlists] 頻道={channel_id} 試跑={dry_run} 時間={now.strftime('%H:%M')} 每日更新={do_daily_update}")
    
    # 2) 查詢目標名單
    # 初始化變數，避免未執行時報錯
    target_shorts, target_vods, target_recent = [], [], []
    # - 來自資料庫彙總的 Top shorts / Top VOD 名單（回傳已排序的 video_id 列表）
    # 每日更新組：只在特定時段查詢 DB
    if do_daily_update:
        target_shorts = query_top_shorts(channel_id, limit=20)
        target_vods   = query_top_vods(channel_id, limit=10)
        target_recent = query_hot_videos(channel_id, limit=10)
    
    # 常態更新組：總是查詢
    target_new_vods = query_new_vods(channel_id, limit=10)
    target_poe327   = query_poe327(channel_id)

    # 3) 取得現有播放清單內容（YouTube Data API）
    # 初始化變數
    current_shorts, current_vods, current_recent = [], [], []

    # 每日更新組：只在特定時段呼叫 API
    if do_daily_update:
        current_shorts = yt_list_playlist_video_ids(pl_shorts, settings=settings)
        current_vods   = yt_list_playlist_video_ids(pl_vods, settings=settings)
        current_recent = yt_list_playlist_video_ids(pl_recent, settings=settings)
    
    # 常態更新組：總是呼叫 API
    current_poe327   = yt_list_playlist_video_ids(p1_poe327, settings=settings)
    current_new_vods = yt_list_playlist_video_ids(pl_new_vods, settings=settings)

    # 4) 計算差異
    add_shorts, del_shorts = [], []
    add_vods, del_vods = [], []
    recent_needs_update = False

    if do_daily_update:
        add_shorts, del_shorts = _diff_sets(target_shorts, current_shorts)
        add_vods,   del_vods   = _diff_sets(target_vods, current_vods)
        # Recent 判定：內容或順序不一致才更新
        recent_needs_update = (target_recent != current_recent)
    
    # 常態組差異計算
    add_poe327,   del_poe327   = _diff_sets(target_poe327, current_poe327)
    add_new_vods, del_new_vods = _diff_sets(target_new_vods, current_new_vods)

    # 5) 依限額裁切 (僅針對有計算差異的清單)
    if max_changes_per_playlist is not None:
        if do_daily_update:
            add_shorts = list(add_shorts)[:max_changes_per_playlist]
            del_shorts = list(del_shorts)[:max_changes_per_playlist]
            add_vods   = list(add_vods)[:max_changes_per_playlist]
            del_vods   = list(del_vods)[:max_changes_per_playlist]
        
        # 常態組也要裁切
        add_poe327   = list(add_poe327)[:max_changes_per_playlist]
        del_poe327   = list(del_poe327)[:max_changes_per_playlist]
        add_new_vods = list(add_new_vods)[:max_changes_per_playlist]
        del_new_vods = list(del_new_vods)[:max_changes_per_playlist]

    # 6) 組裝計畫輸出
    # 決定 recent 的 action 標籤
    if not do_daily_update:
        recent_action = "skip_time_window"
    elif recent_needs_update:
        recent_action = "clear_and_rebuild"
    else:
        recent_action = "skip_identical"

    result = {
        "window": [w_start, w_end],
        "daily_update_triggered": do_daily_update,
        "shorts": {
            "before": current_shorts,
            "target_top20": target_shorts,
            "add": list(add_shorts),
            "remove": list(del_shorts),
            "playlist_id": pl_shorts,
            "status": "planned" if do_daily_update else "skipped_time_window"
        },
        "vods": {
            "before": current_vods,
            "target_top10": target_vods,
            "add": list(add_vods),
            "remove": list(del_vods),
            "playlist_id": pl_vods,
            "status": "planned" if do_daily_update else "skipped_time_window"
        },
        "recent": {
            "before": current_recent,
            "target_top10": target_recent,
            "action": recent_action,
            "playlist_id": pl_recent,
        },
        "metrics": {
            "api": {"list": 0, "insert": 0, "delete": 0},
            "duration_sec": None,
        },
    }

    # Log 輸出
    if do_daily_update:
        print(f"[計畫] shorts 新增={len(add_shorts)} 移除={len(del_shorts)}")
        print(f"[計畫] vods 新增={len(add_vods)} 移除={len(del_vods)}") 
        if recent_needs_update:
            print(f"[計畫] recent 重建={len(target_recent)} (內容或順序變動)") 
        else:
            print(f"[計畫] recent 略過 (內容與順序一致)")
    else:
        print(f"[計畫] Daily清單 (shorts/vods/recent) 跳過更新 (非 00:00-00:30 時段)")

    print(f"[計畫] poe327 新增={len(add_poe327)} 移除={len(del_poe327)}")
    print(f"[計畫] new_vods 新增={len(add_new_vods)} 移除={len(del_new_vods)}")

    if dry_run:
        result["metrics"]["duration_sec"] = round(time.time() - started_at, 3)
        return result

    # 7) 執行更新
    
    # --- 每日更新組 ---
    if do_daily_update:
        # Shorts
        _yt_delete_many(pl_shorts, del_shorts, settings, label="shorts")
        _yt_insert_many(pl_shorts, add_shorts, settings, label="shorts")

        # Vods
        _yt_delete_many(pl_vods, del_vods, settings, label="vods")
        _yt_insert_many(pl_vods, add_vods, settings, label="vods")

        # Recent
        if recent_needs_update:
            _yt_delete_many(pl_recent, current_recent, settings, label="recent-clear")
            _yt_insert_in_order(pl_recent, target_recent, settings, label="recent-rebuild")
        else:
            print(f"[執行] recent 內容一致，跳過更新。")

    # --- 常態更新組 (不受時間限制) ---
    _yt_delete_many(p1_poe327, del_poe327, settings, label="poe327")
    _yt_insert_many(p1_poe327, add_poe327, settings, label="poe327")

    _yt_delete_many(pl_new_vods, del_new_vods, settings, label="new_vods")
    _yt_insert_many(pl_new_vods, add_new_vods, settings, label="new_vods")

    # 8) 回填耗時
    result["metrics"]["duration_sec"] = round(time.time() - started_at, 3)
    print(f"[成功] update_playlists 執行完成，耗時 {result['metrics']['duration_sec']} 秒")
    return result


# ------------- 業務規則/工具函式 -------------

def _resolve_window(window_start: Optional[str], window_end: Optional[str], tz: str = "Asia/Taipei") -> Tuple[str, str]:
    """
    決定排行榜的時間視窗：
    - 預設使用 D-9 ~ D-2（以台北時區為準的概念視窗；此處採用系統本地日期近似）
    - 若提供 window_start/window_end（YYYY-MM-DD），則直接使用
    回傳：(start_date, end_date) 的字串元組。
    """
    if window_start and window_end:
        return window_start, window_end

    # 以本地系統日期近似台北時區日期；若需精準時區，建議引入 zoneinfo/pytz 並以當地午夜切割
    today = dt.date.today()
    start = today - dt.timedelta(days=7)
    end   = today - dt.timedelta(days=0)
    return start.isoformat(), end.isoformat()


def _diff_sets(target: List[str], current: List[str]) -> Tuple[Set[str], Set[str]]:
    """
    計算播放清單目標值與目前狀態的集合差異：
    - 回傳：(需新增的 video_id 集合, 需刪除的 video_id 集合)
    """
    tset, cset = set(target), set(current)
    return tset - cset, cset - tset


def _get_playlists_from_settings(settings: Optional[Dict[str, Any]]) -> Tuple[str, str, str]:
    """
    從 settings 讀取三個播放清單 ID，缺一不可。
    需要的 key：
      - YT_PLAYLIST_SHORTS_TOP：熱門 Shorts 播放清單 ID
      - YT_PLAYLIST_VODS_TOP：熱門 VOD 播放清單 ID
      - YT_PLAYLIST_RECENT_HOT：近期熱門播放清單 ID
    - 若缺少任一 key 或 settings 為 None，拋出錯誤以避免執行期失敗
    """
    if settings is None:
        raise ValueError("[playlist_update] settings 不可為 None，請傳入 load_settings() 的回傳結果")

    keys = [
        "YT_PLAYLIST_SHORTS_TOP",
        "YT_PLAYLIST_VODS_TOP",
        "YT_PLAYLIST_RECENT_HOT",
        "YT_PLAYLIST_POE327",
        "YT_PLAYLIST_NEWPOST",
    ]
    missing = [k for k in keys if not str(settings.get(k, "")).strip()]
    if missing:
        raise ValueError(f"[playlist_update] 缺少必要播放清單 ID：{', '.join(missing)}。請在 .env 或系統環境中設定。")

    pl_shorts = settings["YT_PLAYLIST_SHORTS_TOP"].strip()
    pl_vods   = settings["YT_PLAYLIST_VODS_TOP"].strip()
    pl_recent = settings["YT_PLAYLIST_RECENT_HOT"].strip()
    pl_poe327 = settings["YT_PLAYLIST_POE327"].strip()
    pl_new_vods = settings["YT_PLAYLIST_NEWPOST"].strip()
    return pl_shorts, pl_vods, pl_recent, pl_poe327, pl_new_vods

# ------------- YouTube API 介面（請接到你的實作） -------------

def yt_list_playlist_video_ids(playlist_id: str, settings: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    使用 OAuth 的 YouTube Data API v3 列出指定播放清單中的 videoId。
    - 以 50 筆為一頁分頁抓取，串接所有頁面
    - 內建輕量節流與 call_with_retries 包裝
    回傳：video_id 的列表（依清單目前順序）
    """
    yt = get_youtube_data_client(settings or {})
    out: List[str] = []
    page_token: Optional[str] = None

    while True:
        def _call():
            return yt.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token
            ).execute()

        resp = call_with_retries(_call, settings)
        for it in resp.get("items", []):
            vid = (it.get("contentDetails") or {}).get("videoId")
            if vid:
                out.append(vid)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.05)  # 輕量節流，降低 QPS 波動

    return out


def yt_delete_playlist_items(playlist_id: str, video_ids: Iterable[str], settings: Optional[Dict[str, Any]] = None) -> int:
    """
    依據 videoId 找到對應的 playlistItemId，逐一刪除。
    - 先建立 videoId -> playlistItemId 的對應表，再呼叫 delete
    - 針對找不到對應項目的 videoId 會略過
    回傳：刪除成功的筆數
    """
    yt = get_youtube_data_client(settings or {})
    targets: Set[str] = set(video_ids)
    if not targets:
        return 0

    # 先建立 videoId -> playlistItemId 對應
    video_to_item: Dict[str, str] = {}
    page_token: Optional[str] = None
    while True:
        def _call():
            return yt.playlistItems().list(
                part="id,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token
            ).execute()

        resp = call_with_retries(_call, settings)
        for it in resp.get("items", []):
            vid = (it.get("contentDetails") or {}).get("videoId")
            pid = it.get("id")
            if vid and pid and vid in targets and vid not in video_to_item:
                video_to_item[vid] = pid
                # 若已全部找到可提前結束
                if len(video_to_item) == len(targets):
                    break

        page_token = resp.get("nextPageToken")
        if not page_token or len(video_to_item) == len(targets):
            break

    # 執行刪除
    deleted = 0
    for vid in targets:
        pid = video_to_item.get(vid)
        if not pid:
            continue

        def _call_del():
            return yt.playlistItems().delete(id=pid).execute()

        call_with_retries(_call_del, settings)
        deleted += 1
        time.sleep(0.3)  # 溫和節流，避免 QPS 突刺

    return deleted


def yt_insert_playlist_items(playlist_id: str, video_ids: Iterable[str], settings: Optional[Dict[str, Any]] = None, ordered: bool = False) -> int:
    """
    插入指定 videoIds 到播放清單。
    - ordered=False：維持 YouTube 預設插入至清單尾端
    - ordered=True：依傳入順序以 position 指定插入順序（index 0 開頭）
    - 逐一插入並於每次呼叫之間加入節流
    回傳：新增成功的筆數
    """
    yt = get_youtube_data_client(settings or {})
    count = 0

    for idx, vid in enumerate(video_ids):
        body = {
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {"kind": "youtube#video", "videoId": vid},
            }
        }
        if ordered:
            body["snippet"]["position"] = idx  # 指定插入位置，維持與輸入順序一致

        def _call_ins():
            return yt.playlistItems().insert(part="snippet", body=body).execute()

        call_with_retries(_call_ins, settings)
        count += 1
        time.sleep(0.3)  # 溫和節流，降低 API 壓力

    return count


# ------------- YouTube API 包裝（重試/計數） -------------

def _yt_delete_many(playlist_id: str, video_ids: Iterable[str], settings: Optional[Dict[str, Any]], label: str) -> None:
    """
    批次刪除封裝：打印摘要、空集合快速返回、失敗採指數退避重試。
    - label：用於日誌區分清單種類
    """
    video_ids = list(video_ids)
    if not video_ids:
        print(f"[{label}] 無須刪除")
        return
    print(f"[{label}] 刪除數量={len(video_ids)}")
    _retry(lambda: yt_delete_playlist_items(playlist_id, video_ids, settings), op=f"{label}-delete")

def _yt_insert_many(playlist_id: str, video_ids: Iterable[str], settings: Optional[Dict[str, Any]], label: str) -> None:
    """
    批次新增封裝：打印摘要、空集合快速返回、失敗採指數退避重試。
    - 以預設 unordered 模式插入（清單尾端）
    """
    video_ids = list(video_ids)
    if not video_ids:
        print(f"[{label}] 無須新增")
        return
    print(f"[{label}] 新增數量={len(video_ids)}")
    _retry(lambda: yt_insert_playlist_items(playlist_id, video_ids, settings, ordered=False), op=f"{label}-insert")

def _yt_insert_in_order(playlist_id: str, ordered_video_ids: List[str], settings: Optional[Dict[str, Any]], label: str) -> None:
    """
    依傳入排序重建清單：空集合快速返回、失敗採指數退避重試。
    - 用於「近期熱門」清單的清空後重建
    """
    if not ordered_video_ids:
        print(f"[{label}] 無須重建")
        return
    print(f"[{label}] 重建寫入數量={len(ordered_video_ids)} (已排序)")
    _retry(lambda: yt_insert_playlist_items(playlist_id, ordered_video_ids, settings, ordered=True), op=f"{label}-rebuild-insert")

def _retry(fn, op: str, max_attempts: int = 5, base_delay: float = 1.0):
    """
    對 API 操作做指數退避重試：等待序列 1s, 2s, 4s, 8s, 16s。
    - fn：要執行的可呼叫物件
    - op：操作名稱（用於日誌）
    - max_attempts：最大嘗試次數（預設 5）
    - base_delay：初始等待秒數（預設 1.0）
    行為：
      1) 立即嘗試執行
      2) 失敗則打印警告並等待 2^(n-1) 倍的 base_delay
      3) 直至成功或達到最大次數，最後一次失敗會拋出例外
    """
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                print(f"[錯誤] {op} 在嘗試 {attempt} 次後失敗: {e}")
                raise
            delay = base_delay * (2 ** (attempt - 1))
            print(f"[警告] {op} 第 {attempt} 次嘗試失敗: {e}; 將於 {delay:.1f}秒後重試")
            time.sleep(delay)