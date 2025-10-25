# scripts/utils/dates.py
# 總覽：
# - 日期工具：validate_date_str/parse_date/to_date/today_minus 提供日期字串驗證、解析與便捷換算。
# - 資料庫查詢：get_last_ingested_day/get_channel_started_day 從資料庫取得頻道相關日期。
# - 視窗計算：compute_window 依既有資料與限制計算抓取區間；default_dates_for_window_by_offset 以相對位移回傳日期區間；valid_date_str 作為 argparse 檢核。

from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from sqlalchemy import text

# --------- 基礎工具 ----------

def validate_date_str(s: Optional[str]) -> Optional[str]:
    """
    檢查字串是否為 YYYY-MM-DD。若為 None/空字串，回傳 None；格式錯誤拋 ValueError。
    - 用途：嚴格驗證輸入是否符合 API/SQL 所需的日期格式
    - 行為：透過 datetime.strptime 嘗試解析，成功則回傳原字串，失敗拋出 ValueError
    - 邊界：None 或空字串直接視為未提供，回傳 None
    """
    if not s:
        return None
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        raise ValueError("日期格式需為 YYYY-MM-DD")

def parse_date(s: Optional[str]) -> Optional[date]:
    """
    將 YYYY-MM-DD 轉為 date；None/空字串回傳 None。無效格式回傳 None（不拋例外）。
    - 用途：寬鬆解析日期字串為 date 物件，便於後續比較/計算
    - 行為：解析失敗時回傳 None（與 validate_date_str 的拋例外形成互補）
    - 適用：不想中斷流程時（例如從 DB 讀到未知格式資料）
    """
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def to_date(obj) -> Optional[date]:
    """
    寬鬆轉換為 date：
      - 若為 date 直接回傳
      - 若為 datetime 回傳其 date()
      - 若為字串按 YYYY-MM-DD 嘗試解析
      - 其他型別回傳 None
    - 用途：將 DB 查詢結果或動態型別統一轉為 date 以便比較
    """
    if obj is None:
        return None
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return obj
    if isinstance(obj, datetime):
        return obj.date()
    if isinstance(obj, str):
        return parse_date(obj)
    return None

def today_minus(days: int = 2, min_date: Optional[date] = None) -> date:
    """
    回傳今天往回 days 天的日期。預設 today-2（因 YouTube Analytics T-1 仍在滾動）。
    若提供 min_date，則回傳值會被 clamp 至不早於 min_date。
    - 用途：快速產生相對於今天的日期，用於自然右界或預設起點
    - 行為：若計算結果早於 min_date，改回傳 min_date
    """
    d = date.today() - timedelta(days=days)
    if min_date and d < min_date:
        return min_date
    return d

# --------- DB 讀取工具 ----------

def get_last_ingested_day(engine, channel_id: str) -> Optional[date]:
    """
    查詢 fact_yta_channel_daily 中此頻道最後一筆 day。
    回傳 date 或 None。允許 engine 為 SQLAlchemy Engine。
    - 用途：決定下一次抓取的起點（通常為最後一筆的隔天）
    - SQL：SELECT MAX(day) FROM fact_yta_channel_daily WHERE channel_id = :cid
    - 回傳：將查得的標量轉成 date（容忍 None）
    """
    sql = "SELECT MAX(day) FROM fact_yta_channel_daily WHERE channel_id = :cid"
    with engine.connect() as conn:
        r = conn.execute(text(sql), {"cid": channel_id}).scalar()
        return to_date(r)

def get_channel_started_day(engine, channel_id: str) -> Optional[date]:
    """
    從 dim_channel 取得頻道建立日（若有此欄位）。
    若 dim_channel 未存，或欄位不存在/格式異常，回傳 None。
    - 用途：避免抓取起始時間早於頻道存在時間
    - SQL：SELECT started_on FROM dim_channel WHERE channel_id = :cid
    - 錯誤處理：任何例外皆吞掉並回傳 None，以不影響流程
    """
    sql = "SELECT started_on FROM dim_channel WHERE channel_id = :cid"
    with engine.connect() as conn:
        try:
            r = conn.execute(text(sql), {"cid": channel_id}).scalar()
            return to_date(r)
        except Exception:
            return None

# --------- 視窗計算 ----------

def compute_window(
    engine,
    channel_id: str,
    default_start_date: str,
    hard_end_date: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    計算本次抓取的起訖日期（YYYY-MM-DD 字串）。
    起：max(上次抓取日+1, 頻道建立日, default_start_date)
    訖：min(today-0, hard_end_date or +inf)

    備註：
    - default_start_date 需為 YYYY-MM-DD；若格式錯誤將拋 ValueError。
    - 若計算後 start > end，回傳 (None, None) 代表無需抓取。
    - 這裡只回傳字串，以方便直接餵給 API 層或 SQL。
    """
    # 驗證/解析 default_start_date 與 hard_end_date（硬右界，若提供）
    ds = validate_date_str(default_start_date)
    de = validate_date_str(hard_end_date) if hard_end_date else None

    # 轉為 date 以便比較；理論上 ds 一定有效，保底再檢查
    default_start = parse_date(ds)
    if not default_start:
        raise ValueError("default_start_date 格式錯誤，需為 YYYY-MM-DD")

    # 從資料庫取得此頻道最後一次匯入日期與頻道建立日
    last = get_last_ingested_day(engine, channel_id)  # e.g., 2025-10-10
    ch_start = get_channel_started_day(engine, channel_id)

    # 建立起始候選集合：最後匯入日+1、頻道建立日、預設起點
    start_candidates: list[date] = []
    if last:
        start_candidates.append(last + timedelta(days=1))
    if ch_start:
        start_candidates.append(ch_start)
    start_candidates.append(default_start)

    # 最終起點取候選的最大值；若候選為空（極端情況）則退而求其次 today-2
    start = max(start_candidates) if start_candidates else today_minus(2)

    # 右邊界：today-0 與 hard_end_date（二者取較小）；未給硬右界則以 today 為準
    natural_end = today_minus(0)
    end: date = natural_end
    if de:
        de_d = parse_date(de)
        if de_d:
            end = min(natural_end, de_d)

    # 若無需抓取（起點超過終點），以 (None, None) 表示
    if start > end:
        return None, None

    # 以 ISO 8601 字串回傳，便於下游使用
    return start.isoformat(), end.isoformat()

def default_dates_for_window_by_offset(from_offset: int, to_offset: int):
    """
    以相對今日的位移天數回傳日期區間字串（YYYY-MM-DD, YYYY-MM-DD）。
    - 參數：from_offset 與 to_offset 為向過去偏移的天數（可正可負）
    - 行為：today - from_offset 與 today - to_offset；若 s > e 會自動交換
    - 用途：快速指定如近7天、近30天等相對區間
    """
    today = date.today()
    s = today - timedelta(days=from_offset)
    e = today - timedelta(days=to_offset)
    if s > e:
        s, e = e, s
    return s.isoformat(), e.isoformat()

def valid_date_str(s: str) -> str:
    """
    嚴格驗證 YYYY-MM-DD 格式的輔助函式，成功回傳原字串。
    - 常用於 argparse 型 CLI 參數 type=valid_date_str
    - 解析失敗將由 datetime.strptime 拋出 ValueError
    """
    datetime.strptime(s, "%Y-%m-%d")
    return s