# scripts/db/db.py
# 總覽：
# - 提供資料庫連線與工作單元：Engine/Session 工廠、交易管理、通用查詢工具。
# - 實作與業務相關的專用查詢與 upsert，涵蓋 dim_video 與 fact_yta_channel_daily。
# - 封裝 SQLAlchemy 常用操作（連線、查詢、交易）以簡化上層 ingestion 程式碼維護。

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable, Mapping, Optional, Sequence, Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from scripts.utils.env import load_settings

# ------------------------
# Engine / Session 工廠
# ------------------------

_engine_singleton: Optional[Engine] = None

def get_engine() -> Engine:
    """
    以 .env/環境變數中的 DB_URL 建立並快取全域 Engine。
    - 優先回傳已建立的單例 Engine，避免重複建立連線池。
    - 若尚未建立，透過 load_settings() 取得 DB_URL，呼叫 make_engine() 建立並快取。
    """
    global _engine_singleton
    if _engine_singleton is not None:
        return _engine_singleton
    cfg = load_settings()
    db_url = cfg["DB_URL"]
    _engine_singleton = make_engine(db_url)
    return _engine_singleton


def make_engine(db_url: str) -> Engine:
    """
    建立 SQLAlchemy Engine。
    - pool_pre_ping: 啟用連線前 ping，避免死連線造成錯誤。
    - pool_recycle: 連線回收秒數，降低長連線被中斷的風險。
    - future=True: 使用 2.0 風格 API。
    """
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        future=True,
    )


def make_session_factory(engine: Engine):
    """
    以指定 Engine 建立 sessionmaker。
    - autoflush=False: 由呼叫端控制 flush 時機。
    - autocommit=False: 需明確 commit，符合交易一致性。
    - expire_on_commit=False: commit 後不使實體過期，避免再次查詢。
    """
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@contextmanager
def get_session(engine: Engine):
    """
    提供 with 區塊使用的 Session 交易管理器。
    - 進入時建立 Session；離開時成功自動 commit、例外自動 rollback；最後關閉資源。
    - 適合需要多次 ORM 操作的情境。
    """
    SessionFactory = make_session_factory(engine)
    session: Session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ------------------------
# 通用查詢
# ------------------------

def fetch_scalar(engine: Engine, sql: str, params: Optional[Mapping[str, Any]] = None) -> Any:
    """
    執行查詢並回傳第一列第一欄的純量值。
    - 適合用於 COUNT、MAX、存在性檢查等。
    - params 預設為空 dict，請使用具名參數以避免 SQL 注入。
    """
    with engine.connect() as conn:
        r: Result = conn.execute(text(sql), params or {})
        return r.scalar()


def fetch_one(engine: Engine, sql: str, params: Optional[Mapping[str, Any]] = None) -> Optional[Mapping[str, Any]]:
    """
    執行查詢並回傳單筆映射結果（dict），找不到回傳 None。
    - 使用 mappings().first() 取得鍵值對形式。
    """
    with engine.connect() as conn:
        r: Result = conn.execute(text(sql), params or {})
        row = r.mappings().first()
        return dict(row) if row else None


def fetch_all(engine: Engine, sql: str, params: Optional[Mapping[str, Any]] = None) -> Sequence[Mapping[str, Any]]:
    """
    執行查詢並回傳多筆映射結果（list[dict]）。
    - 適合查詢清單；仍建議以具名參數傳值。
    """
    with engine.connect() as conn:
        r: Result = conn.execute(text(sql), params or {})
        return [dict(row) for row in r.mappings().all()]


# ------------------------
# 專用查詢（dates 用）
# ------------------------

def get_last_ingested_day(engine: Engine, channel_id: str):
    """
    取得指定頻道最後一次成功寫入 fact_yta_channel_daily 的日期（MAX(day)）。
    - 用於計算下一次抓取的起始日。
    """
    sql = "SELECT MAX(day) FROM fact_yta_channel_daily WHERE channel_id = :cid"
    return fetch_scalar(engine, sql, {"cid": channel_id})


def get_channel_started_day(engine: Engine, channel_id: str):
    """
    取得 dim_channel 中頻道建立日（started_on）。
    - 若查詢過程拋出 SQLAlchemyError，回傳 None（容錯避免影響主流程）。
    """
    sql = "SELECT started_on FROM dim_channel WHERE channel_id = :cid"
    try:
        return fetch_scalar(engine, sql, {"cid": channel_id})
    except SQLAlchemyError:
        return None


# ------------------------
# dim_video 存取（供 video_ingestion 使用）
# ------------------------

def get_existing_videos(engine: Engine, video_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    查詢 dim_video 中既有的影片，回傳 dict: { video_id: { ...row } }
    - 動態組裝 IN 子句的具名參數，避免 SQL 注入。
    - 當 video_ids 為空時，直接回傳空 dict。
    """
    if not video_ids:
        return {}
    placeholders = ", ".join([f":v{i}" for i in range(len(video_ids))])
    params = {f"v{i}": vid for i, vid in enumerate(video_ids)}

    sql = f"""
    SELECT
      video_id,
      channel_id,
      video_title,
      published_at,
      duration_sec,
      is_short,
      shorts_check,
      video_type,
      view_count,
      like_count,
      comment_count
    FROM dim_video
    WHERE video_id IN ({placeholders})
    """
    rows = fetch_all(engine, sql, params)
    return {row["video_id"]: row for row in rows}

# dim_video upsert 相關之必備欄位集合（用於輕量驗證）
REQUIRED_FULL_FIELDS = {
    "video_id",
    "channel_id",
    "video_title",
    "published_at",
    "duration_sec",
    "is_short",
    "shorts_check",
    "video_type",
    "view_count",
    "like_count",
    "comment_count",
}

REQUIRED_STATS_FIELDS = {
    "video_id",
    "view_count",
    "like_count",
    "comment_count",
}

def _validate_fields(rows_list, required_fields, label: str):
    """
    輕量驗證：確保每一列資料包含必要欄位。
    - 僅檢查欄位存在，不檢查值的型別或非空；實際 schema 應由 DB 約束保護。
    - 失敗時拋出 ValueError 並指名缺少欄位與索引。
    """
    for i, r in enumerate(rows_list):
        missing = required_fields - set(r.keys())
        if missing:
            raise ValueError(f"{label} rows[{i}] 缺少必備欄位: {sorted(missing)}")

def upsert_dim_video_full(engine: Engine, rows: Iterable[Mapping[str, Any]]) -> int:
    """
    對 dim_video 進行完整 upsert（INSERT ... ON DUPLICATE KEY UPDATE）。
    必備欄位：
      video_id, channel_id, video_title, published_at, duration_sec,
      is_short, shorts_check, video_type, view_count, like_count, comment_count

    回傳：
      result.rowcount（受影響列數；注意：在 ON DUPLICATE KEY UPDATE 下，可能與實際 upsert 筆數不同）

    交易：
      使用 conn.begin() 明確包一個交易區塊，成功自動 commit、失敗自動 rollback。
    """
    rows_list = list(rows)
    if not rows_list:
        return 0

    _validate_fields(rows_list, REQUIRED_FULL_FIELDS, "upsert_dim_video_full")

    sql = """
    INSERT INTO dim_video (
        video_id, channel_id, video_title, published_at, duration_sec,
        is_short, shorts_check, video_type, view_count, like_count, comment_count
    ) VALUES (
        %(video_id)s, %(channel_id)s, %(video_title)s, %(published_at)s, %(duration_sec)s,
        %(is_short)s, %(shorts_check)s, %(video_type)s, %(view_count)s, %(like_count)s, %(comment_count)s
    )
    ON DUPLICATE KEY UPDATE
        channel_id = VALUES(channel_id),
        video_title = VALUES(video_title),
        published_at = VALUES(published_at),
        duration_sec = VALUES(duration_sec),
        is_short = VALUES(is_short),
        shorts_check = VALUES(shorts_check),
        video_type = VALUES(video_type),
        view_count = VALUES(view_count),
        like_count = VALUES(like_count),
        comment_count = VALUES(comment_count),
        updated_at = CURRENT_TIMESTAMP
    """

    try:
        with engine.connect() as conn:
            # 使用 context manager 管理交易，成功自動 commit，失敗自動 rollback
            with conn.begin():
                result = conn.execute(text(sql), rows_list)
                return result.rowcount
    except SQLAlchemyError as e:
        # 保留完整錯誤資訊與堆疊，方便上層記錄與告警
        raise RuntimeError(f"upsert_dim_video_full 失敗: {e}") from e


def upsert_dim_video_stats_only(engine: Engine, rows: Iterable[Mapping[str, Any]]) -> int:
    """
    僅更新 dim_video 的 view_count / like_count / comment_count 三欄。
    - 使用 UPDATE 語句，根據 video_id 定位目標列。
    - 適合僅變動統計數據的輕量更新情境。

    回傳：
      result.rowcount（受影響列數；注意：因為是 UPDATE，數字較直觀，但取決於資料是否有變更）
    """
    rows_list = list(rows)
    if not rows_list:
        return 0

    _validate_fields(rows_list, REQUIRED_STATS_FIELDS, "upsert_dim_video_stats_only")

    sql = """
    UPDATE dim_video
    SET
      view_count = :view_count,
      like_count = :like_count,
      comment_count = :comment_count,
      updated_at = CURRENT_TIMESTAMP
    WHERE video_id = :video_id
    """

    try:
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text(sql), rows_list)
                return result.rowcount
    except SQLAlchemyError as e:
        raise RuntimeError(f"upsert_dim_video_stats_only 失敗: {e}") from e
    
# ------------------------
# 既有：fact_yta_channel_daily
# ------------------------

def upsert_fact_channel_daily(engine: Engine, rows: Iterable[Mapping[str, Any]]):
    """
    寫入/更新 fact_yta_channel_daily（日次頻道指標）：
    - 以 INSERT ... ON DUPLICATE KEY UPDATE 實現 upsert，主鍵通常為 (channel_id, day)。
    - rows 可為多筆，會在單一交易中執行。
    
    回傳：
    - result.rowcount（受影響列數；ON DUPLICATE 情境下與實際筆數可能不同）
    """
    sql = """
    INSERT INTO fact_yta_channel_daily (
      channel_id, day, views, estimatedMinutesWatched, averageViewDuration, averageViewPercentage,
      likes, dislikes, comments, shares, playlistStarts, viewsPerPlaylistStart, cardClicks, cardTeaserClicks,
      subscribersGained, subscribersLost, subscribers_net
    ) VALUES (
      :channel_id, :day, :views, :estimatedMinutesWatched, :averageViewDuration, :averageViewPercentage,
      :likes, :dislikes, :comments, :shares, :playlistStarts, :viewsPerPlaylistStart, :cardClicks, :cardTeaserClicks,
      :subscribersGained, :subscribersLost, :subscribers_net
    )
    ON DUPLICATE KEY UPDATE
      views=VALUES(views),
      estimatedMinutesWatched=VALUES(estimatedMinutesWatched),
      averageViewDuration=VALUES(averageViewDuration),
      averageViewPercentage=VALUES(averageViewPercentage),
      likes=VALUES(likes),
      dislikes=VALUES(dislikes),
      comments=VALUES(comments),
      shares=VALUES(shares),
      playlistStarts=VALUES(playlistStarts),
      viewsPerPlaylistStart=VALUES(viewsPerPlaylistStart),
      cardClicks=VALUES(cardClicks),
      cardTeaserClicks=VALUES(cardTeaserClicks),
      subscribersGained=VALUES(subscribersGained),
      subscribersLost=VALUES(subscribersLost),
      subscribers_net=VALUES(subscribers_net),
      updated_at=CURRENT_TIMESTAMP
    """
    rows_list = list(rows)
    if not rows_list:
        return 0

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            result = conn.execute(text(sql), rows_list)
            trans.commit()
            return result.rowcount
        except SQLAlchemyError:
            trans.rollback()
            raise

def get_raw_cursor(conn):
    """
    取得底層 DBAPI cursor。
    - 少數需要使用特定 DBAPI 特性的情境可用（例如批次匯入、呼叫特定驅動函式）。
    - 注意：繞過 SQLAlchemy 的抽象層需自行負責相容性與資源管理。
    """
    return conn.connection.cursor()

# ============playlist_update use=======================

def query_top_shorts(channel_id: str, limit: int = 20, engine: Optional[Engine] = None) -> List[str]:
    """
    回傳指定頻道的 shorts 影片，依 view_count DESC、published_at DESC 排序的前 N 名 video_id。
    - 過濾條件：video_type = 'shorts' 或 is_short = 1（依 schema 適用其一或並存）。
    - 預設 N=20；可透過參數調整。
    """
    engine = engine or get_engine()
    sql = """
    SELECT video_id
    FROM dim_video
    WHERE channel_id = :channel_id
      AND (video_type = 'shorts' OR is_short = 1)
    ORDER BY view_count DESC, published_at DESC
    LIMIT :limit
    """
    rows = fetch_all(engine, sql, {"channel_id": channel_id, "limit": limit})
    return [r["video_id"] for r in rows]

def query_top_vods(channel_id: str, limit: int = 10, engine: Optional[Engine] = None) -> List[str]:
    """
    回傳指定頻道的 VOD（長影片），依 view_count DESC、published_at DESC 排序的前 N 名 video_id。
    - 過濾條件：video_type = 'vod'
    - 預設 N=10；可透過參數調整。
    """
    engine = engine or get_engine()
    sql = """
    SELECT video_id
    FROM dim_video
    WHERE channel_id = :channel_id
      AND video_type = 'vod'
    ORDER BY view_count DESC, published_at DESC
    LIMIT :limit
    """
    rows = fetch_all(engine, sql, {"channel_id": channel_id, "limit": limit})
    return [r["video_id"] for r in rows]

# 本程式作用摘要：
# - get_engine / make_engine / get_session：建立並管理資料庫連線與交易生命週期。
# - fetch_scalar / fetch_one / fetch_all：通用查詢輔助，簡化 SQL 執行與結果轉換。
# - get_last_ingested_day / get_channel_started_day：提供日期視窗計算所需的專用查詢。
# - get_existing_videos / upsert_dim_video_full / upsert_dim_video_stats_only：影片維度查詢與 upsert。
# - upsert_fact_channel_daily：日次頻道指標 upsert；query_top_shorts / query_top_vods：熱門影片清單。