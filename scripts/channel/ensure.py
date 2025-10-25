# scripts/channel/ensure.py
# 總覽：
# - 確保 dim_channel 表內存在指定 channel_id，若不存在則插入占位資料（避免之後外鍵或查詢失敗）。
# - 流程：先查詢 → 若存在回傳名稱 → 若不存在嘗試插入（處理競態）→ 回傳名稱（可能為 None）。
# - 依賴資料庫方言：目前使用 MySQL/MariaDB 的 ON DUPLICATE KEY UPDATE；其他方言需替換。

from typing import Optional, Dict, Any
from sqlalchemy import text

def ensure_dim_channel_exists(engine, channel_id: str) -> Optional[Dict[str, Any]]:
    """
    確保 dim_channel 表中存在指定的 channel_id。
    
    流程說明：
      1) 先以 SELECT 檢查指定 channel_id 是否存在，並取回 channel_name。
      2) 若已存在，直接回傳 {"channel_name": <已存在或可能為 None 的名稱>}。
      3) 若不存在，執行 INSERT 新增一筆記錄（以 placeholder 名稱入庫，避免空字串/NULL 導致某些約束不通過）。
         - 使用 ON DUPLICATE KEY UPDATE 處理競態（其他交易可能同時間插入），使操作為 no-op。
      4) 最終回傳 {"channel_name": None}（或 placeholder 邏輯下你可視需求改為 placeholder 值）。
    
    重要注意：
    - 交易控制：此函式在使用 Connection 物件時主動呼叫 conn.commit()，確保插入即時生效。
    - DB 方言：ON DUPLICATE KEY UPDATE 為 MySQL/MariaDB 語法；若為 PostgreSQL，請改用
      INSERT ... ON CONFLICT (channel_id) DO NOTHING 或 DO UPDATE 的等價寫法。
    - 欄位設計：若 channel_name 設定為 NOT NULL，請確保 placeholder 合規（例如空字串或 "UNKNOWN"）。
    
    參數：
    - engine: SQLAlchemy Engine（已建立好連線池與方言）。
    - channel_id: 需要確保存在於 dim_channel 的主鍵或唯一鍵。
    
    回傳：
    - dict：{"channel_name": <str 或 None>}
    - 按現行邏輯不會回傳 None（Optional 僅作型別寬鬆）。
    """
    with engine.connect() as conn:
        # 1) 查詢是否已存在該 channel_id 的資料（取回 channel_name）
        row = conn.execute(
            text("SELECT channel_name FROM dim_channel WHERE channel_id = :cid"),
            {"cid": channel_id},
        ).mappings().first()

        if row:
            # 找到既有資料：直接回傳名稱（可能為 None 或字串）
            return {"channel_name": row["channel_name"]}

        # 2) 沒找到則插入占位資料（處理競態：同一時間別的程序也可能在插入）
        # placeholder 設定策略：
        # - 若 schema 容許 NULL，可改為 None；本例用空字串以避免部分 DB/Schema 的 NOT NULL 約束。
        placeholder_name = ""
        conn.execute(
            text("""
            INSERT INTO dim_channel (channel_id, channel_name)
            VALUES (:cid, :name)
            ON DUPLICATE KEY UPDATE channel_name = channel_name
            """),
            {"cid": channel_id, "name": placeholder_name},
        )

        # 3) 顯式提交，確保 INSERT/UPSERT 立刻生效，避免之後查詢不到或外鍵依賴失敗
        conn.commit()

        # 4) 回傳 channel_name 為 None（語意上表示「目前未知」）
        #    若希望反映實際入庫值，可改為回傳 {"channel_name": placeholder_name}
        return {"channel_name": None}

# 本程式作用摘要：
# - ensure_dim_channel_exists：檢查 dim_channel 是否已有指定 channel_id，若無則安全插入占位資料（處理競態），最後回傳名稱資訊。
# - 交易與方言：函式內自行 commit，並使用 MySQL/MariaDB 的 ON DUPLICATE KEY UPDATE；若使用 PostgreSQL 需改為 ON CONFLICT。
# - 占位策略：預設以空字串做為 channel_name，占位避免 NOT NULL 約束衝突；必要時可改為 None 或 "UNKNOWN"。