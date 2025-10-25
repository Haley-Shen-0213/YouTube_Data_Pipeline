# scripts/ingestion/dim_channel.py
# 總覽：
# - 提供維度表 dim_channel 的存在性保證：若無指定 channel_id，則建立最小必要紀錄。
# - 採用原生 SQL（text）與 Engine.connect 進行查詢與插入，並在插入後提交交易。
# - 可擴充接入 YouTube Data API 拉取頻道資訊（title、publishedAt）作為更多欄位初始化。

from sqlalchemy import text

def ensure_dim_channel(engine, channel_id: str):
    """
    若 dim_channel 無此 channel_id，則建立一筆最小必要資料。
    可擴充：呼叫 YouTube Data API 取得 title、publishedAt 當作 started_on。
    
    參數：
    - engine: SQLAlchemy Engine，用於連線與執行 SQL。
    - channel_id: 目標頻道 ID（主鍵或唯一鍵）。

    行為：
    - 先以 SELECT 1 判斷是否存在；若不存在則 INSERT 一筆僅含 channel_id 的紀錄並 commit。
    """
    # 查詢是否存在指定頻道 ID 的紀錄（僅回傳一行一列數值）
    sql_exists = "SELECT 1 FROM dim_channel WHERE channel_id = :cid LIMIT 1"
    # 若不存在時的最小插入語句（僅 channel_id 欄位）
    sql_insert = "INSERT INTO dim_channel (channel_id) VALUES (:cid)"

    # 以上下文管理器開啟連線，確保使用後自動關閉
    with engine.connect() as conn:
        # 執行存在性查詢，並以 scalar() 取得第一欄位的純量值（None 表示不存在）
        r = conn.execute(text(sql_exists), {"cid": channel_id}).scalar()
        # 若不存在，插入一筆並提交交易
        if not r:
            conn.execute(text(sql_insert), {"cid": channel_id})
            conn.commit()

# 本程式作用摘要：
# - ensure_dim_channel：檢查 dim_channel 是否已有指定 channel_id；若無則插入最小紀錄並提交。
# - 使用參數化查詢避免 SQL 注入，並以 LIMIT 1 提升存在性檢查效能。
# - 後續可延伸：補齊名稱、建立時間等欄位，或改用 ORM upsert 以處理競態。