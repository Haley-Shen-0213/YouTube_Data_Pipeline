# scripts/models/fact_yta_channel_daily.py
# 總覽：
# - 定義 YouTube Analytics 的「頻道日彙總」事實表 ORM 模型 FactYtaChannelDaily（SQLAlchemy Declarative）。
# - 以 (channel_id, day) 作為複合主鍵，存放每日聚合的觀看、互動、訂閱變化等指標。
# - 適用於 ETL 寫入與下游查詢報表；created_at/updated_at 由資料庫自動維護。
# - 可作為事實表來源供 Looker/Metabase/BI 報表直接查詢與可視化。

from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import String, Date, BigInteger, Integer, Numeric, TIMESTAMP, text

# 建立 Declarative Base，作為所有 ORM 模型的基底
Base = declarative_base()

class FactYtaChannelDaily(Base):
    """
    FactYtaChannelDaily：YouTube 頻道的每日彙總事實表
    - 主鍵：channel_id + day（複合主鍵），唯一標識某頻道某一天的統計。
    - 欄位：包含觀看數、觀看時長、互動（讚、踩、留言、分享）、播放清單互動、卡片互動、訂閱增減等。
    - 時戳：created_at/updated_at 由資料庫端以 CURRENT_TIMESTAMP 自動設定與更新。
    - 用途：ETL 寫入每日聚合結果，下游用於 KPI 趨勢分析與報表。
    """
    __tablename__ = "fact_yta_channel_daily"

    # 頻道 ID（字串，長度上限 64），作為複合主鍵的一部分
    channel_id: Mapped[str] = mapped_column(String(64), primary_key=True)

    # 指標日期（Date），作為複合主鍵的一部分，代表此列資料的統計日期
    day: Mapped[Date] = mapped_column(Date, primary_key=True)

    # 觀看次數（累計整數），可能為 None（來源缺值）
    views: Mapped[int | None] = mapped_column(BigInteger)

    # 估計觀看分鐘數（累計整數），可能為 None
    estimatedMinutesWatched: Mapped[int | None] = mapped_column(BigInteger)

    # 平均觀看時長（秒，整數），可能為 None
    averageViewDuration: Mapped[int | None] = mapped_column(Integer)

    # 平均觀看百分比（0~100，保留 3 位小數），可能為 None
    averageViewPercentage: Mapped[float | None] = mapped_column(Numeric(6,3))

    # 喜歡數（整數），可能為 None
    likes: Mapped[int | None] = mapped_column(BigInteger)

    # 不喜歡數（整數），可能為 None
    dislikes: Mapped[int | None] = mapped_column(BigInteger)

    # 留言數（整數），可能為 None
    comments: Mapped[int | None] = mapped_column(BigInteger)

    # 分享數（整數），可能為 None
    shares: Mapped[int | None] = mapped_column(BigInteger)

    # 播放清單啟動次數（整數），可能為 None
    playlistStarts: Mapped[int | None] = mapped_column(BigInteger)

    # 每次播放清單啟動的平均觀看數（比率型，保留 6 位小數），可能為 None
    viewsPerPlaylistStart: Mapped[float | None] = mapped_column(Numeric(12,6))

    # 卡片點擊數（整數），可能為 None
    cardClicks: Mapped[int | None] = mapped_column(BigInteger)

    # 卡片預告點擊數（整數），可能為 None
    cardTeaserClicks: Mapped[int | None] = mapped_column(BigInteger)

    # 訂閱者增加數（整數），可能為 None
    subscribersGained: Mapped[int | None] = mapped_column(BigInteger)

    # 訂閱者流失數（整數），可能為 None
    subscribersLost: Mapped[int | None] = mapped_column(BigInteger)

    # 訂閱者淨變動（整數），預設為 0；可在 ETL 端以 gained - lost 計算後寫入
    subscribers_net: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))

    # 建立時間（由資料庫自動填入 CURRENT_TIMESTAMP）
    created_at: Mapped[str] = mapped_column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    # 更新時間（由資料庫在更新時自動設定 CURRENT_TIMESTAMP）
    updated_at: Mapped[str] = mapped_column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

# 本程式作用摘要：
# - 宣告 SQLAlchemy 的 Base，並定義 FactYtaChannelDaily ORM 模型與其欄位與型別。
# - 以 (channel_id, day) 為複合主鍵，承載每日彙總的觀看、互動與訂閱指標。
# - 時戳欄位由資料庫端維護，方便審計與增量同步。
# - 作為 ETL 寫入與 BI 報表查詢的核心事實表結構。