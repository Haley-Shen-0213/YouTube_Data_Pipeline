# scripts/models/fact_yta_video_window.py
# 總覽：
# - 本檔定義 YouTube Analytics「影片視窗彙總」事實表的欄位白名單（KNOWN_FACT_WINDOW_COLS）
#   與對應的 MySQL/MariaDB UPSERT（插入或更新）SQL（UPSERT_SQL_FACT_WINDOW）。
# - 適用場景：以 [start_date, end_date] 区間聚合單支影片的 KPI 指標（觀看數、互動、營收等）後，
#   將結果寫入 fact_yta_video_window 表，若主鍵已存在則更新最新統計與合併擴充欄位 JSON。
# - 擴充方向：可增加新指標欄位、調整 JSON 合併策略、改寫為多列批次插入、或抽象成多平台/多來源共用模型。

# 用途說明：
# 1) KNOWN_FACT_WINDOW_COLS
#    - 作為欄位白名單，利於 ETL 階段在 upsert 之前做資料過濾/驗證（只允許已知欄位進入 SQL）。
#    - 可在 ETL 中用 set 交集快速保留合法欄位，避免動態來源傳入未期望欄位。
# 2) UPSERT_SQL_FACT_WINDOW
#    - 以 INSERT ... ON DUPLICATE KEY UPDATE 進行 upsert。
#    - 主鍵假設包含 (channel_id, video_id, start_date, end_date) 或具備能唯一定位的索引（請在資料庫層設定）。
#    - 更新策略：
#       * 直屬欄位（如 views、likes、estimatedRevenue 等）以新值覆寫舊值。
#       * ext_metrics 以 JSON_MERGE_PATCH 做淺層鍵合併：新值為 NULL 時保留舊值；舊值為 NULL 時採用新值；
#         兩者皆不為 NULL 時，使用 JSON_MERGE_PATCH(ext_metrics, VALUES(ext_metrics)) 合併鍵值。
#       * updated_at = CURRENT_TIMESTAMP 以標記最後更新時間（需在資料表存在該欄位）。
#    - 適合在每日/每小時的滾動視窗聚合後寫回，確保歷史與最新資料一致。
# 後續可擴充與應用建議：
# - 欄位擴充：
#   * 新增更多指標（如平均觀看時長 avgViewDuration、點擊率 CTR、曝光 impressions 等），
#     需同步更新 KNOWN_FACT_WINDOW_COLS、INSERT 列與 VALUES 參數、UPDATE 子句。
#   * 將 ext_metrics 作為彈性延伸載體，存放平台偶發/次要指標；之後確認穩定再提升為實體欄位。
# - JSON 合併策略：
#   * 目前使用 JSON_MERGE_PATCH（淺層覆蓋鍵）。若需深層合併或累加（如數值聚合），
#     可改為儲存原子指標並在讀取層處理，或以觸發器/程序自訂合併邏輯。
# - 併發與一致性：
#   * 高併發場景可考慮以 INSERT IGNORE + 後續 UPDATE，或使用鎖/版本號（行版本 optimistic locking）。
# - 批次插入：
#   * 以 executemany 或組裝多值 VALUES 進行批次 upsert，顯著降低往返次數。
# - 多平台/多來源：
#   * 若未來擴至 TikTok/Instagram 等，可抽象表名與欄位映射，將 UPSERT 模板化以支援多模型。
# - 查詢效能：
#   * 為常用查詢（例如 by channel_id, date range, video_id）建立複合索引。
# - 數據品質：
#   * 在 ETL 注入前做欄位型別驗證與邊界檢查（日期格式、非負值、NULL 規則），並可用 KNOWN_FACT_WINDOW_COLS 過濾輸入。

KNOWN_FACT_WINDOW_COLS = {
    "channel_id","video_id","start_date","end_date",
    "video_title","video_published_at",
    "views","estimatedMinutesWatched","likes","comments","shares",
    "subscribersGained","subscribersLost","estimatedRevenue","watchTime",
    "ext_metrics",
}

UPSERT_SQL_FACT_WINDOW = """
INSERT INTO fact_yta_video_window (
  channel_id, video_id, start_date, end_date,
  video_title, video_published_at,
  views, estimatedMinutesWatched, likes, comments, shares,
  subscribersGained, subscribersLost, estimatedRevenue, watchTime,
  ext_metrics
) VALUES (
  %(channel_id)s, %(video_id)s, %(start_date)s, %(end_date)s,
  %(video_title)s, %(video_published_at)s,
  %(views)s, %(estimatedMinutesWatched)s, %(likes)s, %(comments)s, %(shares)s,
  %(subscribersGained)s, %(subscribersLost)s, %(estimatedRevenue)s, %(watchTime)s,
  %(ext_metrics)s
)
ON DUPLICATE KEY UPDATE
  video_title = VALUES(video_title),
  video_published_at = VALUES(video_published_at),
  views = VALUES(views),
  estimatedMinutesWatched = VALUES(estimatedMinutesWatched),
  likes = VALUES(likes),
  comments = VALUES(comments),
  shares = VALUES(shares),
  subscribersGained = VALUES(subscribersGained),
  subscribersLost = VALUES(subscribersLost),
  estimatedRevenue = VALUES(estimatedRevenue),
  watchTime = VALUES(watchTime),
  ext_metrics = CASE
      WHEN VALUES(ext_metrics) IS NULL THEN ext_metrics
      WHEN ext_metrics IS NULL THEN VALUES(ext_metrics)
      ELSE JSON_MERGE_PATCH(ext_metrics, VALUES(ext_metrics))
  END,
  updated_at = CURRENT_TIMESTAMP
"""