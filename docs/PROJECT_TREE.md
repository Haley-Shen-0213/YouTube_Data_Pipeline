==========================================================================================================================
YouTube_Data_Pipeline/
├─ credentials/
│  ├─ analytics_oauth_client_secret.json
│  ├─ analytics_oauth_token.json
│  ├─ oauth_client_secret.json
│  ├─ youtube_oauth_client_secret.json
│  └─ youtube_oauth_token.json
│  - 作用：存放 YouTube Data API 與 YouTube Analytics 的 OAuth 憑證與存取令牌，供 API 客戶端載入。
├─ docs/
│  ├─ oauth_guide.md
│  └─ PROJECT_TREE.md
│  - 作用：專案文件；oauth_guide.md 說明 OAuth 設定與授權流程；PROJECT_TREE.md 為專案結構說明。
├─ logs/
│  ├─ pipeline_20251025_120334_FAIL.log
│  ├─ pipeline_20251025_120822_OK.log
│  └─ pipeline_20251025_121637_OK.log
│  - 作用：保存每次 Pipeline 執行的摘要與詳細日誌，供通知與除錯追蹤。
├─ scripts/
│  ├─ channel/
│  │  ├─ ensure.py
│  │  │  - 作用：確保 dim_channel 內存在指定 channel_id；不存在則插入占位並 commit；處理競態（MySQL/MariaDB 使用 ON DUPLICATE KEY UPDATE）。
│  │  └─ init.py
│  │     - 作用：套件初始化（便於相對匯入）。
│  ├─ classifiers/
│  │  └─ init.py
│  │     - 作用：預留分類器模組入口（影片型別或其他分類擴充點）。
│  ├─ db/
│  │  ├─ db.py
│  │  │  - 作用：資料庫抽象層與工具。提供 Engine/Session 工廠、交易管理、通用查詢，以及 dim_video、fact_yta_channel_daily 等 upsert 與查詢。
│  │  └─ init.py
│  │     - 作用：套件初始化。
│  ├─ ingestion/
│  │  ├─ channel_daily.py
│  │  │  - 作用：頻道日次指標 ETL。流程：確保 dim_channel → 計算日期視窗 → 呼叫 YA API → 整理並 upsert 至 fact_yta_channel_daily。含頻道 ID 解析。
│  │  ├─ dim_channel.py
│  │  │  - 作用：維度表 dim_channel 的存在性保證與基本初始化（原生 SQL + commit），可擴充抓取頻道更多資訊。
│  │  ├─ init.py
│  │  └─ ya_api.py
│  │     - 作用：YouTube Analytics v2 低階客戶端與通用查詢介面（reports.query），供上層方法如 query_channel_daily 使用。
│  ├─ models/
│  │  ├─ fact_yta_channel_daily.py
│  │  │  - 作用：定義頻道日彙總事實表 ORM 模型（以 channel_id+day 為複合主鍵），支援 ETL 寫入與報表查詢。
│  │  ├─ fact_yta_video_window.py
│  │  │  - 作用：定義影片視窗彙總事實表的欄位白名單與 MySQL/MariaDB UPSERT SQL，寫入 [start_date, end_date] 視窗聚合 KPI。
│  │  └─ init.py
│  │     - 作用：套件初始化。
│  ├─ notifications/
│  │  ├─ init.py
│  │  ├─ runner.py
│  │  │  - 作用：統一重試與管線執行封裝；run_step_with_retry 執行單步驟、run_pipeline_and_notify 順序執行四步並產出摘要、寫入日誌與觸發通知。
│  │  └─ senders.py
│  │     - 作用：多通道通知（Email/LINE/Discord）。format_summary_text 組裝摘要；notify_all 統一發送並容錯。
│  ├─ services/
│  │  ├─ init.py
│  │  ├─ playlist_update.py
│  │  │  - 作用：更新三個播放清單（熱門 Shorts/VOD/近期熱門）；支援 dry-run、差異計算、批次操作、指數退避與節流。
│  │  ├─ top_videos_query.py
│  │  │  - 作用：呼叫 YouTube Analytics 回傳指定期間 Top N 影片排行，僅做查詢整理不涉及資料庫。
│  │  └─ video_ingestion.py
│  │     - 作用：抓取影片清單與詳情、更新 dim_video；取得 D-3~D-2 的 Top Videos 並 upsert 到 fact_yta_video_window；提供欄位正規化與批次 upsert。
│  ├─ utils/
│  │  ├─ dates.py
│  │  │  - 作用：日期工具與視窗計算。驗證/解析字串、today 位移、從 DB 取最後抓取日/頻道建立日、計算抓取區間。
│  │  ├─ env.py
│  │  │  - 作用：統一載入 .env/環境變數，檢核必填（CHANNEL_ID/START_DATE/END_DATE/DB_URL），提供預設（OUTPUT_DIR/LOG_DIR），錯誤時以 SystemExit 中止。
│  │  ├─ init.py
│  │  ├─ terminal.py
│  │  │  - 作用：清空終端畫面的小工具（跨平台）。
│  │  └─ tree.py
│  │     - 作用：輸出專案目錄樹到終端並寫入 docs/PROJECT_TREE.md；支援自訂輸出檔與排除目錄。
│  ├─ youtube/
│  │  ├─ client.py
│  │  │  - 作用：YouTube API/Analytics OAuth 與用戶端。解析 scopes/憑證路徑/埠、提供 with_retries/呼叫重試、建立 googleapiclient 服務、診斷憑證狀態。
│  │  ├─ init.py
│  │  ├─ playlists.py
│  │  │  - 作用：透過 uploads 播放清單遍歷 videoId，支援日期過濾與重試；提供時間界線工具與 RFC3339 解析。
│  │  └─ videos.py
│  │     - 作用：批次呼叫 videos.list 解析常用欄位；依狀態/時長/URL 決定影片型別；提供 requests 重試、時間與時長解析、DB 影片 meta 讀取。
│  ├─ cli.py
│  │  - 作用：CLI 入口（Typer）。預設 run_all，提供 ingest_channel_daily、fetch_videos、top_videos、update_playlists；整合通知與重試。
│  ├─ init.py
│  └─ run_probe.py
│     - 作用：管線探測/試跑腳本，輸出摘要到 data/probe_summary.json，驗證憑證與 API/DB 連通性。
├─ .env
│  - 作用：環境變數設定（CHANNEL_ID、DB_URL、日期等）。
├─ .env.sample
│  - 作用：範例環境設定檔。
├─ .gitignore
│  - 作用：版本控制忽略規則。
├─ CHANGELOG.md
│  - 作用：版本/變更紀錄。
└─ README.md