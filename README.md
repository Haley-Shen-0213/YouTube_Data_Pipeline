===== README.md =====
# YouTube Data Pipeline (Python 3.13)

以 YouTube Data API v3 抓取頻道/影片/留言等公開資料（API Key），並可選擇透過 YouTube Analytics API（OAuth）取得營運指標；同時支援 YouTube Data API 的 OAuth（YDAO）以進行寫入/管理操作。提供 CLI、型別註解、基礎測試與 CI，協助快速將結構化資料落地到 MySQL。

- 資料來源：
  - YouTube Data API v3（API Key；亦支援 OAuth 寫入/管理）
  - YouTube Analytics API（OAuth）
- 功能：頻道/影片/留言抓取、影片時長解析、日指標彙整、MySQL 寫入
- 工具：CLI（probe/fetch）、ruff、mypy、pytest、GitHub Actions CI
- 版本註記：本文件對應 版本 0.1.0（首版發布，2025/10/25 12:00）

[2025/10/25 21:30 更新 版本 0.1.1]
- 新增：CLI 入口 scripts/cli.py（Typer），提供下列指令：
  - run_all（預設）、ingest_channel_daily、fetch_videos、top_videos、update_playlists
- 新增：通知與重試模組 scripts/notifications/{runner.py, senders.py}
  - run_pipeline_and_notify 彙整四步驟執行、寫入 logs 與多通道通知（Email/LINE/Discord）
- 新增：YouTube API/Analytics 客戶端與 ETL
  - scripts/youtube/{client.py, playlists.py, videos.py}
  - scripts/ingestion/{ya_api.py, channel_daily.py, dim_channel.py}
- 新增：影片資料匯入與排行/清單
  - scripts/services/{video_ingestion.py, top_videos_query.py, playlist_update.py}
- 新增：資料庫模型與 UPSERT
  - scripts/models/{fact_yta_channel_daily.py, fact_yta_video_window.py}
- 新增：公用工具 scripts/utils/{env.py, dates.py, terminal.py, tree.py}
- 新增：probe 腳本 scripts/run_probe.py、探測輸出 data/probe_summary.json 與 logs/* 執行記錄
- 新增：文件 docs/PROJECT_TREE.md 與 OAuth 指南 docs/oauth_guide.md
- 環境參數新增：
  - PLAYLIST_UPDATE_BATCH, PLAYLIST_UPDATE_COOLDOWN_SEC, NOTIFY_SENDERS
  - LOG_DIR/OUTPUT_DIR 會新增管線摘要與探測輸出
- 說明補充：在「快速開始」、「設定」、「驗證與健檢」與「使用方法（CLI）」各節補充 0.1.1 新指令與輸出位置
- 版本：更新至 0.1.1

[2025/10/25 22:30 更新 版本 0.1.1.1]
- 版本：更新至 0.1.1.1 修正不能直接啟動主程式的錯誤
## 目錄
- 快速開始
- 系統需求
- 安裝
- 設定（.env 與認證）
  - YouTube Data API v3（YDAO：OAuth 寫入/管理；YPKG：API Key 公開只讀）
  - YouTube Analytics API（YAAO：OAuth）
  - 資料庫（MySQL）
- 驗證與健檢（Probes）
- 使用方法（CLI）
- 資料庫說明（MySQL）
- 專案結構
- 開發與測試
- 版本與變更紀錄

## 快速開始
1) 建立虛擬環境並安裝套件
- pip: pip install -e .
2) 建立 .env（參考 .env.example）
3) 執行健檢
- 僅 Data API（公開只讀）：python scripts/run_probe.py --check data
- 僅 Analytics API（OAuth）：python scripts/run_probe.py --check analytics
- 只檢查資料庫：python scripts/run_probe.py --check db
- 全部：python scripts/run_probe.py --check all

[新增於 0.1.1]
4) 直接以 CLI 跑完整管線與通知
- 預設（run_all）：python scripts/cli.py
- 指定子任務：
  - 日指標：python scripts/cli.py ingest_channel_daily --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
  - 影片匯入：python scripts/cli.py fetch_videos --channel-id $CHANNEL_ID
  - 取熱門：python scripts/cli.py top_videos --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
  - 更新清單：python scripts/cli.py update_playlists --dry-run

## 系統需求
- Python 3.13
- MySQL 8.x
- 建議使用 venv/uv/conda

## 安裝
- pip install -e .
- （可選）pre-commit: pip install pre-commit && pre-commit install

## 設定（.env 與認證）
本專案支援三種憑證機制，請依需求配置，可同時啟用。

### 基本環境
- ENV=local
- LOG_LEVEL=INFO
- TZ=UTC

### 1) YouTube Data API v3
支援兩種模式：
- YPKG（API Key，公開只讀）
- YDAO（OAuth，寫入/管理；亦可讀取私有資料）

YPKG — 公開只讀（選配）
- 變數：
  - YPKG_API_KEY=（你的 API Key）
  - YPKG_ENABLE=true
  - YPKG_PREF_ONLY_PUBLIC=false
- 用途：僅限搜尋/影片/頻道等公開資料端點。不可進行寫入或讀取私有資料。
- 安全：請在 GCP 為 API Key 設 IP/HTTP 來源限制，避免外洩與濫用。

YDAO — OAuth（寫入/管理）
- 變數：
  - YDAO_OAUTH_SCOPES=https://www.googleapis.com/auth/youtube,https://www.googleapis.com/auth/youtube.force-ssl
  - YDAO_OAUTH_PORT=8081
  - YDAO_CREDENTIALS_PATH=credentials/youtube_oauth_client_secret.json
  - YDAO_TOKEN_PATH=credentials/youtube_oauth_token.json
- 用途：維護播放清單、上傳、訂閱管理或讀取私有資源等需要權限之操作。

### 2) YouTube Analytics API（YAAO：OAuth）
- 變數：
  - YAAO_OAUTH_SCOPES=https://www.googleapis.com/auth/yt-analytics.readonly,https://www.googleapis.com/auth/yt-analytics-monetary.readonly
  - YAAO_OAUTH_PORT=8082
  - YAAO_CREDENTIALS_PATH=credentials/analytics_oauth_client_secret.json
  - YAAO_TOKEN_PATH=credentials/analytics_oauth_token.json
- 用途：取得 views、watch time、營收、來源分析等營運指標（Analytics scope 為只讀）。

### 3) 其他行為參數
- REQUEST_TIMEOUT_SEC=30
- RETRY_MAX=5
- RETRY_BACKOFF_BASE=1.5
- MAX_RESULTS=50
- SLICE_DAYS=7

[新增於 0.1.1]
- PLAYLIST_UPDATE_BATCH=50
- PLAYLIST_UPDATE_COOLDOWN_SEC=2
- NOTIFY_SENDERS=discord,email,line
- LOG_DIR=logs（仍可覆寫；新增通知與 runner 會寫入管線摘要）
- OUTPUT_DIR=data（新增 probe_summary.json 產出位置）

### 4) 基本輸出與資料庫
- OUTPUT_DIR=data
- LOG_DIR=logs
- DB_URL=mysql+pymysql://user:password@localhost:3306/DB_NAME?charset=utf8mb4

### 5) 範例頻道與日期
- CHANNEL_ID=UCmsqsWmkuJcZzU_NcvN9YVA
- START_DATE=2025-01-01
- END_DATE=2025-01-31

### 憑證檔放置位置（建議命名）
- credentials/youtube_oauth_client_secret.json（Data OAuth 用）
- credentials/youtube_oauth_token.json（Data OAuth 產生）
- credentials/analytics_oauth_client_secret.json（Analytics OAuth 用）
- credentials/analytics_oauth_token.json（Analytics OAuth 產生）

更多細節詳見：docs/oauth_guide.md

## 驗證與健檢（Probes）
- Data API（公開只讀）健檢：驗證 API Key 可用性與搜尋列表
  - python scripts/run_probe.py --check data --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
- Analytics API 健檢：驗證 OAuth 與最小報表（例：views）
  - python scripts/run_probe.py --check analytics --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
- 資料庫連線檢查
  - python scripts/run_probe.py --check db
- 全部檢查
  - python scripts/run_probe.py --check all

Probe 輸出：
- 於 logs/ 產生 .log 與 .json，利於 CI 與人工判讀。

[新增於 0.1.1]
- run_probe.py 會於 data/probe_summary.json 寫入探測摘要，並在 logs/ 生成 pipeline_YYYYMMDD_hhmmss_*.log
- 若啟用通知（NOTIFY_SENDERS），probe 與管線摘要會透過 Email/LINE/Discord 推送（容錯不中斷主流程）

## 使用方法（CLI）
- 公開資料抓取：python -m src.cli.fetch public --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
- Analytics 指標抓取：python -m src.cli.fetch analytics --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
- 撰寫/管理操作（需 YDAO OAuth）：python -m src.cli.manage playlist --action upsert --video-id ...

註：實際指令以專案內 CLI 實作為準。

[新增於 0.1.1]
- 新 CLI（scripts/cli.py）示例與參數對齊：
  - 預設整體管線與通知：
    - python scripts/cli.py
    - 行為：依序執行影片匯入、日指標彙整、熱門影片查詢、播放清單更新，並於結束推送通知
  - 子任務：
    - 日指標匯入：python scripts/cli.py ingest_channel_daily --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
    - 影片匯入：python scripts/cli.py fetch_videos --channel-id $CHANNEL_ID
    - 熱門影片查詢：python scripts/cli.py top_videos --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE --limit 20
    - 播放清單更新：python scripts/cli.py update_playlists --dry-run
  - 通知行為：
    - 依 NOTIFY_SENDERS=discord,email,line 推送；失敗不影響主流程
  - 輸出位置：
    - logs/pipeline_*.log、data/probe_summary.json、data/* 匯出資料

## 資料庫說明（MySQL）
- 維度表＋事實表：dim_channel、dim_video、dim_date、fact_yta_daily、fact_yta_channel_daily、fact_yta_revenue、fact_yta_traffic_source_agg、view_fact_yta_channel_daily_clean
- 建議型別：百分比/比率 DECIMAL(6,3)，營收 DECIMAL(10,4)+
- 依需求建立索引，詳見 docs/schema.md

[新增於 0.1.1]
- 新增表與寫入邏輯：
  - fact_yta_channel_daily（頻道日指標，複合鍵：channel_id+day）
  - fact_yta_video_window（影片視窗彙總，UPSERT SQL 內建）
- DB 寫入策略：
  - MySQL/MariaDB 使用 ON DUPLICATE KEY UPDATE
  - 交易與批次 upsert，避免重複與鎖表

## 專案結構
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
│  │  └─ __init__.py
│  │     - 作用：套件初始化（便於相對匯入）。
│  ├─ classifiers/
│  │  └─ __init__.py
│  │     - 作用：預留分類器模組入口（影片型別或其他分類擴充點）。
│  ├─ db/
│  │  ├─ db.py
│  │  │  - 作用：資料庫抽象層與工具。提供 Engine/Session 工廠、交易管理、通用查詢，以及 dim_video、fact_yta_channel_daily 等 upsert 與查詢。
│  │  └─ __init__.py
│  │     - 作用：套件初始化。
│  ├─ ingestion/
│  │  ├─ channel_daily.py
│  │  │  - 作用：頻道日次指標 ETL。流程：確保 dim_channel → 計算日期視窗 → 呼叫 YA API → 整理並 upsert 至 fact_yta_channel_daily。含頻道 ID 解析。
│  │  ├─ dim_channel.py
│  │  │  - 作用：維度表 dim_channel 的存在性保證與基本初始化（原生 SQL + commit），可擴充抓取頻道更多資訊。
│  │  ├─ __init__.py
│  │  └─ ya_api.py
│  │     - 作用：YouTube Analytics v2 低階客戶端與通用查詢介面（reports.query），供上層方法如 query_channel_daily 使用。
│  ├─ models/
│  │  ├─ fact_yta_channel_daily.py
│  │  │  - 作用：定義頻道日彙總事實表 ORM 模型（以 channel_id+day 為複合主鍵），支援 ETL 寫入與報表查詢。
│  │  ├─ fact_yta_video_window.py
│  │  │  - 作用：定義影片視窗彙總事實表的欄位白名單與 MySQL/MariaDB UPSERT SQL，寫入 [start_date, end_date] 視窗聚合 KPI。
│  │  └─ __init__.py
│  │     - 作用：套件初始化。
│  ├─ notifications/
│  │  ├─ __init__.py
│  │  ├─ runner.py
│  │  │  - 作用：統一重試與管線執行封裝；run_step_with_retry 執行單步驟、run_pipeline_and_notify 順序執行四步並產出摘要、寫入日誌與觸發通知。
│  │  └─ senders.py
│  │     - 作用：多通道通知（Email/LINE/Discord）。format_summary_text 組裝摘要；notify_all 統一發送並容錯。
│  ├─ services/
│  │  ├─ __init__.py
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
│  │  ├─ __init__.py
│  │  ├─ terminal.py
│  │  │  - 作用：清空終端畫面的小工具（跨平台）。
│  │  └─ tree.py
│  │     - 作用：輸出專案目錄樹到終端並寫入 docs/PROJECT_TREE.md；支援自訂輸出檔與排除目錄。
│  ├─ youtube/
│  │  ├─ client.py
│  │  │  - 作用：YouTube API/Analytics OAuth 與用戶端。解析 scopes/憑證路徑/埠、提供 with_retries/呼叫重試、建立 googleapiclient 服務、診斷憑證狀態。
│  │  ├─ __init__.py
│  │  ├─ playlists.py
│  │  │  - 作用：透過 uploads 播放清單遍歷 videoId，支援日期過濾與重試；提供時間界線工具與 RFC3339 解析。
│  │  └─ videos.py
│  │     - 作用：批次呼叫 videos.list 解析常用欄位；依狀態/時長/URL 決定影片型別；提供 requests 重試、時間與時長解析、DB 影片 meta 讀取。
│  ├─ cli.py
│  │  - 作用：CLI 入口（Typer）。預設 run_all，提供 ingest_channel_daily、fetch_videos、top_videos、update_playlists；整合通知與重試。
│  ├─ __init__.py
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

[新增於 0.1.1]
- 新增與調整（位於 scripts/ 與子模組）：
  - cli.py（Typer CLI 入口）
  - notifications/{runner.py, senders.py}
  - youtube/{client.py, playlists.py, videos.py}
  - ingestion/{ya_api.py, channel_daily.py, dim_channel.py}
  - services/{video_ingestion.py, top_videos_query.py, playlist_update.py}
  - models/{fact_yta_channel_daily.py, fact_yta_video_window.py}
  - utils/{env.py, dates.py, terminal.py, tree.py}
  - run_probe.py, docs/PROJECT_TREE.md, docs/oauth_guide.md
  - data/probe_summary.json、logs/pipeline_*.log（執行後產生）

## 開發與測試
- 型別檢查：mypy src
- Lint：ruff check .
- 測試：pytest -q

[新增於 0.1.1]
- 建議在 CI 中增加：
  - 以 scripts/run_probe.py --check all 驗證憑證與 DB 連線（可於 PR workflow 的 optional job）
  - 產物收集：上傳 logs/*.log 與 data/probe_summary.json 供追蹤

## 安全與版本控制
- .gitignore 建議：.env、.env.*、credentials/*、任何 token 檔、logs/*、data/*
- 多環境部署建議：.env.local / .env.prod 並配合 Secret Manager / Vault 發佈機密

## 版本與變更紀錄
- 規範：Keep a Changelog；版本：Semantic Versioning（MAJOR.MINOR.PATCH）
- 最新版本：0.1.1（2025/10/25 21:30）
- 詳細變更請見 CHANGELOG.md

[2025/10/25 21:30 更新 版本 0.1.1]
- 增加 CLI 入口與多通道通知
- 擴充 Analytics 與 Data API 客戶端與 ETL
- 新增影片排行查詢與播放清單管理
- 補充環境變數與 probe/log 產出說明
- 將資料庫模型與 UPSERT 規範化

[2025/10/25 22:30 更新 版本 0.1.1.1]
- 版本：更新至 0.1.1.1 修正不能直接啟動主程式的錯誤