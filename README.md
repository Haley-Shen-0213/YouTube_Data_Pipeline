===== README.md =====
# YouTube Data Pipeline (Python 3.13)

以 YouTube Data API v3 抓取頻道/影片/留言等公開資料（API Key），並可選擇透過 YouTube Analytics API（OAuth）取得營運指標；同時支援 YouTube Data API 的 OAuth（YDAO）以進行寫入/管理操作。提供 CLI、型別註解、基礎測試與 CI，協助快速將結構化資料落地到 MySQL。

- 資料來源：
  - YouTube Data API v3（API Key；亦支援 OAuth 寫入/管理）
  - YouTube Analytics API（OAuth）
- 功能：頻道/影片/留言抓取、影片時長解析、日指標彙整、MySQL 寫入
- 工具：CLI（probe/fetch）、ruff、mypy、pytest、GitHub Actions CI
- 版本註記：本文件對應 版本 0.1.0（首版發布，2025/10/25 12:00）

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

### 3) 其他行為參數（可選）
- REQUEST_TIMEOUT_SEC=30
- RETRY_MAX=5
- RETRY_BACKOFF_BASE=1.5
- MAX_RESULTS=50
- SLICE_DAYS=7

### 4) 基本輸出與資料庫
- OUTPUT_DIR=data
- LOG_DIR=logs
- DB_URL=mysql+pymysql://yt_user:yt_password@localhost:3306/yt_analytics?charset=utf8mb4

### 5) 範例頻道與日期（可選）
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

## 使用方法（CLI）
- 公開資料抓取：python -m src.cli.fetch public --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
- Analytics 指標抓取：python -m src.cli.fetch analytics --channel-id $CHANNEL_ID --start $START_DATE --end $END_DATE
- 撰寫/管理操作（需 YDAO OAuth）：python -m src.cli.manage playlist --action upsert --video-id ...

註：實際指令以專案內 CLI 實作為準。

## 資料庫說明（MySQL）
- 維度表＋事實表：dim_channel、dim_video、dim_date、fact_yta_daily、fact_yta_channel_daily、fact_yta_revenue、fact_yta_traffic_source_agg、view_fact_yta_channel_daily_clean
- 建議型別：百分比/比率 DECIMAL(6,3)，營收 DECIMAL(10,4)+
- 依需求建立索引，詳見 docs/schema.md

## 專案結構
- scripts/：probe 與工具腳本（含 run_probe.py）
- src/：核心程式碼（clients/ingestion/storage/analysis/cli）
- logs/、data/、credentials/：輸出與憑證
- docs/：文件（含 oauth_guide.md、schema.md）

## 開發與測試
- 型別檢查：mypy src
- Lint：ruff check .
- 測試：pytest -q

## 安全與版本控制
- .gitignore 建議：.env、.env.*、credentials/*、任何 token 檔、logs/*、data/*
- 多環境部署建議：.env.local / .env.prod 並配合 Secret Manager / Vault 發佈機密

## 版本與變更紀錄
- 規範：Keep a Changelog；版本：Semantic Versioning（MAJOR.MINOR.PATCH）
- 最新版本：0.1.0（首版發布，2025/10/25 12:00）
- 詳細變更請見 CHANGELOG.md
