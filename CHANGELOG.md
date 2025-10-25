===== CHANGELOG.md =====
# Changelog
All notable changes to this project will be documented in this file.

The format follows Keep a Changelog and the project adheres to Semantic Versioning.

## [0.1.1] - 2025-10-25 21:30
### Added
- 新增 Typer CLI 入口 scripts/cli.py，提供：
  - run_all、ingest_channel_daily、fetch_videos、top_videos、update_playlists
- 新增通知與重試模組：scripts/notifications/{runner.py, senders.py}
  - 支援多通道通知（Discord/Email/LINE），輸出管線摘要與日誌鏈結
- 新增 YouTube API/Analytics 客戶端與 ETL：
  - scripts/youtube/{client.py, playlists.py, videos.py}
  - scripts/ingestion/{ya_api.py, channel_daily.py, dim_channel.py}
- 新增服務模組與功能：
  - scripts/services/{video_ingestion.py, top_videos_query.py, playlist_update.py}
- 新增資料庫模型與 UPSERT 腳手架：
  - scripts/models/{fact_yta_channel_daily.py, fact_yta_video_window.py}
- 新增共用工具：
  - scripts/utils/{env.py, dates.py, terminal.py, tree.py}
- 新增文件與輸出：
  - docs/PROJECT_TREE.md、docs/oauth_guide.md
  - data/probe_summary.json（由 run_probe 產生），logs/pipeline_*.log

### Changed
- README 增補 0.1.1 的 CLI 使用範例、環境變數（PLAYLIST_UPDATE_BATCH、PLAYLIST_UPDATE_COOLDOWN_SEC、NOTIFY_SENDERS 等）、probe/log 輸出位置說明
- 專案結構章節補充 scripts 下新增模組與檔案

### Deprecated
- 無

### Fixed
- 改善 probe 與管線在網路錯誤時的重試與通知流程，避免中斷主作業

### Removed
- 無

## [0.1.0] - 2025-10-25 12:00
### Added
- 分離並清楚說明兩種 YouTube 服務的認證與設定：
  - YouTube Data API（API Key，公開只讀）
  - YouTube Analytics API（OAuth，營運指標）
- 新增 .env.example，清楚標示 Data/Analytics/DB 與常用營運參數
- 新增探測腳本 scripts/run_probe.py，支援 --check data|analytics|db|all
- 客戶端初版骨架：
  - src/clients/youtube_data.py
  - src/clients/youtube_analytics.py
- OAuth 管理骨架：src/app/google_auth.py
- README 首版重寫：快速開始、Probes、CLI、專案結構、版本規範

### Changed
- 調整專案說明與目錄，凸顯最小可執行路徑與驗證流程

### Deprecated
- 舊散落的 probe 腳本規劃併入 scripts/run_probe.py（後續版本將移除）

### Fixed
- N/A

### Removed
- Postgres 支援（統一為 MySQL）

[0.1.1]: https://github.com/Haley-Shen-0213/YouTube_Data_Pipeline/releases/tag/v0.1.1
[0.1.0]: https://github.com/Haley-Shen-0213/YouTube_Data_Pipeline/releases/tag/v0.1.0
