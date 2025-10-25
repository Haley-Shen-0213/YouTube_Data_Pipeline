===== CHANGELOG.md =====
# Changelog
All notable changes to this project will be documented in this file.

The format follows Keep a Changelog and the project adheres to Semantic Versioning.

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

[0.1.0]: https://github.com/Haley-Shen-0213/YouTube_Data_Pipeline/releases/tag/v0.1.0
