===== docs/oauth_guide.md =====
# OAuth 流程說明（最小步驟與注意事項）

本專案同時支援三種憑證機制：
- YDAO：YouTube Data API v3（OAuth，用於寫入/管理）
- YAAO：YouTube Analytics API（OAuth，用於報表/營運指標）
- YPKG：API Key（公開只讀，選配；不需 OAuth）

請先依 .env 完成對應區塊設定，再依序進行授權或測試。

## 檔案與目錄建議（便於辨識）
- 本文件建議路徑：docs/oauth_guide.md（本文）
- 憑證與 Token：
  - credentials/youtube_oauth_client_secret.json（YDAO 用）
  - credentials/youtube_oauth_token.json（YDAO 產生）
  - credentials/analytics_oauth_client_secret.json（YAAO 用）
  - credentials/analytics_oauth_token.json（YAAO 產生）
- 授權腳本（範例命名，依技術棧調整）：
  - scripts/oauth_data.(py|ts|js)
  - scripts/oauth_analytics.(py|ts|js)
- .gitignore 建議：
  - .env / .env.* / credentials/* / logs/* / data/*

## 前置檢查（對照 .env）
- 基本環境：
  - ENV=local、LOG_LEVEL=INFO、TZ=UTC
- Data（YDAO）：
  - YDAO_OAUTH_SCOPES=https://www.googleapis.com/auth/youtube,https://www.googleapis.com/auth/youtube.force-ssl
  - YDAO_OAUTH_PORT=8081
  - YDAO_CREDENTIALS_PATH=credentials/youtube_oauth_client_secret.json
  - YDAO_TOKEN_PATH=credentials/youtube_oauth_token.json
- Analytics（YAAO）：
  - YAAO_OAUTH_SCOPES=https://www.googleapis.com/auth/yt-analytics.readonly,https://www.googleapis.com/auth/yt-analytics-monetary.readonly
  - YAAO_OAUTH_PORT=8082
  - YAAO_CREDENTIALS_PATH=credentials/analytics_oauth_client_secret.json
  - YAAO_TOKEN_PATH=credentials/analytics_oauth_token.json
- 公開只讀（YPKG，選配）：
  - YPKG_API_KEY=你的 API Key（你目前已設定）
  - YPKG_ENABLE=true
  - YPKG_PREF_ONLY_PUBLIC=false

## 在 Google Cloud Console 建立 OAuth 2.0 用戶端
- 建議分別建立 Data 與 Analytics 各一組 OAuth Client（易於權限隔離與輪替）
- 類型：Desktop App 或 Web（建議 Desktop，設定最簡單）
- 如果使用本機回呼伺服器，請設定 Authorized redirect URIs：
  - http://localhost:8081/（對應 YDAO_OAUTH_PORT）
  - http://localhost:8082/（對應 YAAO_OAUTH_PORT）
- 下載 client_secret JSON 並放置於：
  - YDAO_CREDENTIALS_PATH
  - YAAO_CREDENTIALS_PATH

## 最小授權步驟
A. Data（YDAO）
1) 執行：python scripts/oauth_data.py 或 npm run oauth:data
2) 瀏覽器登入並勾選與 YDAO_OAUTH_SCOPES 一致的範圍
3) 成功後回呼至 http://localhost:8081/，生成：
   - credentials/youtube_oauth_token.json（對應 YDAO_TOKEN_PATH）

B. Analytics（YAAO）
1) 執行：python scripts/oauth_analytics.py 或 npm run oauth:analytics
2) 瀏覽器勾選與 YAAO_OAUTH_SCOPES 一致的範圍
3) 成功後回呼至 http://localhost:8082/，生成：
   - credentials/analytics_oauth_token.json（對應 YAAO_TOKEN_PATH）

## 驗證授權是否成功
- 檢查 token 檔生成：
  - credentials/youtube_oauth_token.json
  - credentials/analytics_oauth_token.json
- 最小 API 呼叫：
  - Data：呼叫 channels.list mine=true 或播放清單維護端點
  - Analytics：呼叫 reports.query 取得 views 或 impressions（配合 CHANNEL_ID、日期區間）

## 注意事項與常見問題
- Scopes 對齊：授權頁面核准的 scopes 必須覆蓋 .env，否則可能 403/insufficientPermissions
- 回呼埠占用：若 8081/8082 被占用，請更改 YDAO_OAUTH_PORT/YAAO_OAUTH_PORT 並同步更新 GCP redirect URI
- 憑證路徑：確保 *_CREDENTIALS_PATH 指向正確 client_secret JSON
- Token 管理：
  - token JSON 內含 refresh_token，請勿提交至版本庫
  - 若更新 scopes 後缺少 refresh_token，建議撤銷既有授權並重新授權
- 帳號權限：授權帳號需擁有對應頻道/資源權限（特別是 Analytics 綁定）
- 企業/代理網路：若擋回呼或 OAuth 流量，改用可用網路或設定代理
- API Key（YPKG）用途：
  - 僅限公開只讀端點；涉及私有或寫入請改用 OAuth
  - 建議於 GCP 為 API Key 設 IP/HTTP 來源限制，避免濫用

## 推薦檔名與放置位置（總結）
- 說明文件：docs/oauth_guide.md（本文件）
- OAuth 用戶端密鑰與 Token：
  - credentials/youtube_oauth_client_secret.json
  - credentials/youtube_oauth_token.json
  - credentials/analytics_oauth_client_secret.json
  - credentials/analytics_oauth_token.json
- 授權腳本：
  - scripts/oauth_data.(py|ts|js)
  - scripts/oauth_analytics.(py|ts|js)
