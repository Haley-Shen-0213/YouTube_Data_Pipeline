# scripts/utils/env.py
# 總覽：
# - 模組用途：統一載入 .env 與系統環境變數，整理與驗證設定鍵，並提供預設值。
# - 主要函式：load_settings 讀取環境、去除空白、檢查必填（CHANNEL_ID/START_DATE/END_DATE/DB_URL），套用預設（OUTPUT_DIR/LOG_DIR）。
# - 例外處理：若缺少必填鍵，透過 SystemExit 中止並輸出明確錯誤訊息。

import os
from dotenv import load_dotenv
from typing import Dict

def load_settings() -> Dict[str, str]:
    """
    載入所有環境變數（.env + 系統環境），並確保必填鍵存在；回傳包含所有鍵的字典（值一律為字串）。
    - 必填鍵：CHANNEL_ID、START_DATE、END_DATE、DB_URL（缺少時以 SystemExit 結束並列出缺項）
    - 預設鍵：OUTPUT_DIR="data"、LOG_DIR="logs"（若未提供才套用）
    - 正規化：會將所有字串值去除前後空白，避免因空白造成判斷錯誤
    """
    # 1) 從專案根目錄或當前工作目錄讀取 .env，載入到 process 環境變數中
    #    設定 override=False 以避免覆蓋已存在的系統環境（例如在部署環境由外部注入的密鑰）
    load_dotenv(override=False)

    # 2) 取得目前所有環境變數（已包含 .env 載入的鍵）
    #    將其複製為一個普通 dict（鍵/值均為字串）
    settings: Dict[str, str] = {k: v for k, v in os.environ.items()}

    # 3) 正規化值：去除每個字串值的前後空白，避免「空白字串」造成必填判斷誤判
    for k, v in list(settings.items()):
        if isinstance(v, str):
            settings[k] = v.strip()

    # 4) 檢查必填鍵是否存在且非空
    #    - CHANNEL_ID：YouTube 頻道 ID
    #    - START_DATE/END_DATE：日期區間（YYYY-MM-DD）
    #    - DB_URL：資料庫連線字串
    required = ["CHANNEL_ID", "START_DATE", "END_DATE", "DB_URL"]
    missing = [k for k in required if not settings.get(k)]
    if missing:
        # 使用 SystemExit 以明確中止程序，並輸出缺少的鍵名，便於在 CI/啟動時立即發現配置問題
        raise SystemExit(f"[ERROR] Missing env: {', '.join(missing)}")

    # 5) 設定可選鍵的預設值（若未提供才套用）
    #    - OUTPUT_DIR：輸出資料目錄，預設 data
    #    - LOG_DIR：日誌目錄，預設 logs
    settings.setdefault("OUTPUT_DIR", "data")
    settings.setdefault("LOG_DIR", "logs")

    # 6) 回傳整理後的設定字典，供應用其餘部分統一使用
    return settings