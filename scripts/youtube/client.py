# scripts/youtube/client.py
# 總覽：
# - 憑證/設定輔助：_scopes_from_settings、_get_credentials_path/_get_token_path/_get_port（從 settings 解析 OAuth 參數）
# - 重試機制：with_retries 裝飾器 + call_with_retries 包裝任意 API 呼叫（對 429/5xx 指數退避）
# - OAuth 流程：_load_credentials 載入/刷新/互動授權，get_youtube_data_client 建立 googleapiclient 服務，get_bearer_token 取 access token
# - 診斷工具：debug_describe_auth 回傳目前設定與憑證健康狀態摘要（不含敏感內容）

from __future__ import annotations

import json
import os
import time
import functools
from typing import Callable, Dict, List, Optional, TypeVar, Any

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError

T = TypeVar("T")

# ------------------------------
# 讀取與解析設定
# ------------------------------

def _scopes_from_settings(settings: Dict[str, str]) -> List[str]:
    """
    從 settings（通常為環境變數字典）中解析 OAuth 權限範圍（scopes）。
    - 來源鍵：YDAO_OAUTH_SCOPES，逗號分隔字串；若缺省，給管理 YouTube 的廣義範圍。
    - 回傳：scope 字串列表（去除空白與空值）。
    """
    raw = (settings.get("YDAO_OAUTH_SCOPES") or "").strip()
    if not raw:
        # 預設提供涵蓋管理影片/播放清單的最大權限之一
        return ["https://www.googleapis.com/auth/youtube"]
    return [s.strip() for s in raw.split(",") if s.strip()]

def _get_credentials_path(settings: Dict[str, str]) -> str:
    """
    從 settings 取得 client_secrets 檔路徑。
    - 來源鍵：YDAO_CREDENTIALS_PATH（必填）
    - 若未提供，拋出 ValueError。
    """
    cred_path = (settings.get("YDAO_CREDENTIALS_PATH") or "").strip()
    if not cred_path:
        raise ValueError("Missing YDAO_CREDENTIALS_PATH")
    return cred_path

def _get_token_path(settings: Dict[str, str]) -> Optional[str]:
    """
    從 settings 取得 token 檔路徑（可選）。
    - 來源鍵：YDAO_TOKEN_PATH；缺省或空字串則回傳 None。
    - 若提供，_load_credentials 會自動讀寫該檔保存/更新使用者授權憑證。
    """
    token_path = (settings.get("YDAO_TOKEN_PATH") or "").strip()
    return token_path or None

def _get_port(settings: Dict[str, str]) -> int:
    """
    取得本機 OAuth 回呼埠號（可選）。
    - 來源鍵：YDAO_OAUTH_PORT；非數字或缺省時回傳 0（代表使用 console 流程）。
    - port > 0 時採用 run_local_server；否則 run_console。
    """
    raw = (settings.get("YDAO_OAUTH_PORT") or "").strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0

# ------------------------------
# 指數退避重試（可用於包裝 Data API 呼叫）
# ------------------------------

def with_retries(max_retries: int = 5, backoff_base: float = 1.5, retry_on: Optional[List[int]] = None):
    """
    裝飾器：對暫時性錯誤實施退避重試（適用於 googleapiclient 呼叫）。
    - 參數：
      - max_retries：最大重試次數（不含首次呼叫）；實際嘗試最多為 1 + max_retries 次。
      - backoff_base：退避底數；等待秒數為 backoff_base ** attempt（attempt 從 0 起）。
      - retry_on：需重試的 HTTP 狀態碼集合；預設 [429, 500, 502, 503, 504]。
    - 行為：
      - 捕捉 HttpError，若狀態碼在 retry_on 且尚未超過次數上限，則 sleep 後重試。
      - 其他錯誤或超過次數上限，直接拋出原例外。
    """
    if retry_on is None:
        retry_on = [429, 500, 502, 503, 504]

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except HttpError as e:
                    # googleapiclient.errors.HttpError 通常可由 e.resp.status 取得狀態碼
                    status = getattr(getattr(e, "resp", None), "status", None)
                    # 兼容某些套件版本可能暴露 status_code
                    if status is None:
                        status = getattr(e, "status_code", None)
                    if isinstance(status, int) and status in retry_on and attempt < max_retries:
                        sleep_s = backoff_base ** attempt
                        time.sleep(sleep_s)
                        attempt += 1
                        continue
                    # 非可重試狀態或已達上限：拋出
                    raise
        return wrapper
    return decorator

# ------------------------------
# OAuth 憑證載入與初始化
# ------------------------------

def _load_credentials(settings: Dict[str, str]) -> Credentials:
    """
    載入/初始化使用者 OAuth 憑證。
    - 流程：
      1) 解析 scopes、credentials 路徑、token 路徑。
      2) 若 token 檔存在則讀入憑證（scopes 需一致）。
      3) 若憑證無效：若可 refresh 則刷新；否則執行互動授權流程（本機伺服器或 console）。
      4) 成功後（或刷新後）若設定了 token 路徑，將憑證寫回檔案。
    - 例外：
      - credentials 檔不存在時，拋出 FileNotFoundError。
    """
    scopes = _scopes_from_settings(settings)
    cred_path = _get_credentials_path(settings)
    token_path = _get_token_path(settings)

    if not os.path.exists(cred_path):
        raise FileNotFoundError(f"YDAO_CREDENTIALS_PATH not found: {cred_path}")

    creds: Optional[Credentials] = None

    # 1) 嘗試從 token 檔載入既有憑證
    if token_path and os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, scopes=scopes)

    # 2) 若尚未有效，嘗試刷新或進行授權流程
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # 有 refresh_token 可直接刷新
            creds.refresh(Request())
        else:
            # 無可刷新資訊：走互動授權流程
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, scopes=scopes)
            port = _get_port(settings)
            if port and port > 0:
                # 啟動本機回呼伺服器（瀏覽器授權）
                creds = flow.run_local_server(port=port, prompt="consent")
            else:
                # 無法開埠（或 CI 環境）：改走 console 授權
                creds = flow.run_console(prompt="consent")

        # 3) 寫回/更新 token 檔（若指定了路徑）
        if token_path:
            dir_ = os.path.dirname(token_path)
            if dir_:
                os.makedirs(dir_, exist_ok=True)
            with open(token_path, "w", encoding="utf-8") as f:
                f.write(creds.to_json())

    return creds

# ------------------------------
# 對外介面：YouTube Data API Client 與 Bearer Token
# ------------------------------

def get_youtube_data_client(settings: Dict[str, str]):
    """
    建立並回傳 YouTube Data API v3 的 googleapiclient 服務物件。
    - 用途：需要 OAuth 的寫入/管理操作（如 playlistItems.insert、videos.update 等）。
    - 關閉 discovery 快取以避免某些部署環境的檔案權限問題。
    """
    creds = _load_credentials(settings)
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def get_bearer_token(settings: Dict[str, str]) -> str:
    """
    取得目前有效的 OAuth access token 字串（適合搭配 requests 使用）。
    - 若憑證已過期但可刷新，會自動刷新後回傳最新 token。
    """
    creds = _load_credentials(settings)
    if not creds.valid:
        creds.refresh(Request())
    return creds.token

# ------------------------------
# 輔助：簡易呼叫範本（可選）
# 你可以在 playlists.py / videos.py 中這樣用：
#   yt = get_youtube_data_client(settings)
#   resp = call_with_retries(lambda: yt.playlistItems().insert(...).execute(), settings)
# ------------------------------

def call_with_retries(callable_fn: Callable[[], T], settings: Optional[Dict[str, str]] = None) -> T:
    """
    以設定檔參數包裝一次性呼叫並套用退避重試。
    - 來源設定：
      - RETRY_MAX：最大重試次數（預設 5）
      - RETRY_BACKOFF_BASE：退避底數（預設 1.5）
    - 用法：call_with_retries(lambda: yt.playlistItems().insert(...).execute(), settings)
    """
    max_retries = 5
    backoff_base = 1.5
    if settings:
        # 安全解析整數/浮點，失敗時維持預設
        try:
            max_retries = int(settings.get("RETRY_MAX", max_retries))
        except Exception:
            pass
        try:
            backoff_base = float(settings.get("RETRY_BACKOFF_BASE", backoff_base))
        except Exception:
            pass

    @with_retries(max_retries=max_retries, backoff_base=backoff_base)
    def _wrapped():
        return callable_fn()

    return _wrapped()

# ------------------------------
# 健康檢查/診斷（可選）
# ------------------------------

def debug_describe_auth(settings: Dict[str, str]) -> Dict[str, Any]:
    """
    回傳 OAuth 設定與憑證狀態的摘要資訊，協助診斷問題。
    - 包含：scopes、credentials/token 檔是否存在、token 是否有效/過期、是否有 refresh_token、OAuth 監聽埠等。
    - 不回傳敏感 token 內容，但會在 error 欄位放入例外摘要（若載入過程失敗）。
    """
    scopes = _scopes_from_settings(settings)
    cred_path = _get_credentials_path(settings)
    token_path = _get_token_path(settings)

    summary: Dict[str, Any] = {
        "scopes": scopes,
        "credentials_exists": os.path.exists(cred_path),
        "token_path": token_path,
        "token_exists": bool(token_path and os.path.exists(token_path)),
        "oauth_port": _get_port(settings),
    }

    try:
        creds = _load_credentials(settings)
        summary.update(
            {
                "token_valid": bool(creds and creds.valid),
                "token_expired": bool(creds and creds.expired),
                "has_refresh_token": bool(creds and creds.refresh_token),
            }
        )
    except Exception as e:
        # 將例外摘要記錄在欄位中便於排查
        summary["error"] = f"{type(e).__name__}: {e}"

    return summary