import os
import sys
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple, Union

import httpx
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# OAuth / Google API 客戶端
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# 抽離後的清除終端工具（若在服務模式下不一定會用到，但保留）
from scripts.utils.terminal import clear_terminal


# =========================
# 資料類型
# =========================
@dataclass
class ProbeResult:
    """
    表示單一探針（probe）執行結果的資料結構。

    欄位
    - name: 探針名稱（如 'ypkg', 'ydao', 'yaao', 'db', 'consistency'）
    - ok: 是否成功
    - message: 人類可讀的說明訊息
    - extra: 其他額外資訊（選用），例如 mine_channel_id
    """
    name: str
    ok: bool
    message: str
    extra: Optional[Dict[str, Any]] = None

@dataclass
class ProbeOutputs:
    """
    封裝所有探針的結果，便於結構化回傳給其他模組使用。
    """
    ypkg: ProbeResult
    db: ProbeResult
    ydao: ProbeResult
    yaao: ProbeResult
    consistency: ProbeResult


# =========================
# 工具函式
# =========================
def parse_bool(val: Optional[str], default: bool = False) -> bool:
    """
    將字串解析為布林值。

    規則
    - True 值：'1', 'true', 'yes', 'y', 'on'（不分大小寫）
    - False 值：其餘或 None（回傳 default）

    參數
    - val: 待解析的字串或 None
    - default: 當 val 為 None 時的預設布林值

    回傳
    - bool: 解析後的布林值
    """
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def load_env() -> Dict[str, str]:
    """
    載入並驗證必要的環境變數，提供相容舊鍵的回退機制。

    必要鍵
    - CHANNEL_ID, START_DATE, END_DATE, DB_URL

    相容處理
    - YPKG_API_KEY 與舊鍵 YT_API_KEY
    - YDAO_* / YAAO_* 與舊鍵 GOOGLE_*（在新鍵缺失時回退）

    回傳
    - Dict[str, str]: 綜合後的設定值字典

    失敗行為
    - 若缺少必要鍵，印出錯誤並以代碼 1 結束程式。
    """
    load_dotenv()

    required = ["CHANNEL_ID", "START_DATE", "END_DATE", "DB_URL"]
    values: Dict[str, str] = {}
    missing: List[str] = []

    for key in required:
        v = os.getenv(key)
        if not v:
            missing.append(key)
        else:
            values[key] = v.strip()

    # 相容舊鍵與新鍵（皆可存在）
    # Public(API Key)
    if os.getenv("YPKG_API_KEY"):
        values["YPKG_API_KEY"] = os.getenv("YPKG_API_KEY").strip()
    if os.getenv("YT_API_KEY") and "YPKG_API_KEY" not in values:
        # fallback 舊鍵
        values["YPKG_API_KEY"] = os.getenv("YT_API_KEY").strip()

    values["YPKG_ENABLE"] = os.getenv("YPKG_ENABLE", "true").strip()
    values["YPKG_PREF_ONLY_PUBLIC"] = os.getenv("YPKG_PREF_ONLY_PUBLIC", "false").strip()

    # YDAO（Data OAuth）
    if os.getenv("YDAO_OAUTH_SCOPES"):
        values["YDAO_OAUTH_SCOPES"] = os.getenv("YDAO_OAUTH_SCOPES").strip()
    if os.getenv("YDAO_OAUTH_PORT"):
        values["YDAO_OAUTH_PORT"] = os.getenv("YDAO_OAUTH_PORT").strip()
    if os.getenv("YDAO_CREDENTIALS_PATH"):
        values["YDAO_CREDENTIALS_PATH"] = os.getenv("YDAO_CREDENTIALS_PATH").strip()
    if os.getenv("YDAO_TOKEN_PATH"):
        values["YDAO_TOKEN_PATH"] = os.getenv("YDAO_TOKEN_PATH").strip()

    # YAAO（Analytics OAuth）
    if os.getenv("YAAO_OAUTH_SCOPES"):
        values["YAAO_OAUTH_SCOPES"] = os.getenv("YAAO_OAUTH_SCOPES").strip()
    if os.getenv("YAAO_OAUTH_PORT"):
        values["YAAO_OAUTH_PORT"] = os.getenv("YAAO_OAUTH_PORT").strip()
    if os.getenv("YAAO_CREDENTIALS_PATH"):
        values["YAAO_CREDENTIALS_PATH"] = os.getenv("YAAO_CREDENTIALS_PATH").strip()
    if os.getenv("YAAO_TOKEN_PATH"):
        values["YAAO_TOKEN_PATH"] = os.getenv("YAAO_TOKEN_PATH").strip()

    # 舊版相容 Google OAuth 欄位（若新鍵沒提供，當作 fallback）
    if os.getenv("GOOGLE_CREDENTIALS_PATH") and "YAAO_CREDENTIALS_PATH" not in values and "YDAO_CREDENTIALS_PATH" not in values:
        values["GOOGLE_CREDENTIALS_PATH"] = os.getenv("GOOGLE_CREDENTIALS_PATH").strip()
    if os.getenv("GOOGLE_TOKEN_PATH") and "YAAO_TOKEN_PATH" not in values and "YDAO_TOKEN_PATH" not in values:
        values["GOOGLE_TOKEN_PATH"] = os.getenv("GOOGLE_TOKEN_PATH").strip()

    # 其他
    if os.getenv("OUTPUT_DIR"):
        values["OUTPUT_DIR"] = os.getenv("OUTPUT_DIR").strip()
    if os.getenv("LOG_DIR"):
        values["LOG_DIR"] = os.getenv("LOG_DIR").strip()

    if missing:
        print(f"[錯誤] 缺少必要的環境變數: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    return values


def iso_date_boundaries(start_str: str, end_str: str) -> Tuple[str, str]:
    """
    將起訖日期（ISO-8601）轉換為 YouTube API 需要的 RFC3339 範圍。

    規則
    - 起始：以 UTC 的當日 00:00:00Z
    - 結束：以 UTC 的當日 23:59:59Z（含當日）

    參數
    - start_str: 起始日期字串，例如 '2025-10-01'
    - end_str: 結束日期字串，例如 '2025-10-31'

    回傳
    - Tuple[str, str]: (start_rfc3339, end_rfc3339)

    失敗行為
    - 日期格式錯誤或 end < start 時，印錯並結束程式。
    """
    try:
        start = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(end_str).replace(tzinfo=timezone.utc)
    except ValueError:
        print("[錯誤] START_DATE/END_DATE 必須是 ISO-8601 格式，例如 2025-10-01", file=sys.stderr)
        sys.exit(1)

    if end < start:
        print("[錯誤] END_DATE 必須大於或等於 START_DATE", file=sys.stderr)
        sys.exit(1)

    start_rfc3339 = start.isoformat().replace("+00:00", "Z")
    end_eod = (end + timedelta(days=1)) - timedelta(seconds=1)
    end_rfc3339 = end_eod.isoformat().replace("+00:00", "Z")
    return start_rfc3339, end_rfc3339


# =========================
# 驗證：Public(API Key)
# =========================
def count_videos_in_range(api_key: str, channel_id: str, start_iso: str, end_iso: str) -> Tuple[int, Optional[str]]:
    """
    使用 YouTube Search API 計算特定頻道於時間範圍內發佈的影片數量。

    參數
    - api_key: 公開 API Key
    - channel_id: 頻道 ID
    - start_iso: RFC3339 的開始時間（含）
    - end_iso: RFC3339 的結束時間（含）

    回傳
    - (total_count, last_video_channel_id): 影片總數與最後一個項目的頻道 ID（目前保留 None）

    例外
    - 網路或 API 錯誤時會以 SystemExit 中止並輸出錯誤原因。
    """
    base_url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": api_key,
        "channelId": channel_id,
        "part": "id",
        "type": "video",
        "order": "date",
        "publishedAfter": start_iso,
        "publishedBefore": end_iso,
        "maxResults": 50,
    }

    total = 0
    next_page: Optional[str] = None
    last_video_channel: Optional[str] = None

    with httpx.Client(timeout=20.0) as client:
        while True:
            if next_page:
                params["pageToken"] = next_page
            else:
                params.pop("pageToken", None)

            try:
                resp = client.get(base_url, params=params)
            except httpx.RequestError as e:
                raise SystemExit(f"[錯誤] 網路錯誤: {e}")

            if resp.status_code == 429:
                raise SystemExit("[錯誤] 配額超限 (429)。請稍後再試或縮小查詢範圍。")
            if resp.status_code >= 500:
                raise SystemExit(f"[錯誤] 伺服器錯誤 {resp.status_code}: {resp.text[:200]}")
            if resp.status_code != 200:
                raise SystemExit(f"[錯誤] API 錯誤 {resp.status_code}: {resp.text[:200]}")

            data = resp.json()
            items: List[Dict[str, Any]] = data.get("items", [])

            for it in items:
                kind = it.get("id", {}).get("kind")
                if kind == "youtube#video":
                    total += 1
            next_page = data.get("nextPageToken")
            if not next_page:
                break

    return total, last_video_channel


# =========================
# 驗證：DB
# =========================
def test_mysql_connection(db_url: str) -> "ProbeResult":
    """
    測試 MySQL 連線是否正常，透過簡單的 SELECT 1 驗證。

    參數
    - db_url: SQLAlchemy 風格的連線字串，例如
      'mysql+pymysql://user:pass@localhost:3306/yt_analytics?charset=utf8mb4'

    回傳
    - ProbeResult: 成功或失敗的結果與訊息

    例外
    - 捕捉 SQLAlchemyError 並回傳失敗結果，不會丟出例外。
    """
    try:
        engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                return ProbeResult("db", True, "MySQL 連線正常 (SELECT 1 回傳 1)")
            else:
                return ProbeResult("db", False, f"MySQL 連線結果異常: {result}")
    except SQLAlchemyError as e:
        return ProbeResult("db", False, f"MySQL 連線失敗: {e.__class__.__name__}: {e}")


# =========================
# OAuth 共用
# =========================
def ensure_dir_for(path: str) -> None:
    """
    確保給定路徑的父目錄存在，若不存在則自動建立。

    參數
    - path: 檔案路徑（非目錄）
    """
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def get_oauth_credentials(
    scopes: List[str],
    token_path: Optional[str] = None,
    client_secret_path: Optional[str] = None,
    port: Optional[int] = None,
) -> Credentials:
    """
    取得或建立 OAuth 憑證（Google InstalledAppFlow）。

    流程
    1) 若 token 檔存在，嘗試載入並驗證有效性。
    2) 若過期且有 refresh_token，嘗試刷新。
    3) 否則以互動方式進行授權（run_local_server）。
    4) 成功後寫回 token 檔。

    參數
    - scopes: 權限範圍清單
    - token_path: token 檔案路徑（預設 'token.json'）
    - client_secret_path: OAuth client secret JSON 路徑（預設 'client_secret.json'）
    - port: 本機回呼埠；None 或 0 表示讓系統自動分配

    回傳
    - Credentials: 可用的憑證物件

    例外
    - 缺少 client secret、互動授權失敗、或寫入 token 檔失敗時，以 SystemExit 中止。
    """
    token_path = token_path or "token.json"
    client_secret_path = client_secret_path or "client_secret.json"
    ensure_dir_for(token_path)

    creds: Optional[Credentials] = None

    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, scopes)
        except Exception as e:
            print(f"[警告] 載入現有 Token 失敗，將重新授權: {e}", file=sys.stderr)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"[警告] 刷新 Token 失敗，將重新進行授權: {e}", file=sys.stderr)
                creds = None

        if not creds or not creds.valid:
            if not os.path.exists(client_secret_path):
                raise SystemExit(f"[錯誤] 找不到 OAuth client secret 檔案: {client_secret_path}")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, scopes)
                print("請訪問下方網址以授權此應用程式：") # 中文提示
                creds = flow.run_local_server(port=port or 0)
            except Exception as e:
                raise SystemExit(f"[錯誤] OAuth 互動授權失敗: {e}")

        try:
            with open(token_path, "w", encoding="utf-8") as token:
                token.write(creds.to_json())
        except Exception as e:
            raise SystemExit(f"[錯誤] 寫入 Token 檔案失敗 '{token_path}': {e}")

    return creds


# =========================
# 驗證：YDAO（Data API via OAuth）
# =========================
def probe_ydao(channel_env_id: str, env: Dict[str, str]) -> "ProbeResult":
    """
    驗證 YouTube Data API（OAuth）可用性，並比對 mine channel 與環境變數 CHANNEL_ID 是否一致。

    參數
    - channel_env_id: 期待的一致性頻道 ID（來自 ENV）
    - env: load_env 載入的設定集合

    回傳
    - ProbeResult: 成功/失敗與訊息；extra 內含 mine_channel_id（若成功）
    """
    scopes_str = env.get("YDAO_OAUTH_SCOPES", "https://www.googleapis.com/auth/youtube,https://www.googleapis.com/auth/youtube.force-ssl")
    scopes = [s.strip() for s in scopes_str.split(",") if s.strip()]
    token_path = env.get("YDAO_TOKEN_PATH") or env.get("GOOGLE_TOKEN_PATH") or "credentials/youtube_oauth_token.json"
    client_path = env.get("YDAO_CREDENTIALS_PATH") or env.get("GOOGLE_CREDENTIALS_PATH") or "credentials/youtube_oauth_client_secret.json"
    port = int(env.get("YDAO_OAUTH_PORT", "0"))

    try:
        creds = get_oauth_credentials(scopes, token_path=token_path, client_secret_path=client_path, port=port)
        yt = build("youtube", "v3", credentials=creds)

        # 取得我的頻道資料，驗證 OAuth 與 Data API 可用，並比對 CHANNEL_ID
        me = yt.channels().list(part="id,snippet", mine=True, maxResults=1).execute()
        items = me.get("items", [])
        if not items:
            return ProbeResult("ydao", False, "找不到已授權使用者的頻道。")
        my_channel_id = items[0].get("id")
        ok = (my_channel_id == channel_env_id)
        msg = f"Data OAuth 正常。我的頻道 ID={my_channel_id}。與環境變數 CHANNEL_ID 符合={ok}"
        return ProbeResult("ydao", ok, msg, extra={"mine_channel_id": my_channel_id})
    except Exception as e:
        return ProbeResult("ydao", False, f"Data OAuth 失敗: {e.__class__.__name__}: {e}")


# =========================
# 驗證：YAAO（Analytics API via OAuth）
# =========================
def probe_yaao(channel_env_id: str, start_date: str, end_date: str, env: Dict[str, str]) -> "ProbeResult":
    """
    驗證 YouTube Analytics API（OAuth）可用性。

    步驟
    1) 以 ids='channel==MINE' 查詢 views 以確認授權有效。
    2) 再以指定 channel_id 呼叫一次以進行一致性驗證。

    參數
    - channel_env_id: 期待的一致性頻道 ID（來自 ENV）
    - start_date: 查詢起始（YYYY-MM-DD）
    - end_date: 查詢結束（YYYY-MM-DD）
    - env: load_env 載入的設定集合

    回傳
    - ProbeResult: 成功/失敗的結果與訊息；extra 可能包含推測的 channel_id（目前多為 None）
    """
    scopes_str = env.get("YAAO_OAUTH_SCOPES", "https://www.googleapis.com/auth/yt-analytics.readonly,https://www.googleapis.com/auth/yt-analytics-monetary.readonly")
    scopes = [s.strip() for s in scopes_str.split(",") if s.strip()]
    token_path = env.get("YAAO_TOKEN_PATH") or env.get("GOOGLE_TOKEN_PATH") or "credentials/analytics_oauth_token.json"
    client_path = env.get("YAAO_CREDENTIALS_PATH") or env.get("GOOGLE_CREDENTIALS_PATH") or "credentials/analytics_oauth_client_secret.json"
    port = int(env.get("YAAO_OAUTH_PORT", "0"))

    try:
        creds = get_oauth_credentials(scopes, token_path=token_path, client_secret_path=client_path, port=port)
        analytics = build("youtubeAnalytics", "v2", credentials=creds)

        # 先用 MINE 查詢一次 views 驗證授權
        analytics.reports().query(
            ids="channel==MINE",
            startDate=start_date,
            endDate=end_date,
            metrics="views",
            dimensions="day",
            maxResults=1
        ).execute()

        # 嘗試從 response 推測 channelId（部分情境不提供，這裡先保留 None）
        inferred_id: Optional[str] = None

        # 再以指定 channel_id 呼叫一次做一致性驗證
        analytics.reports().query(
            ids=f"channel=={channel_env_id}",
            startDate=start_date,
            endDate=end_date,
            metrics="views",
            maxResults=1
        ).execute()

        ok = True  # 兩次呼叫皆成功視為 OK
        msg = "Analytics OAuth 正常。針對 MINE 與指定頻道的報表查詢皆成功。"
        return ProbeResult("yaao", ok, msg, extra={"inferred_channel_id": inferred_id})
    except Exception as e:
        return ProbeResult("yaao", False, f"Analytics OAuth 失敗: {e.__class__.__name__}: {e}")


# =========================
# 驗證：YPKG（Public API Key）
# =========================
def probe_ypkg(channel_id: str, start_iso: str, end_iso: str, env: Dict[str, str]) -> "ProbeResult":
    """
    驗證公開 API Key 可用性，並回傳指定期間內的影片數量。

    參數
    - channel_id: 頻道 ID
    - start_iso: RFC3339 開始時間（含）
    - end_iso: RFC3339 結束時間（含）
    - env: 環境設定（需含 YPKG_ENABLE 與 YPKG_API_KEY）

    回傳
    - ProbeResult: 成功/失敗與訊息（成功包含影片數量）

    例外
    - 網路或 API 錯誤會被捕捉並包裝成失敗訊息。
    """
    enabled = parse_bool(env.get("YPKG_ENABLE", "true"))
    api_key = env.get("YPKG_API_KEY")
    if not enabled:
        return ProbeResult("ypkg", True, "公開 API 檢測已略過 (YPKG_ENABLE=false)。")
    if not api_key:
        return ProbeResult("ypkg", False, "公開 API 檢測失敗: 未提供 YPKG_API_KEY。")

    try:
        total, _ = count_videos_in_range(api_key, channel_id, start_iso, end_iso)
        return ProbeResult("ypkg", True, f"公開 API Key 正常。區間內影片總數={total}")
    except SystemExit as e:
        return ProbeResult("ypkg", False, f"{e}")
    except Exception as e:
        return ProbeResult("ypkg", False, f"公開 API 檢測失敗: {e.__class__.__name__}: {e}")


# =========================
# 輔助：一致性檢查（mine vs env）
# =========================
def verify_consistency(env_channel_id: str, ydao_result: ProbeResult) -> ProbeResult:
    """
    比對 YDAO 回傳的 mine_channel_id 與環境 CHANNEL_ID 是否一致。

    回傳
    - ProbeResult: name='consistency'，ok=True/False，extra 包含 mine 與 env
    """
    try:
        mine = (ydao_result.extra or {}).get("mine_channel_id")
        ok = bool(mine) and (mine == env_channel_id)
        msg = f"CHANNEL_ID 一致性 (YDAO mine vs ENV): {ok} (mine={mine}, env={env_channel_id})"
        return ProbeResult("consistency", ok, msg, extra={"mine": mine, "env": env_channel_id})
    except Exception as e:
        return ProbeResult("consistency", False, f"一致性檢查失敗: {e.__class__.__name__}: {e}")


# =========================
# 封裝：對外可呼叫 API
# =========================
def run_verifications(env: Optional[Dict[str, str]] = None) -> ProbeOutputs:
    """
    執行所有驗證並回傳結構化結果，給其他Python程式調用。

    參數
    - env: 若提供，使用該設定（keys 同 load_env）；若為 None，將呼叫 load_env() 從 .env/環境取得。

    回傳
    - ProbeOutputs: 包含 ypkg/db/ydao/yaao/consistency 的 ProbeResult 結果物件。
    """
    # 允許外部呼叫時直接傳入 env；若未傳入，則由本函式自行載入
    local_env = env or load_env()

    channel_id = local_env["CHANNEL_ID"]
    start_date = local_env["START_DATE"]
    end_date = local_env["END_DATE"]
    db_url = local_env["DB_URL"]

    start_iso, end_iso = iso_date_boundaries(start_date, end_date)

    # 個別執行 probe，互不影響
    r_ypkg = probe_ypkg(channel_id, start_iso, end_iso, local_env)
    r_db = test_mysql_connection(db_url)
    r_ydao = probe_ydao(channel_id, local_env)
    r_yaao = probe_yaao(channel_id, start_date, end_date, local_env)
    r_consistency = verify_consistency(channel_id, r_ydao)

    return ProbeOutputs(
        ypkg=r_ypkg,
        db=r_db,
        ydao=r_ydao,
        yaao=r_yaao,
        consistency=r_consistency,
    )


def verify_all(env: Optional[Dict[str, str]] = None) -> Tuple[bool, List[bool]]:
    """
    提供簡潔的布林結果給其他程式呼叫。

    回傳
    - overall: 全部通過才 True
    - flags: 依序為 [ypkg, db, ydao, yaao, consistency]
    """
    outputs = run_verifications(env)
    flags = [
        outputs.ypkg.ok,
        outputs.db.ok,
        outputs.ydao.ok,
        outputs.yaao.ok,
        outputs.consistency.ok,
    ]
    overall = all(flags)
    return overall, flags


# =========================
# CLI 與主流程
# =========================
def run_probe() -> None:
    """
    命令列進入點：執行各項探針，印出摘要，並將結果寫入檔案。

    行為
    - 讀取環境變數與日期界線
    - 依使用者參數選擇執行 public/db/oauth-data/oauth-analytics 或全部
    - 印出人類可讀的摘要
    - 將結果寫入 OUTPUT_DIR/probe_summary.json
    - 若任一關鍵驗證失敗，回傳代碼 1
    """
    import argparse

    clear_terminal()

    parser = argparse.ArgumentParser(description="Run probes for YouTube Data/Analytics and DB connectivity.")
    parser.add_argument(
        "--check",
        choices=["all", "public", "oauth-data", "oauth-analytics", "data", "analytics", "db"],
        default="all",
        help="Which probe to run. 'data'='public', 'analytics'='oauth-analytics' for convenience."
    )
    args = parser.parse_args()

    env = load_env()

    channel_id = env["CHANNEL_ID"]
    start_date = env["START_DATE"]
    end_date = env["END_DATE"]
    db_url = env["DB_URL"]

    output_dir = env.get("OUTPUT_DIR", "data")
    log_dir = env.get("LOG_DIR", "logs")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    start_iso, end_iso = iso_date_boundaries(start_date, end_date)

    # 別名對齊
    check = args.check
    if check == "data":
        check = "public"
    if check == "analytics":
        check = "oauth-analytics"

    results: List[ProbeResult] = []

    print("===========================================================================")
    print("=== 探針設定 (Probes Configuration) ===")
    print(f"- 頻道 ID (Channel ID)  : {channel_id}")
    print(f"- 日期範圍 (Date Range) : {start_date} ~ {end_date}")
    print(f"- 資料庫主機 (DB Host)  : {db_url.split('@')[-1] if '@' in db_url else db_url}")
    print(f"- 輸出目錄 (OUTPUT_DIR) : {output_dir}")
    print(f"- 日誌目錄 (LOG_DIR)    : {log_dir}")
    print(f"- 啟用 YPKG (Public)    : {env.get('YPKG_ENABLE', 'true')}")
    print(f"- YDAO 權限範圍         : {env.get('YDAO_OAUTH_SCOPES', '(default)')}")
    print(f"- YAAO 權限範圍         : {env.get('YAAO_OAUTH_SCOPES', '(default)')}")
    print("")

    # 依指令執行
    if check in ("all", "public"):
        print("[1/?] 正在檢測公開 API (YPKG)...")
        results.append(probe_ypkg(channel_id, start_iso, end_iso, env))
    if check in ("all", "db"):
        print("[?/ ?] 正在測試 MySQL 連線...")
        results.append(test_mysql_connection(db_url))
    if check in ("all", "oauth-data"):
        print("[?/ ?] 正在檢測 Data API OAuth (YDAO)...")
        results.append(probe_ydao(channel_id, env))
    if check in ("all", "oauth-analytics"):
        print("[?/ ?] 正在檢測 Analytics API OAuth (YAAO)...")
        results.append(probe_yaao(channel_id, start_date, end_date, env))

    # 一致性檢查彙整
    consistency_notes: List[str] = []
    ydao_res = next((r for r in results if r.name == "ydao"), None)
    if ydao_res and ydao_res.ok and ydao_res.extra and ydao_res.extra.get("mine_channel_id"):
        mine_id = ydao_res.extra["mine_channel_id"]
        consistency_notes.append(f"CHANNEL_ID 一致性 (YDAO mine vs ENV): {mine_id == channel_id} (mine={mine_id}, env={channel_id})")
    # 摘要輸出
    print("\n=== 執行摘要 (Summary) ===")
    for r in results:
        status = "OK" if r.ok else "FAILED"
        print(f"- {r.name:>12}: {status} - {r.message}")

    for note in consistency_notes:
        print(f"- Consistency: {note}")

    # 將結果輸出到檔案
    summary_path = os.path.join(output_dir, "probe_summary.json")
    try:
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "results": [r.__dict__ for r in results],
                    "consistency": consistency_notes,
                    "channel_id_env": channel_id,
                    "date_range": {"start": start_date, "end": end_date},
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"\n摘要已儲存至: {summary_path}")
    except Exception as e:
        print(f"[警告] 寫入摘要檔案失敗: {e}", file=sys.stderr)

    # 退出碼策略
    #critical_fail = any(r.name in ("ypkg", "ydao", "yaao", "db") and not r.ok for r in results)
    #sys.exit(1 if critical_fail else 0)

if __name__ == "__main__":
    run_probe()