# 路徑：scripts/run_probe.py
import os
import sys
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# OAuth / Google API 客戶端
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# 引入自定義模組
from scripts.utils.terminal import clear_terminal
from scripts.utils.env import load_settings  # 統一設定讀取

# =========================
# 資料類型
# =========================
@dataclass
class ProbeResult:
    """
    表示單一探針（probe）執行結果的資料結構。
    """
    name: str
    ok: bool
    message: str
    extra: Optional[Dict[str, Any]] = None

@dataclass
class ProbeOutputs:
    """
    封裝探針結果：僅包含 Public API(ypkg), DB, Data OAuth(ydao) 與一致性檢查。
    已移除 YAAO。
    """
    ypkg: ProbeResult
    db: ProbeResult
    ydao: ProbeResult
    consistency: ProbeResult


# =========================
# 工具函式
# =========================
def parse_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")

def iso_date_boundaries(start_str: str, end_str: str) -> Tuple[str, str]:
    """
    轉換日期為 RFC3339 格式 (用於 Data API Search)。
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

def ensure_dir_for(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# =========================
# 驗證：Public(API Key) - Data API v3
# =========================
def count_videos_in_range(api_key: str, channel_id: str, start_iso: str, end_iso: str) -> Tuple[int, Optional[str]]:
    """
    使用 YouTube Search API 計算特定頻道於時間範圍內發佈的影片數量。
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


def probe_ypkg(channel_id: str, start_iso: str, end_iso: str, env: Dict[str, str]) -> "ProbeResult":
    """
    驗證公開 API Key 可用性。
    """
    enabled = parse_bool(env.get("YPKG_ENABLE", "true"))
    
    # 處理 API Key 的 fallback (YPKG_API_KEY 優先，YT_API_KEY 為舊版相容)
    api_key = env.get("YPKG_API_KEY") or env.get("YT_API_KEY")
    
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
# 驗證：DB
# =========================
def test_mysql_connection(db_url: str) -> "ProbeResult":
    """
    測試 MySQL 連線是否正常。
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
def get_oauth_credentials(
    scopes: List[str],
    token_path: Optional[str] = None,
    client_secret_path: Optional[str] = None,
    port: Optional[int] = None,
    interactive: bool = False,
) -> Credentials:
    """
    取得或建立 OAuth 憑證。
    包含自動刷新重試機制，避免因短暫網路問題導致跳出重新授權。
    :param interactive: 
        True: 當 Token 失效且無法刷新時，允許跳出瀏覽器進行人工授權。
        False: 自動化模式。若 Token 失效，直接拋出錯誤，不卡住程式。
    """
    token_path = token_path or "token.json"
    client_secret_path = client_secret_path or "client_secret.json"
    ensure_dir_for(token_path)

    creds: Optional[Credentials] = None

    # 1. 嘗試讀取現有 Token
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, scopes)
        except Exception as e:
            print(f"[警告] 載入現有 Token 失敗，將重新授權: {e}", file=sys.stderr)
            creds = None

    # 2. 檢查是否有效或過期
    if not creds or not creds.valid:
        # 如果有過期的憑證且有 refresh_token，嘗試刷新
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            
            # === [修改點] 加入重試機制 ===
            max_retries = 3
            refresh_success = False
            
            for i in range(max_retries):
                try:
                    # 建立 Request 物件 (預設 timeout 可能較短，這裡依賴重試)
                    req = Request()
                    print(f"正在嘗試刷新 Token (第 {i+1}/{max_retries} 次)...")
                    creds.refresh(req)
                    refresh_success = True
                    print("Token 刷新成功！")
                    break  # 成功則跳出迴圈
                except Exception as e:
                    print(f"[警告] 刷新 Token 失敗 ({i+1}/{max_retries}): {e}", file=sys.stderr)
                    if i < max_retries - 1:
                        # 指數退避或固定等待
                        wait_sec = 3 * (i + 1)  # 失敗後等待 3, 6, 9 秒
                        print(f"等待 {wait_sec} 秒後重試...", file=sys.stderr)
                        time.sleep(wait_sec)
            
            # 如果重試多次後仍然失敗，才將 creds 設為 None (觸發重新授權)
            if not refresh_success:
                print(f"[錯誤] 重試 {max_retries} 次後仍無法刷新 Token，可能是 Refresh Token 失效或網路持續不通。", file=sys.stderr)
                
                creds = None # 標記為無效，進入下一步判斷
            # =============================

        # 3. 若無憑證或刷新失敗，執行互動式授權
        if not creds or not creds.valid:
            # === [關鍵防卡機制] ===
            if not interactive:
                # 自動化模式：絕對不要開啟瀏覽器，直接報錯
                # 這個錯誤會被外層的 retry 機制捕獲 (如果是網路問題導致刷新失敗)
                # 或者直接讓程式崩潰 (如果是真的過期)，避免卡死
                raise RuntimeError("OAuth Token 已失效且無法刷新，且目前處於非互動模式 (interactive=False)。請手動執行 run_probe 進行授權。")
            
            # 互動模式：才允許執行 flow.run_local_server
            if not os.path.exists(client_secret_path):
                raise SystemExit(f"[錯誤] 找不到 OAuth client secret 檔案: {client_secret_path}")
            try:
                print("[資訊] 啟動互動式授權流程 (將開啟瀏覽器)...")
                flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, scopes)
                print("請訪問下方網址以授權此應用程式：")
                creds = flow.run_local_server(port=port or 0)
            except Exception as e:
                raise SystemExit(f"[錯誤] OAuth 互動授權失敗: {e}")

        # 4. 寫回 Token 檔案
        if creds and creds.valid:
            try:
                with open(token_path, "w", encoding="utf-8") as token:
                    token.write(creds.to_json())
            except Exception as e:
                print(f"[警告] 寫入 Token 檔案失敗: {e}", file=sys.stderr)

    return creds


# =========================
# 驗證：YDAO（Data API via OAuth）
# =========================
def probe_ydao(channel_env_id: str, env: Dict[str, str], interactive: bool = True) -> "ProbeResult":
    """
    驗證 YouTube Data API（OAuth）可用性。
    """
    scopes_str = env.get("YDAO_OAUTH_SCOPES", "https://www.googleapis.com/auth/youtube,https://www.googleapis.com/auth/youtube.force-ssl")
    scopes = [s.strip() for s in scopes_str.split(",") if s.strip()]
    
    # 使用 YDAO 開頭的變數
    token_path = env.get("YDAO_TOKEN_PATH") 
    client_path = env.get("YDAO_CREDENTIALS_PATH") 
    port = int(env.get("YDAO_OAUTH_PORT", "0"))

    try:
        # 這裡設定 interactive=True，因為 run_probe 是診斷工具，允許使用者登入
        creds = get_oauth_credentials(
            scopes, 
            token_path=token_path, 
            client_secret_path=client_path, 
            port=port,
            interactive=interactive 
        )
        yt = build("youtube", "v3", credentials=creds)

        # 取得我的頻道資料，驗證 OAuth 與 Data API 可用
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
# 輔助：一致性檢查（mine vs env）
# =========================
def verify_consistency(env_channel_id: str, ydao_result: ProbeResult) -> ProbeResult:
    """
    比對 YDAO 回傳的 mine_channel_id 與環境 CHANNEL_ID 是否一致。
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
    執行 DB 與 Data API 驗證並回傳結構化結果。
    """
    # 使用 scripts.utils.env.load_settings 統一載入
    local_env = env or load_settings()

    channel_id = local_env["CHANNEL_ID"]
    start_date = local_env["START_DATE"]
    end_date = local_env["END_DATE"]
    db_url = local_env["DB_URL"]

    start_iso, end_iso = iso_date_boundaries(start_date, end_date)

    # 執行 Probe
    r_ypkg = probe_ypkg(channel_id, start_iso, end_iso, local_env)
    r_db = test_mysql_connection(db_url)
    r_ydao = probe_ydao(channel_id, local_env)
    r_consistency = verify_consistency(channel_id, r_ydao)

    return ProbeOutputs(
        ypkg=r_ypkg,
        db=r_db,
        ydao=r_ydao,
        consistency=r_consistency,
    )


def verify_all(env: Optional[Dict[str, str]] = None) -> Tuple[bool, List[bool]]:
    """
    提供簡潔的布林結果給其他程式呼叫。
    回傳 flags 順序: [ypkg, db, ydao, consistency]
    """
    outputs = run_verifications(env)
    flags = [
        outputs.ypkg.ok,
        outputs.db.ok,
        outputs.ydao.ok,
        outputs.consistency.ok,
    ]
    overall = all(flags)
    return overall, flags


# =========================
# CLI 與主流程
# =========================
def run_probe(interactive_mode: bool = True) -> None:
    """
    命令列進入點：執行 DB 與 Data API 探針。
    """
    import argparse
    if interactive_mode:
        clear_terminal()
    call_mode = "自動模式" if not interactive_mode else "手動模式"
    print(f"目前互動模式：{call_mode}")
    # 只有在直接執行此腳本時，才解析 argparse
    # 如果是被 import 呼叫，我們通常希望執行所有檢查
    args_check = "all"
    
    # 簡單判斷：如果是從 command line 執行且有參數，才跑 argparse
    # 這樣避免 track_velocity 的參數干擾到這裡
    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Run probes for YouTube Data API and DB connectivity.")
        parser.add_argument("--check", choices=["all", "public", "oauth", "db"], default="all")
        args = parser.parse_args()
        args_check = args.check

    # 1. 載入設定 (包含 .env 與系統環境變數)
    env = load_settings()

    channel_id = env["CHANNEL_ID"]
    start_date = env["START_DATE"]
    end_date = env["END_DATE"]
    db_url = env["DB_URL"]
    output_dir = env["OUTPUT_DIR"]
    log_dir = env["LOG_DIR"]

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    start_iso, end_iso = iso_date_boundaries(start_date, end_date)

    results: List[ProbeResult] = []

    print("===========================================================================")
    print("=== 探針設定 (Probes Configuration: DB & Data API) ===")
    print(f"- 頻道 ID (Channel ID)  : {channel_id}")
    print(f"- 日期範圍 (Date Range) : {start_date} ~ {end_date}")
    print(f"- 資料庫主機 (DB Host)  : {db_url.split('@')[-1] if '@' in db_url else db_url}")
    print(f"- 啟用 YPKG (Public)    : {env.get('YPKG_ENABLE', 'true')}")
    print(f"- YDAO 權限範圍         : {env.get('YDAO_OAUTH_SCOPES', '(default)')}")
    print("")

    # 依指令執行
    if args_check in ("all", "public"):
        print("[1/?] 正在檢測公開 API (YPKG)...")
        results.append(probe_ypkg(channel_id, start_iso, end_iso, env))
    
    if args_check in ("all", "db"):
        print("[?/ ?] 正在測試 MySQL 連線...")
        results.append(test_mysql_connection(db_url))
    
    if args_check in ("all", "oauth"):
        print("[?/ ?] 正在檢測 Data API OAuth (YDAO)...")
        results.append(probe_ydao(channel_id, env, interactive=interactive_mode))

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
    # [新增] 如果是非互動模式，且有任何失敗，應該拋出例外讓外層知道
    # 這樣 track_velocity 才能捕捉到錯誤並決定是否重試
    if not interactive_mode:
        failures = [r for r in results if not r.ok]
        if failures:
            fail_msg = "; ".join([f"{r.name}: {r.message}" for r in failures])
            raise RuntimeError(f"探針檢測失敗: {fail_msg}")

if __name__ == "__main__":
    run_probe(interactive_mode=True)


# ==================================================================================
# UNUSED CODE (DEPRECATED / REMOVED)
# ==================================================================================
"""
# 舊版 load_env (已由 scripts.utils.env.load_settings 取代)
def load_env() -> Dict[str, str]:
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
    # ... (其餘舊邏輯)
    return values

# 舊版 Analytics (YAAO) 驗證邏輯
def probe_yaao(channel_env_id: str, start_date: str, end_date: str, env: Dict[str, str]) -> "ProbeResult":
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
        
        # 再以指定 channel_id 呼叫一次做一致性驗證
        analytics.reports().query(
            ids=f"channel=={channel_env_id}",
            startDate=start_date,
            endDate=end_date,
            metrics="views",
            maxResults=1
        ).execute()

        ok = True
        msg = "Analytics OAuth 正常。針對 MINE 與指定頻道的報表查詢皆成功。"
        return ProbeResult("yaao", ok, msg, extra={"inferred_channel_id": None})
    except Exception as e:
        return ProbeResult("yaao", False, f"Analytics OAuth 失敗: {e.__class__.__name__}: {e}")
"""
