# 路徑：scripts/track_velocity.py
import sys
import time, random
import typer
from rich.console import Console
from typing import Optional, Dict, Any

# 載入專案設定（.env 等）
from scripts.utils.env import load_settings
# 頻道日更 ingestion 與頻道 ID 辨識工具
from scripts.ingestion.channel_daily import ingest_channel_daily, _resolve_channel_id
# 清空終端畫面的小工具（純顯示用途）
from scripts.utils.terminal import clear_terminal
# Pipeline 執行與通知（含重試、彙整結果、發送通知）
from scripts.notifications.runner import run_pipeline_and_notify
# 備註：notify_all 目前在此檔未使用，若未被其他模組引用，可移除以避免未使用 import 的警告
from scripts.notifications.senders import notify_all  # noqa: F401
from scripts.db.db import get_engine
from scripts.channel.ensure import ensure_dim_channel_exists
# 影片抓取與熱門影片分析服務
from scripts.services.video_ingestion import run_fetch_videos
# 新增：排行榜看板服務
from scripts.services.ranking_dashboard import run_ranking_update

# 建立 Typer 應用程式，並提供全域 help 描述
app = typer.Typer(help="YouTube Data Pipeline track_velocity", invoke_without_command=True)

# 子命令：update_rankings
# 功能：更新 Discord 看板
@app.command("update_rankings")
def cmd_update_rankings():
    """
    更新 Discord 排行榜看板 (Top 10)
    如果不指定 category，將自動更新所有榜單。
    """
    # 讀取設定與解析頻道 ID
    cfg = load_settings()
    
    # 定義所有需要更新的榜單
    all_categories = ['15min', 'hourly', 'daily', 'weekly', 'monthly']
            
    print(f">> 準備更新排行榜: {all_categories}")
    
    for cat in all_categories:
        try:
            # 將 settings 傳遞給服務函式
            run_ranking_update(cat, cfg)
        except Exception as e:
            print(f"❌ 更新 {cat} 榜單時發生錯誤: {e}")

# 子命令：fetch_videos
# 功能：列出頻道上傳清單並批次抓取影片詳情，更新本地資料（dim_video 等）
@app.command("fetch_videos")
def cmd_fetch_videos(
    channel_id: Optional[str] = typer.Option(None, "--channel-id", "-c", help="YouTube channel id；預設讀取 .env CHANNEL_ID"),
    max_results: int = typer.Option(50, "--max-results", "-m", min=1, max=50, help="videos.list 每批上限；預設 50"),
    published_after: Optional[str] = typer.Option(None, "--published-after", help="YYYY-MM-DD"),
    published_before: Optional[str] = typer.Option(None, "--published-before", help="YYYY-MM-DD"),
):
    """
    每天頻道影片資料更新：
      - 列出 uploads 播放清單 video_id
      - videos.list 批次抓取詳情（最多 50/批）
      - 依 shorts_check 規則 upsert 到 dim_video
    """
    # 讀取設定與解析頻道 ID
    cfg = load_settings()
    cid = _resolve_channel_id(channel_id, cfg)

    # 執行影片抓取流程
    run_fetch_videos(
        channel_id=cid,
        max_results=max_results,
        published_after=published_after,
        published_before=published_before,
        settings=cfg,
    )

# 子命令：run_all（預設主流程）
# 功能：以統一的重試與通知機制，依序執行四個步驟：ingest_channel_daily → fetch_videos → top_videos → update_playlists
@app.command("run_all")
def cmd_run_all(
    channel_id: Optional[str] = typer.Option(None, "--channel-id", "-c", help="YouTube channel id；預設讀取 .env CHANNEL_ID"),

    # fetch_videos 相關參數
    fv_max_results: int = typer.Option(50, "--fv-max-results", min=1, max=50, help="fetch_videos: videos.list 每批上限"),
    fv_published_after: Optional[str] = typer.Option(None, "--fv-published-after", help="fetch_videos: YYYY-MM-DD"),
    fv_published_before: Optional[str] = typer.Option(None, "--fv-published-before", help="fetch_videos: YYYY-MM-DD"),

    # 重試設定（run_all 全域）
    max_retries: int = typer.Option(3, "--max-retries", help="每個步驟最多重試次數（不含首次執行）"),
    backoff_base: float = typer.Option(2.0, "--backoff-base", help="指數退避底數，>1"),
    backoff_initial: float = typer.Option(1.0, "--backoff-initial", help="首次重試等待秒數"),
    backoff_max: float = typer.Option(30.0, "--backoff-max", help="單次重試最大等待秒數"),
):
    """
    依序執行四個步驟（任一步驟失敗則中止）並具備重試機制：
      1) fetch_videos
      2) update_rankings (更新 Discord 看板)
    遇到 403（例如配額用盡）或其他明確 4xx 錯誤時不重試，直接中止。
    """
    # 美化輸出（分隔線、標題等）
    console = Console()

    # 讀取設定與解析頻道 ID
    cfg = load_settings()
    cid = _resolve_channel_id(channel_id, cfg)
    console.rule(f"Run All Pipeline for 頻道ID={cid}")

    # 初始化 DB 與確保頻道存在（若無則建立 dim_channel 基本資料）
    engine = get_engine()
    ch_payload = ensure_dim_channel_exists(engine, cid)
    name = (ch_payload or {}).get("channel_name")
    console.rule(f"Run All Pipeline for 頻道名稱={name}")

    # 判斷是否值得重試的錯誤類型（依訊息字串判定）
    def _should_retry(exc: Exception) -> bool:
        msg = str(exc).lower()
        # 授權/配額等用戶側錯誤，不重試
        if "403" in msg or "http 403" in msg or "quota" in msg:
            return False
        if "http 4" in msg or " 4xx" in msg:
            return False
        # 部分 Windows 網路錯誤可重試
        if "winerror" in msg:
            return True
        # 常見暫時性錯誤關鍵字：逾時、連線中斷、5xx、429 等
        transient_keywords = [
            "timeout", "timed out", "time-out",
            "connection reset", "connection aborted", "connection refused",
            "temporarily unavailable", "try again", "unavailable",
            "server error", "http 5", " 5xx",
            "rate limit", "too many requests", "429",
        ]
        return any(k in msg for k in transient_keywords)

    # 計算每次重試的等待秒數，含抖動（jitter）
    def _sleep_for_retry(attempt_idx: int):
        # 第一次重試使用 backoff_initial，之後依照指數退避上升，但不超過 backoff_max
        wait = min(backoff_initial * (backoff_base ** (attempt_idx - 1)), backoff_max)
        jitter = wait * random.uniform(0.8, 1.2)
        time.sleep(jitter)
        return jitter

    # 將四個步驟以統一規格描述，交由 runner 處理重試與序列執行
    steps_spec = [
        {
            "name": "fetch_videos",
            "fn": run_fetch_videos,
            "args": [],
            "kwargs": {
                "channel_id": cid,
                "max_results": fv_max_results,
                "published_after": fv_published_after,
                "published_before": fv_published_before,
                "settings": cfg,
            },
        },
        {
            "name": "update_rankings",
            "fn": cmd_update_rankings,
            "args": [],
            "kwargs": {
            },
        },
    ]

    # 統一交給 runner 執行（內含：序列執行、錯誤攔截、是否重試、通知彙整）
    exit_code = run_pipeline_and_notify(
        cfg=cfg,
        console=console,
        steps_spec=steps_spec,
        should_retry=_should_retry,
        sleep_for_retry=_sleep_for_retry,
        max_retries=max_retries,
    )

    # 輸出結束線與狀態
    console.rule("All done" + (" (success)" if exit_code == 0 else " (failed)"))
    # 以 exit code 結束，提供給外部（shell/CI）判斷成功或失敗
    raise typer.Exit(code=exit_code)

# main：當作預設子命令的包裝器
# - 讓沒有經過 Typer 的情況下也能直接執行 run_all（例如 __main__ 分支在無參數時）
def main():
    # 直接當作「預設子命令」執行 run_all
    return cmd_run_all(
        channel_id=None,

        # fetch_videos 相關參數
        fv_max_results=50,
        fv_published_after=None,
        fv_published_before=None,

        # 重試設定（run_all 全域）
        max_retries=3,
        backoff_base=2.0,
        backoff_initial=1.0,
        backoff_max=30.0,
        )

# 入口點
if __name__ == "__main__":
    # 清空終端畫面，讓輸出更乾淨
    clear_terminal()    

    # 行為說明：
    # - 若「沒有帶任何子命令或參數」，則視為想要一鍵執行完整管線 → 呼叫 main()（即 run_all）
    # - 否則，交由 Typer 分派對應的子命令（run_all / fetch_videos / top_videos / update_playlists / ingest_channel_daily）
    if len(sys.argv) <= 1:
        # 例如：python -m scripts.cli
        try:
            from scripts.run_probe import run_probe
            run_probe()
            code = main()
            # 註：cmd_run_all 內部最後會 raise typer.Exit，通常不會回傳到這行
        except typer.Exit as e:
            # 將 Typer 的 Exit 統一轉成系統退出碼
            raise SystemExit(e.exit_code)
    else:
        # 例如：
        # - python -m scripts.cli run_all --tv-top 20
        # - python -m scripts.cli fetch_videos --max-results 10
        # - python -m scripts.cli update_playlists --dry-run
        # - python -m scripts.cli ingest_channel_daily
        app()