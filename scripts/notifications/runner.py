# scripts/notifications/runner.py
# 總覽：
# - run_step_with_retry：以統一重試機制執行單一步驟，回傳成功/失敗與耗時等資訊。
# - run_pipeline_and_notify：依步驟規格順序執行整個 Pipeline，產出摘要、寫入日誌並觸發通知。
# - format_summary_text/notify_all：由 senders 模組提供，這裡負責組裝摘要與呼叫多通道通知。
# - 其餘輔助：參數淨化（避免保留鍵衝突）、建立 logs 目錄、寫入結果檔案。

import os
import time
import traceback
import sys
import io
from typing import Callable, Dict, Any, List

from .senders import notify_all, format_summary_text

# 保留鍵：steps_spec 的 kwargs 中不應傳遞這些鍵到實際步驟函式，避免與 runner 控制參數衝突
RESERVED_KW_KEYS = {"name", "fn", "should_retry", "sleep_for_retry", "max_retries", "console"}

# --- [新增] 1. 用於同時顯示並捕捉輸出的工具類別 ---
class StreamTee:
    """
    一個類似 Tee 的串流工具，會將寫入的內容同時送到：
    1. 原始串流 (例如螢幕/終端機)
    2. 內部的 StringIO (用於捕捉字串)
    """
    def __init__(self, original_stream):
        self.original_stream = original_stream
        self.capture_buffer = io.StringIO()

    def write(self, data):
        # 1. 寫入原始位置 (只有當原始串流存在時才寫入，避免 NoneType 錯誤)
        if self.original_stream:
            try:
                self.original_stream.write(data)
                # 確保即時刷新，讓 Log 在終端機即時顯示
                self.original_stream.flush()
            except Exception:
                # 萬一原始串流寫入失敗 (例如 pipe 斷裂)，忽略錯誤以保證程式繼續執行
                pass
        
        # 2. 寫入緩衝區 (存起來發報告用，這部分一定要執行)
        self.capture_buffer.write(data)

    def flush(self):
        if self.original_stream:
            try:
                self.original_stream.flush()
            except Exception:
                pass
        self.capture_buffer.flush()

    def get_captured_text(self):
        return self.capture_buffer.getvalue()

# ------------------------------------------------

def run_step_with_retry(
    name: str,
    fn: Callable,
    should_retry: Callable[[Exception], bool],
    sleep_for_retry: Callable[[int], float],
    max_retries: int,
    console,
    *args,
    **kwargs
) -> Dict[str, Any]:
    """
    以一致的重試策略執行單一步驟。
    參數：
      - name：步驟名稱（用於日誌與回傳）
      - fn：實際要執行的函式
      - should_retry：判斷遇到例外是否該重試的函式，簽名 (Exception) -> bool
      - sleep_for_retry：執行等待並回傳等待秒數的函式，簽名 (attempt_idx: int) -> float
      - max_retries：最多可重試次數（總嘗試次數 = 1 + max_retries）
      - console：用於輸出到終端（rich Console）
      - *args/**kwargs：傳遞給 fn 的參數
    回傳：
      - step_info：dict，包含 name/ok/attempts/elapsed/error/traceback
    行為：
      - 成功：立即回傳 ok=True
      - 失敗：依 should_retry 判斷並呼叫 sleep_for_retry，再次嘗試直至超過上限或不該重試

    (已修改) 增加 Log 捕捉功能
    """
    attempt = 0
    start_ts = time.time()

    # 準備捕捉器
    stdout_tee = StreamTee(sys.stdout)

    step_info: Dict[str, Any] = {
        "name": name,
        "ok": False,
        "attempts": 0,
        "elapsed": 0.0,
        "error": None,
        "traceback": None,
        "logs": ""  # [新增] 用來存放捕捉到的 Log
    }

    # 暫時替換系統 stdout，這樣 fn 裡面的 print 就會經過 StreamTee
    original_stdout = sys.stdout
    sys.stdout = stdout_tee

    try:
        while True:
            attempt += 1
            step_info["attempts"] = attempt
            console.print(f"[bold cyan]>> Start {name} (attempt {attempt})[/bold cyan]")
            
            try:
                # 執行實際步驟
                fn(*args, **kwargs)
                
                elapsed = time.time() - start_ts
                step_info["elapsed"] = elapsed
                step_info["ok"] = True
                console.print(f"[bold green]<< Success {name}[/bold green] ({elapsed:.2f}s)")
                
                # [新增] 成功後，把捕捉到的文字存入 step_info
                step_info["logs"] = stdout_tee.get_captured_text()
                return step_info
                
            except Exception as e:
                elapsed = time.time() - start_ts
                step_info["elapsed"] = elapsed
                step_info["error"] = str(e)
                step_info["traceback"] = traceback.format_exc()

                # 不重試的情況
                if (attempt > (1 + max_retries)) or (not should_retry(e)):
                    console.print(f"[bold red]<< Failed {name} ({elapsed:.2f}s): {e}[/bold red]")
                    # [新增] 失敗時也要存 Log，方便除錯
                    step_info["logs"] = stdout_tee.get_captured_text()
                    return step_info

                # 準備重試
                next_attempt = attempt + 1
                wait = sleep_for_retry(attempt_idx=attempt)
                console.print(f"[yellow].. Retried {name} after {wait:.1f}s (next attempt {next_attempt}/{1+max_retries})[/yellow]")
    
    finally:
        # [重要] 務必將 stdout 還原，避免影響後續程式
        sys.stdout = original_stdout

def _sanitize_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    清理步驟規格中的 kwargs：過濾掉 RESERVED_KW_KEYS，避免與 runner 內部控制參數衝突。
    例：steps_spec[i]["kwargs"] 可能包含 name/fn 等，這些不應傳入實際步驟函式。
    """
    if not kwargs:
        return {}
    return {k: v for k, v in kwargs.items() if k not in RESERVED_KW_KEYS}

def _ensure_logs_dir() -> str:
    """
    確保當前工作目錄下存在 logs 資料夾，若無則建立。
    回傳：logs 資料夾的絕對路徑
    """
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    return logs_dir

def _write_summary_log(text: str, status: str) -> str:
    """
    將摘要文字寫入 logs 檔案，檔名包含時間戳與狀態（OK/FAIL）。
    參數：
      - text：要寫入的摘要內容
      - status："成功"/"失敗"
    回傳：寫入的檔案路徑
    """
    logs_dir = _ensure_logs_dir()
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    suffix = "OK" if status == "成功" else "FAIL"
    path = os.path.join(logs_dir, f"pipeline_{ts}_{suffix}.log")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path

def run_pipeline_and_notify(
    cfg: Dict[str, str],
    console,
    steps_spec: List[Dict[str, Any]],
    should_retry,
    sleep_for_retry,
    max_retries: int
) -> int:
    """
    依 steps_spec 描述的步驟順序執行整個 Pipeline，並於結束時輸出摘要、寫入 log。
    若執行失敗，則額外發送通知。
    """
    started = time.time()
    steps_result: List[Dict[str, Any]] = []
    status = "成功"
    extra_details: List[str] = []

    try:
        for spec in steps_spec:
            # 取得步驟函式的 args/kwargs；kwargs 需先過濾保留鍵
            args = (spec.get("args", []) or [])
            kwargs = _sanitize_kwargs(spec.get("kwargs", {}) or {})

            # 以固定位置參數傳入 run_step_with_retry 的控制參數
            info = run_step_with_retry(
                spec["name"],         # name
                spec["fn"],           # fn
                should_retry,         # should_retry
                sleep_for_retry,      # sleep_for_retry
                max_retries,          # max_retries
                console,              # console
                *args,                # positional args for fn
                **kwargs,             # keyword args for fn
            )

            steps_result.append(info)
            
            # 將該步驟捕捉到的 Log 加入到 extra_details 中
            log_content = info.get("logs", "").strip()
            if log_content:
                extra_details.append(f"\n--- [{spec['name']}] 執行紀錄 ---")
                extra_details.append(log_content)
                
            if not info["ok"]:
                # 任一步驟失敗，標記總狀態為失敗並中斷迴圈
                status = "失敗"
                break

        if status == "成功":
            extra_details.append("Pipeline 全部步驟執行完成。")
        else:
            extra_details.append("Pipeline 中途失敗，已停止後續步驟。")

        exit_code = 0 if status == "成功" else 1
        return exit_code

    except Exception:
        # 捕捉未預期例外並記錄堆疊
        status = "失敗"
        extra_details.append(traceback.format_exc())
        return 1

    finally:
        # 無論成功或失敗，都組裝摘要並顯示在 Console
        summary_text = format_summary_text(status, started, steps_result, "\n".join(extra_details))
        console.rule("Pipeline Summary")
        console.print(summary_text)

        # 1. 寫入本地摘要檔 (無論成功失敗都執行)
        try:
            log_path = _write_summary_log(summary_text, status)
            console.print(f"[dim]Summary written to: {log_path}[/dim]")
        except Exception as log_err:
            console.print(f"[red]寫入 logs 失敗：[/red]{log_err}")

        # 2. 發送通知 (修改處：只有在 status 為 "失敗" 時才執行)
        if status == "失敗":
            try:
                console.print("[yellow]偵測到失敗，正在發送通知...[/yellow]")
                notify_all(cfg, status=status, started_at=started, steps=steps_result, extra_details="\n".join(extra_details))
            except Exception as notify_err:
                console.print(f"[red]通知發送失敗：[/red]{notify_err}")
        else:
            console.print("[dim]執行成功，略過發送通知。[/dim]")
