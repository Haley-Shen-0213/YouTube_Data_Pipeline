# 路徑: scripts/utils/terminal.py
# 用途: 清除終端畫面（Windows/macOS/Linux）

import os
import platform
import shutil


def clear_terminal() -> None:
    """
    清除目前終端機畫面，並在底部畫一條分隔線。

    行為說明
    - 會偵測作業系統：
      - Windows: 使用 'cls'
      - 其他（macOS/Linux/Unix 類）: 使用 'clear'
    - 嘗試取得終端大小以決定分隔線長度；若失敗則使用 80 欄寬。

    參數
    - 無

    回傳
    - None

    可能例外
    - 不主動拋出例外；若系統呼叫失敗，仍會繼續並以預設寬度印出分隔線。
    """
    try:
        cols, _ = shutil.get_terminal_size(fallback=(80, 24))
    except Exception:
        cols = 80
    system = platform.system().lower()
    if "windows" in system:
        os.system("cls")
    else:
        os.system("clear")
    print("=" * cols)