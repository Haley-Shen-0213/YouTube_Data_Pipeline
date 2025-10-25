# 檔案：scripts/utils/tree.py

# 用法：
# - 直接執行：python scripts/utils/tree.py
#   → 同步在終端顯示並寫入 docs/PROJECT_TREE_YYYYMMDD_HHMMSS.md
# - 自訂輸出目錄：python scripts/utils/tree.py --out docs/
#   → 產出 docs/PROJECT_TREE_YYYYMMDD_HHMMSS.md
# - 自訂輸出檔名：python scripts/utils/tree.py --out docs/TREE.md
#   → 產出 docs/TREE_YYYYMMDD_HHMMSS.md
# - 調整排除目錄：python scripts/utils/tree.py --exclude .git .venv __pycache__

import os
import platform
import shutil
from pathlib import Path
from typing import Iterable, Set
from datetime import datetime

def clear_terminal() -> None:
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

DEFAULT_EXCLUDE_DIRS = {
    '.git', '__pycache__', '.tokens', '.venv', '.idea', '.vscode'
}
DEFAULT_EXCLUDE_FILES = {'.DS_Store'}

def build_tree(root: Path, prefix: str = '', exclude_dirs: Set[str] = None, exclude_files: Set[str] = None) -> str:
    exclude_dirs = exclude_dirs or set()
    exclude_files = exclude_files or set()
    entries = [
        e for e in sorted(root.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        if e.name not in exclude_files and e.name not in exclude_dirs
    ]
    lines = []
    for i, e in enumerate(entries):
        is_last = (i == len(entries) - 1)
        connector = '└─ ' if is_last else '├─ '
        if e.is_dir():
            lines.append(f"{prefix}{connector}{e.name}/")
            ext_prefix = f"{prefix}{'   ' if is_last else '│  '}"
            subtree = build_tree(e, ext_prefix, exclude_dirs, exclude_files)
            if subtree:
                lines.append(subtree)
        else:
            lines.append(f"{prefix}{connector}{e.name}")
    return '\n'.join(lines)

def render_project_tree(exclude_dirs: Set[str], exclude_files: Set[str]) -> str:
    root = Path(__file__).resolve().parents[2]  # 專案根目錄
    return f"{root.name}/\n" + build_tree(root, '', exclude_dirs, exclude_files)

def write_docs(doc_path: Path, tree_str: str):
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    with doc_path.open('w', encoding='utf-8') as f:
        f.write("# 專案檔案樹（自動生成）\n\n")
        f.write("```\n")
        f.write(tree_str)
        f.write("\n```\n")
    print(f"[OK] Wrote tree to {doc_path}")

def make_timestamped_path(out_arg: str) -> Path:
    """
    將使用者傳入的 --out 參數轉換為帶時間戳記的輸出路徑。
    規則：
    - 若 out_arg 是目錄（或以斜線結尾），輸出為 <dir>/PROJECT_TREE_YYYYMMDD_HHMMSS.md
    - 若 out_arg 是檔案，輸出為 <dir>/<stem>_YYYYMMDD_HHMMSS<suffix>
    - 若未帶 --out，預設為 docs/PROJECT_TREE_YYYYMMDD_HHMMSS.md
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not out_arg:
        return Path(f"docs/PROJECT_TREE_{ts}.md")

    p = Path(out_arg)
    # 若參數末尾帶路徑分隔符或該目錄存在，視為目錄
    if out_arg.endswith(("/", "\\")) or (p.exists() and p.is_dir()):
        return p.joinpath(f"PROJECT_TREE_{ts}.md")
    # 若父層不存在，但 out_arg 沒有副檔名又看起來像資料夾名，也當作目錄處理
    if not p.suffix and (out_arg.endswith(("/", "\\")) or not p.parent.suffix):
        return p.joinpath(f"PROJECT_TREE_{ts}.md")
    # 一般檔案情境：插入時間戳記到檔名
    stem = p.stem
    suffix = p.suffix or ".md"
    parent = p.parent if p.parent.as_posix() != "" else Path(".")
    return parent.joinpath(f"{stem}_{ts}{suffix}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="輸出專案檔案樹：同步顯示於終端並寫入文件（自動帶入日期時間戳記）")
    parser.add_argument('--out', type=str, default='', help='輸出檔案或目錄（留空則輸出至 docs/PROJECT_TREE_YYYYMMDD_HHMMSS.md）')
    parser.add_argument('--exclude', nargs='*', default=[], help='額外排除的目錄/檔名（以名稱比對）')
    args = parser.parse_args()

    # 合併排除清單（目錄與檔案名稱同名時也會被排除）
    exclude_dirs = set(DEFAULT_EXCLUDE_DIRS)
    exclude_files = set(DEFAULT_EXCLUDE_FILES)
    for name in args.exclude:
        exclude_dirs.add(name)
        exclude_files.add(name)

    tree_str = render_project_tree(exclude_dirs, exclude_files)

    # 1) 顯示到終端
    print(tree_str)

    # 2) 產生帶時間戳記的輸出路徑並輸出成文件
    out_path = make_timestamped_path(args.out)
    write_docs(out_path, tree_str)

if __name__ == '__main__':
    clear_terminal()
    main()