import os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import csv
import math

import xlsxwriter

try:
    import filetype
except Exception:
    filetype = None

# Row limit will depend on whether folder hyperlinks are created
LOG_ENTRIES = []

CATEGORY_MAP = {
    "办公文档": {
        ".doc", ".docx", ".xls", ".xlsx", ".xlsm", ".csv",
        ".ppt", ".pptx", ".pdf", ".odt", ".ods", ".odp", ".rtf", ".txt",
    },
    "图片": {
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tif", ".tiff",
        ".webp", ".svg", ".heic", ".ico", ".raw",
    },
    "音频": {
        ".mp3", ".wav", ".m4a", ".flac", ".aac", ".ogg", ".wma", ".aiff",
    },
    "视频": {
        ".mp4", ".mkv", ".mov", ".avi", ".wmv", ".flv", ".mpeg", ".3gp", ".webm",
    },
    "压缩包": {
        ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso",
    },
    "财务账套": {
        ".udb", ".vdb", ".vdd", ".ais", ".tplus", ".bdf", ".pbd",
        ".ini", ".bak", ".bkp", ".aiy", ".air", ".ldb", ".gdb", ".gdb"
    },
    "数据库": {
        ".db", ".db3", ".mdb", ".accdb", ".sqlite", ".sqlite3",
        ".sql", ".dbf", ".dbx", ".dbs", ".mdf", ".ndf", ".ldf",
        ".frm", ".ibd", ".myd", ".myi", ".par", ".dmp", ".ora",
        ".dat", ".fdb", ".gdb", ".sdf", ".kdb", ".ais", ".tplus",
    },
    "可执行文件": {
        ".exe", ".msi", ".bat", ".sh", ".app", ".apk", ".com",
    },
    "代码文件": {
        ".py", ".js", ".html", ".css", ".cpp", ".c", ".java", ".cs",
        ".php", ".rb", ".ts", ".json", ".xml", ".yml",
    },
}
OTHER_CATEGORY = "其他"


def add_log(action: str, path: str, note: str = "") -> None:
    timestamp = datetime.now()
    entry = [timestamp, action, path, note]
    LOG_ENTRIES.append(entry)
    time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    output = f"[{time_str}] {action}: {path}"
    if note:
        output += f" ({note})"
    print(output)


def categorize(ext: str) -> str:
    ext = ext.lower()
    for category, extensions in CATEGORY_MAP.items():
        if ext in extensions:
            return category
    return OTHER_CATEGORY


def detect_file_type(path: str) -> str:
    if not filetype:
        return "unknown"
    try:
        kind = filetype.guess(path)
        if kind is None:
            return "unknown"
        return f"{kind.mime} ({kind.extension})"
    except Exception:
        return "error"


def scan_file(file_path: Path) -> dict | None:
    try:
        stat = file_path.stat()
        info = {
            "path": str(file_path),
            "folder": str(file_path.parent),
            "name": file_path.name,
            "ext": file_path.suffix,
            "created": datetime.fromtimestamp(stat.st_ctime),
            "modified": datetime.fromtimestamp(stat.st_mtime),
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "category": categorize(file_path.suffix),
            "detected": detect_file_type(str(file_path)),
        }
        add_log("扫描完成", str(file_path), info["category"])
        return info
    except Exception as exc:
        add_log("扫描失败", str(file_path), str(exc))
        return None


def scan_directory(root: Path, workers: int) -> list:
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for dirpath, _, files in os.walk(root):
            for fname in files:
                fpath = Path(dirpath) / fname
                futures.append(executor.submit(scan_file, fpath))
        for f in futures:
            res = f.result()
            if res:
                results.append(res)
    return results


def export_to_excel(records: list, out_path: Path, link_folder: bool) -> None:
    """Export scan records to an Excel workbook using xlsxwriter."""

    row_limit = 32765 if link_folder else 65530

    categorized = defaultdict(list)
    for item in records:
        categorized[item["category"]].append(item)

    wb = xlsxwriter.Workbook(out_path)
    summary_ws = wb.add_worksheet("汇总")
    log_ws = wb.add_worksheet("运行日志")

    summary_ws.write_row(0, 0, ["分类", "文件数", "总大小(MB)"])
    log_ws.write_row(0, 0, ["时间", "操作", "文件路径", "备注"])
    for idx, entry in enumerate(LOG_ENTRIES, start=1):
        log_ws.write_row(idx, 0, entry)

    total_files = 0
    total_size = 0.0
    summary_row = 1

    for category, rows in categorized.items():
        part_count = math.ceil(len(rows) / row_limit)
        for part in range(part_count):
            chunk = rows[part * row_limit : (part + 1) * row_limit]
            title = category if part_count == 1 else f"{category}_part{part + 1}"
            ws = wb.add_worksheet(title[:31])
            ws.write_row(0, 0, [
                "文件夹路径",
                "文件名",
                "扩展名",
                "创建日期",
                "修改日期",
                "文件大小(MB)",
                "检测类型",
                "文件链接",
            ])

            row_idx = 1
            for r in chunk:
                if link_folder:
                    ws.write_url(row_idx, 0, f"file:///{r['folder'].replace(' ', '%20')}", string=r["folder"])
                else:
                    ws.write(row_idx, 0, r["folder"])
                ws.write(row_idx, 1, r["name"])
                ws.write(row_idx, 2, r["ext"])
                ws.write(row_idx, 3, r["created"].strftime("%Y-%m-%d %H:%M:%S"))
                ws.write(row_idx, 4, r["modified"].strftime("%Y-%m-%d %H:%M:%S"))
                ws.write(row_idx, 5, r["size_mb"])
                ws.write(row_idx, 6, r["detected"])
                ws.write_url(row_idx, 7, f"file:///{r['path'].replace(' ', '%20')}", string=r["path"])
                row_idx += 1

            count = len(chunk)
            size_sum = round(sum(x["size_mb"] for x in chunk), 2)
            summary_ws.write_row(summary_row, 0, [title, count, size_sum])
            summary_row += 1
            total_files += count
            total_size += size_sum

    summary_ws.write(summary_row + 1, 0, "总计")
    summary_ws.write(summary_row + 1, 1, total_files)
    summary_ws.write(summary_row + 1, 2, round(total_size, 2))

    wb.close()
    add_log("保存工作簿", str(out_path))


def export_to_csv(records: list, out_path: Path) -> None:
    """Export all scan records to a single UTF-8 CSV.

    The file uses a BOM so it can be opened directly in Excel without
    encoding issues.
    """

    headers = [
        "文件夹路径",
        "文件名",
        "扩展名",
        "创建日期",
        "修改日期",
        "文件大小(MB)",
        "检测类型",
        "文件链接",
        "分类",
    ]

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for r in records:
            writer.writerow([
                r["folder"],
                r["name"],
                r["ext"],
                r["created"].strftime("%Y-%m-%d %H:%M:%S"),
                r["modified"].strftime("%Y-%m-%d %H:%M:%S"),
                r["size_mb"],
                r["detected"],
                r["path"],
                r["category"],
            ])

    add_log("保存CSV", str(out_path))


def get_desktop_path() -> Path:
    if os.name == "nt":
        home = os.environ.get("USERPROFILE")
    else:
        home = os.environ.get("HOME")
    desktop = Path(home or Path.home()) / "Desktop"
    return desktop if desktop.exists() else Path(home or Path.home())


def choose_workers() -> int:
    cpu = os.cpu_count() or 1
    if cpu < 4:
        workers = cpu * 2
    else:
        workers = cpu * 4
    return min(64, max(1, workers))


def main() -> None:
    target = input("请输入要扫描的路径: ").strip()
    if not target:
        print("未提供路径，退出。")
        return
    root_path = Path(target)
    if not root_path.exists():
        print("路径不存在，退出。")
        return

    while True:
        mode = input("选择输出格式: 1. xlsx  2. csv : ").strip()
        if mode in {"1", "2"}:
            break
        print("输入无效，请重新选择。")

    link_folder = False
    if mode == "1":
        ans = input("是否生成文件夹链接(Y/N): ").strip().lower()
        link_folder = ans == "y"

    workers = choose_workers()
    add_log("开始扫描", str(root_path), f"线程数: {workers}")
    print(f"开始扫描 '{root_path}'，使用 {workers} 个线程...")

    data = scan_directory(root_path, workers)

    add_log("扫描结束", str(root_path), f"共发现文件: {len(data)}")
    if not data:
        print("未找到任何文件。")
        return

    desktop = get_desktop_path()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if mode == "1":
        out_file = desktop / f"扫描结果_{timestamp}.xlsx"
        export_to_excel(data, out_file, link_folder)
    else:
        out_file = desktop / f"扫描结果_{timestamp}.csv"
        export_to_csv(data, out_file)

    print(f"扫描完成，结果已保存至 {out_file}")


if __name__ == "__main__":
    main()
