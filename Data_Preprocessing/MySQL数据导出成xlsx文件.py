#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL → Excel 导出器（修复 XlsxWriter 传参 + 并发分工作簿 + 实时日志）
"""

import os
import sys
import re
import time
import hashlib
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Optional, Tuple, Any

# ====== 连接信息（按你提供）======
DB_SERVER   = "localhost"
DB_PORT     = 3306
DB_USER     = "root"
DB_PASSWORD = "010203"

# ====== 导出与性能参数 ======
ROWS_PER_WORKBOOK     = 1_000_000     # 每个工作簿最多数据行（表头另计）
BASE_CHUNK_ROWS       = 50_000        # 基础分块大小（并发时会做轻微下调）
SHEET_NAME            = "提取"
ENGINE_ECHO           = False         # 调试 SQL 时可置 True
MAX_WORKERS_HARD_CAP  = 61            # 并发线程上限（按需求）
FILENAME_MAXLEN       = 120           # 文件名最长字符（含中文）

# ====== 依赖 ======
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

# ====== 线程安全输出 ======
PRINT_LOCK = threading.Lock()
def log(msg: str):
    with PRINT_LOCK:
        print(msg, flush=True)

# ====== 工具函数 ======
def q_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"

_INVALID_CHARS = r'[<>:"/\\|?*\x00-\x1F]'
_WINDOWS_RESERVED = {"CON","PRN","AUX","NUL","COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
                     "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9"}

def safe_filename(base: str, ext: str = ".xlsx", max_len: int = FILENAME_MAXLEN) -> str:
    if base is None or str(base).strip() == "":
        base = "NULL"
    s = str(base).strip()
    s = re.sub(_INVALID_CHARS, "_", s)
    s = s.rstrip(". ")
    if not s:
        s = "EMPTY"
    if s.upper() in _WINDOWS_RESERVED:
        s = f"_{s}_"
    keep = max_len - len(ext)
    if keep < 10:
        keep = 10
    if len(s) > keep:
        h6 = hashlib.md5(s.encode("utf-8")).hexdigest()[:6]
        s = s[: max(4, keep - 7)] + f"_{h6}"
    return s + ext

def unique_path(directory: str, filename: str) -> str:
    p = os.path.join(directory, filename)
    if not os.path.exists(p):
        return p
    stem, ext = os.path.splitext(filename)
    i = 2
    while True:
        cand = f"{stem}_{i}{ext}"
        pc = os.path.join(directory, cand)
        if not os.path.exists(pc):
            return pc
        i += 1

def input_index(prompt: str, total: int) -> int:
    while True:
        s = input(prompt).strip()
        if not s.isdigit():
            print("请输入数字序号。")
            continue
        idx = int(s)
        if 1 <= idx <= total:
            return idx
        print(f"请输入 1 到 {total} 之间的序号。")

def yn_input(prompt: str) -> bool:
    while True:
        s = input(prompt).strip().lower()
        if s in {"y", "yes"}:
            return True
        if s in {"n", "no"}:
            return False
        print("请输入 Y 或 N。")

def ensure_dir(path: str) -> str:
    p = os.path.expanduser(os.path.expandvars(path.strip()))
    if not p:
        raise ValueError("导出目录不能为空")
    os.makedirs(p, exist_ok=True)
    return p

def auto_workers(n_tasks: int) -> int:
    cpu2 = (os.cpu_count() or 8) * 2
    return max(1, min(MAX_WORKERS_HARD_CAP, cpu2, n_tasks))

def chunk_rows_for_workers(workers: int) -> int:
    # 并发多时，适当降低单线程块大小，避免内存过高
    if workers >= 16:
        return max(10_000, BASE_CHUNK_ROWS // 3)
    if workers >= 8:
        return max(10_000, BASE_CHUNK_ROWS // 2)
    return BASE_CHUNK_ROWS

# ====== 数据库操作 ======
def make_engine(db: Optional[str] = None) -> Engine:
    if db:
        url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}:{DB_PORT}/{db}?charset=utf8mb4"
    else:
        url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}:{DB_PORT}/?charset=utf8mb4"
    # 为并发安全，每个线程各自创建 engine（轻量）。不依赖连接池做跨线程复用。
    eng = create_engine(url, echo=ENGINE_ECHO, pool_pre_ping=True, future=True)
    return eng

def list_databases(eng: Engine) -> List[str]:
    with eng.connect() as conn:
        rs = conn.execute(text("SHOW DATABASES"))
        return [r[0] for r in rs]

def list_tables(eng: Engine) -> List[str]:
    with eng.connect() as conn:
        rs = conn.execute(text("SHOW FULL TABLES"))
        return [r[0] for r in rs if str(r[1]).upper() == "BASE TABLE"]

def list_columns(eng: Engine, database: str, table: str) -> List[str]:
    q = f"SHOW FULL COLUMNS FROM {q_ident(database)}.{q_ident(table)}"
    with eng.connect() as conn:
        rs = conn.execute(text(q))
        return [r[0] for r in rs]

def iter_query_chunks(eng: Engine, sql: str, params=None, chunksize: int = BASE_CHUNK_ROWS) -> Iterable[pd.DataFrame]:
    for df in pd.read_sql_query(sql=sql, con=eng, params=params, chunksize=chunksize):
        yield df

# ====== 写 Excel ======
def new_excel_writer(path: str) -> pd.ExcelWriter:
    # 关键修复：pandas 2.x 需要把 xlsxwriter 的 options 放进 engine_kwargs
    engine_kwargs = {"options": {"strings_to_urls": False}}  # 禁止自动识别超链接，写入更快
    return pd.ExcelWriter(path, engine="xlsxwriter", engine_kwargs=engine_kwargs)

def write_chunks_to_workbooks(
    out_dir: str,
    base_filename_without_ext: str,
    chunk_iter: Iterable[pd.DataFrame],
    rows_per_wb: int,
    sheet_name: str,
    prefix: str = ""
) -> Tuple[int, int]:
    """将分块数据写入一个或多个工作簿。返回 (总行数, 生成工作簿数)。"""
    total_rows = 0
    wb_count   = 0
    current_rows_in_wb = 0
    writer = None
    wrote_header = False

    def open_new_writer(seq: int):
        nonlocal writer, wrote_header, current_rows_in_wb, wb_count
        if seq == 1:
            fn = safe_filename(base_filename_without_ext, ".xlsx")
        else:
            fn = safe_filename(f"{base_filename_without_ext}_{seq}", ".xlsx")
        full = unique_path(out_dir, fn)
        writer = new_excel_writer(full)
        wrote_header = False
        current_rows_in_wb = 0
        wb_count += 1
        log(f"{prefix}[写入] 打开工作簿：{full}")

    def close_writer():
        nonlocal writer
        if writer is not None:
            writer.close()
            writer = None

    try:
        open_new_writer(1)
        for df in chunk_iter:
            if df is None or df.empty:
                continue
            start_idx = 0
            while start_idx < len(df):
                remain = rows_per_wb - current_rows_in_wb
                if remain <= 0:
                    close_writer()
                    open_new_writer(wb_count + 1)
                    remain = rows_per_wb

                end_idx = min(len(df), start_idx + remain)
                part = df.iloc[start_idx:end_idx]

                header_flag = not wrote_header
                startrow = 0 if header_flag else (current_rows_in_wb + 1)
                part.to_excel(writer, sheet_name=sheet_name, index=False,
                              startrow=startrow, header=header_flag)

                wrote_header = True
                rows_written = len(part)
                current_rows_in_wb += rows_written
                total_rows += rows_written
                start_idx = end_idx

                log(f"{prefix}  + 本簿新增 {rows_written} 行 | 本簿累计 {current_rows_in_wb} | 总计 {total_rows}")

        close_writer()
        return total_rows, wb_count
    except Exception:
        close_writer()
        raise

# ====== 导出流程 ======
def choose_database() -> str:
    eng = make_engine(None)
    dbs = list_databases(eng)
    if not dbs:
        raise RuntimeError("该实例下未发现任何数据库。")

    print("请输入对应编号选择数据库 回车确认")
    for i, db in enumerate(dbs, start=1):
        print(f"{i}-{db}")
    idx = input_index("序号：", len(dbs))
    return dbs[idx - 1]

def choose_table(database: str) -> str:
    eng = make_engine(database)
    tbls = list_tables(eng)
    if not tbls:
        raise RuntimeError(f"数据库 {database} 下未发现任何表。")
    print(f"\n请选择要导出的表（{database}）：")
    for i, t in enumerate(tbls, start=1):
        print(f"{i}-{t}")
    idx = input_index("序号：", len(tbls))
    return tbls[idx - 1]

def choose_split_field(database: str, table: str) -> Optional[str]:
    eng = make_engine(database)
    use_split = yn_input("\n是否有需要分工作簿导出的字段（Y/N）？ ")
    if not use_split:
        return None
    cols = list_columns(eng, database, table)
    if not cols:
        print("未获取到字段信息，将按整表导出。")
        return None
    print("\n请选择用于分工作簿的字段：")
    for i, c in enumerate(cols, start=1):
        print(f"{i}-{c}")
    idx = input_index("序号：", len(cols))
    return cols[idx - 1]

def export_whole_table(database: str, table: str, out_dir: str):
    eng = make_engine(database)
    sql = f"SELECT * FROM {q_ident(database)}.{q_ident(table)}"
    log(f"\n[导出整表] {database}.{table}")
    chunksize = BASE_CHUNK_ROWS
    chunks = iter_query_chunks(eng, sql, params=None, chunksize=chunksize)
    total, wbs = write_chunks_to_workbooks(out_dir, table, chunks,
                                           rows_per_wb=ROWS_PER_WORKBOOK,
                                           sheet_name=SHEET_NAME,
                                           prefix="[整表] ")
    log(f"\n[整表完成] 行数 {total} | 工作簿 {wbs}")

def _export_one_value_task(database: str, table: str, field: str, value: Any,
                           out_dir: str, idx: int, total_vals: int,
                           chunksize: int) -> Tuple[int, int]:
    """单个唯一值任务（在线程中执行）。"""
    eng = make_engine(database)
    val_disp = "NULL" if value is None else str(value)

    if value is None:
        cond = f"({q_ident(field)} IS NULL)"
        params = None
    else:
        cond = f"({q_ident(field)} = %s)"
        params = (value,)
    sql = f"SELECT * FROM {q_ident(database)}.{q_ident(table)} WHERE {cond}"

    prefix = f"[{idx}/{total_vals}] [{field}={val_disp}] "
    log(f"{prefix}开始查询并写入…")
    chunks = iter_query_chunks(eng, sql, params=params, chunksize=chunksize)
    total, wbs = write_chunks_to_workbooks(
        out_dir=out_dir,
        base_filename_without_ext=val_disp,
        chunk_iter=chunks,
        rows_per_wb=ROWS_PER_WORKBOOK,
        sheet_name=SHEET_NAME,
        prefix=prefix
    )
    log(f"{prefix}完成：行数 {total} | 工作簿 {wbs}")
    return total, wbs

def export_by_field_concurrent(database: str, table: str, field: str, out_dir: str):
    eng = make_engine(database)
    log(f"\n[按字段分工作簿] {database}.{table}  字段：{field}")

    # 取唯一值
    distinct_sql = f"SELECT DISTINCT {q_ident(field)} AS v FROM {q_ident(database)}.{q_ident(table)}"
    distinct_vals: List[Any] = []
    with eng.connect() as conn:
        rs = conn.execute(text(distinct_sql))
        for r in rs:
            distinct_vals.append(r[0])

    n = len(distinct_vals)
    if n == 0:
        log("该字段无任何值，跳过。")
        return

    # 线程数与分块大小自适应
    workers = auto_workers(n)
    chunksize = chunk_rows_for_workers(workers)
    log(f"共 {n} 个唯一值，将并发导出（工作线程={workers}，chunksize={chunksize}）。")

    grand_rows = 0
    grand_wbs  = 0
    done = 0

    # 提交任务
    futures = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="export") as ex:
        for i, val in enumerate(distinct_vals, start=1):
            fut = ex.submit(_export_one_value_task,
                            database, table, field, val,
                            out_dir, i, n, chunksize)
            futures.append(fut)

        # 监控完成进度
        for fut in as_completed(futures):
            try:
                rows, wbs = fut.result()
                grand_rows += rows
                grand_wbs  += wbs
                done += 1
                log(f"[进度] 完成 {done}/{n} | 累计行数 {grand_rows} | 生成工作簿 {grand_wbs}")
            except OperationalError as oe:
                done += 1
                log(f"[进度] 任务失败（{done}/{n}）：数据库错误：{oe}")
            except Exception as e:
                done += 1
                log(f"[进度] 任务失败（{done}/{n}）：{e}")

    log(f"\n[分工作簿完成] 总行数 {grand_rows} | 总工作簿 {grand_wbs}")

# ====== 主入口 ======
def main():
    print("=== MySQL → Excel 导出器（并发版） ===")
    print(f"目标实例：{DB_USER}@{DB_SERVER}:{DB_PORT}\n")

    try:
        db = choose_database()
        table = choose_table(db)
        field = choose_split_field(db, table)

        out_dir = input("\n请输入文件导出路径：").strip()
        out_dir = ensure_dir(out_dir)

        t0 = time.time()
        if field:
            export_by_field_concurrent(db, table, field, out_dir)
        else:
            export_whole_table(db, table, out_dir)
        dt = time.time() - t0
        log(f"\n全部完成，用时 {dt:.1f} 秒。")

    except KeyboardInterrupt:
        log("\n用户中断。")
    except Exception as e:
        log("\n× 发生错误：{}".format(e))
        log("—— 详细堆栈 ——")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
