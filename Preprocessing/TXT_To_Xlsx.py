#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import csv
import pathlib
from typing import Optional, Tuple, List

import pandas as pd

ENCODING_CANDIDATES: List[str] = ["utf-8-sig", "utf-8", "gb18030", "cp936", "latin1"]
READ_CHUNKSIZE: Optional[int] = None
MAX_PREVIEW_ROWS: int = 0

def has_header(file_path: str) -> bool:
    try:
        with open(file_path, "rb") as f:
            data = f.read(64 * 1024)
        text = data.decode("utf-8", errors="ignore")
        first_line = text.splitlines()[0] if text else ""
        if 0 < len(first_line) < 500 and re.search(r"[A-Za-z\u4e00-\u9fa5]", first_line):
            return True
    except Exception:
        pass
    return True

def list_txt_files(root: str) -> List[str]:
    files = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith(".txt"):
                files.append(os.path.join(dirpath, fn))
    return files

def build_output_path(input_root: str, txt_path: str, out_root: str) -> str:
    rel = os.path.relpath(txt_path, start=input_root)
    rel_no_ext = os.path.splitext(rel)[0]
    out_path = os.path.join(out_root, rel_no_ext + ".xlsx")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    return out_path

def get_user_sep_raw() -> str:
    # 不做 strip，保留你输入的真空格/制表符；仅去掉行末换行
    raw = sys.stdin.readline()
    return raw.rstrip("\r\n")

def normalize_sep(user_sep_raw: str) -> Tuple[Optional[str], bool, bool]:
    """
    返回 (sep, use_whitespace, auto_mode)
    - sep: 传给 pandas 的分隔符（字符串或 None）
    - use_whitespace: True 表示使用 delim_whitespace=True（把连续空白当作一个分隔）
    - auto_mode: True 表示启用自动识别（用户直接回车）
    规则：
      1) 输入 'space'/'空格'/'whitespace' -> use_whitespace=True
      2) 输入字面空格(一个或多个) -> 精确空格数分隔，如 '  ' 表示两个空格
      3) 输入 '\t'/'tab' -> 制表符
      4) 输入其它字符串 -> 作为分隔符原样使用
      5) 直接回车空输入 -> 自动识别模式
    """
    s = user_sep_raw
    s_lower = s.lower()

    if s == "":  # 直接回车 -> 自动识别
        return (None, False, True)

    # 关键词：空白模式
    if s_lower in {"space", "空格", "whitespace", "空白"}:
        return (None, True, False)

    # 字面空格（全是空白且不为空）-> 精确空格数
    if s.strip("") == s and s != "" and s.isspace():
        return (s, False, False)

    # 制表符
    if s_lower in {"\\t", "tab", "制表", "制表符"}:
        return ("\t", False, False)

    # 其它可视字符
    return (s, False, False)

def read_with_sep(txt_path: str, sep: Optional[str], use_whitespace: bool) -> pd.DataFrame:
    # 如果是简单的一字符分隔（非空白），可用 C 引擎更快；否则用 python 引擎
    use_c_engine = (not use_whitespace) and isinstance(sep, str) and len(sep) == 1 and not sep.isspace()

    read_kwargs = dict(
        dtype=str,
        header=0 if has_header(txt_path) else None,
        skip_blank_lines=True,
        on_bad_lines="skip",
        quoting=csv.QUOTE_NONE,
        engine="c" if use_c_engine else "python",
    )

    if use_whitespace:
        read_kwargs.update(dict(delim_whitespace=True))
    else:
        read_kwargs.update(dict(sep=sep))

    # 只有 C 引擎才支持 low_memory
    if use_c_engine:
        read_kwargs.update(dict(low_memory=False))

    if READ_CHUNKSIZE:
        read_kwargs.update(dict(chunksize=READ_CHUNKSIZE))

    last_err = None
    for enc in ENCODING_CANDIDATES:
        try:
            if READ_CHUNKSIZE:
                chunks = []
                for chunk in pd.read_csv(txt_path, encoding=enc, **read_kwargs):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
            else:
                df = pd.read_csv(txt_path, encoding=enc, **read_kwargs)

            df = df.fillna("")
            if MAX_PREVIEW_ROWS > 0:
                df = df.head(MAX_PREVIEW_ROWS)
            return df
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"读取失败（尝试编码：{ENCODING_CANDIDATES}）。最后错误：{last_err}")


def try_auto_seps(txt_path: str) -> pd.DataFrame:
    # 自动识别顺序：制表符、竖线、逗号、分号、任意空白
    candidates = [
        ("\t", False),
        ("|", False),
        (",", False),
        (";", False),
        (None, True),  # 任意空白
    ]
    last_err = None
    for sep, use_ws in candidates:
        try:
            df = read_with_sep(txt_path, sep, use_ws)
            # 简单判定：若只有1列且行内包含明显分隔符，继续尝试
            if df.shape[1] == 1:
                text_sample = df.iloc[:50, 0].astype(str).str.cat(sep="\n")
                if any(ch in text_sample for ch in ["\t", "|", ",", ";", "  "]):
                    last_err = "疑似未正确分列"
                    continue
            return df
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"自动识别分隔符失败：{last_err}")

def convert_one(txt_path: str, sep: Optional[str], use_whitespace: bool, auto_mode: bool) -> pd.DataFrame:
    if auto_mode:
        return try_auto_seps(txt_path)
    else:
        return read_with_sep(txt_path, sep, use_whitespace)

def main():
    print("请输入文件路径（可为本地或局域网共享路径，如 \\\\server\\share\\folder ）：")
    in_dir = input().strip().strip('"').strip("'")
    if not in_dir or not os.path.isdir(in_dir):
        print(f"路径不存在或不是文件夹：{in_dir}")
        sys.exit(1)

    print("请输入分隔符号（直接按空格=使用字面空格；输入 space/空格/whitespace=任意空白；输入 \\t/tab=制表；留空直接回车=自动识别）：")
    user_sep_raw = get_user_sep_raw()
    sep, use_whitespace, auto_mode = normalize_sep(user_sep_raw)

    in_dir_abs = os.path.abspath(in_dir)
    parent = os.path.dirname(in_dir_abs.rstrip("\\/"))
    base = os.path.basename(in_dir_abs.rstrip("\\/"))
    out_root = os.path.join(parent, f"{base}_xlsx")

    txt_files = list_txt_files(in_dir_abs)
    if not txt_files:
        print("未找到任何 .txt 文件。")
        sys.exit(0)

    total, ok, fail = len(txt_files), 0, 0
    failures = []

    print(f"发现 {total} 个 .txt 文件，开始转换 ...")
    for i, txt_path in enumerate(txt_files, 1):
        try:
            df = convert_one(txt_path, sep, use_whitespace, auto_mode)
            out_path = build_output_path(in_dir_abs, txt_path, out_root)
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Sheet1")
            ok += 1
            print(f"[{i}/{total}] 成功：{txt_path} -> {out_path}")
        except Exception as e:
            fail += 1
            failures.append((txt_path, str(e)))
            print(f"[{i}/{total}] 失败：{txt_path}\n   原因：{e}")

    print("\n====== 完成 ======")
    print(f"成功：{ok}，失败：{fail}，输出根目录：{out_root}")
    if failures:
        print("\n以下文件转换失败：")
        for p, msg in failures:
            print(f"- {p}\n  原因：{msg}")

if __name__ == "__main__":
    pathlib.Path.cwd()
    main()
