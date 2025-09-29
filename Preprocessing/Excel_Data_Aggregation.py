# -*- coding: utf-8 -*-
"""
excel_aggregate_sum_v3.py

- 选择分组列(key_col)
- 可选多列做“求和”：会计风格识别（括号负数/全角/中文逗号/¥/$/Unicode负号等）
- 求和列：Decimal 精确求和 -> 四舍五入两位 -> 以数值写入，Excel列格式 0.00
- 非求和列：唯一值拼接为文本，防科学计数
- 第一列“被聚合行数”：整数数值写入，列格式 0
"""

import os
import re
import math
import warnings
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import List, Any, Tuple
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

# 屏蔽 openpyxl 警告
warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style, apply openpyxl's default",
    category=UserWarning
)

import pandas as pd

# ===== 开关 =====
DIAG_PRINT = False  # 校验失败时是否打印样本

# ===== 常量与正则 =====
_EMPTY_TOKENS = {
    "", "-", "—", "–", "－",
    "N/A", "n/a", "NA", "na",
    "None", "none", "null", "Null", "NULL",
    "nan", "NaN", "NAN"
}
# 允许 .38 这种形式
_NUMERIC_RE = re.compile(r'^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$')

# ===== 基础工具 =====
def _normalize_sep(sep: str) -> str:
    if sep is None:
        return "；"
    s = str(sep).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    return s if s else "；"

def _to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    return str(x).strip()

def _remove_all_spaces_and_controls(s: str) -> str:
    s = re.sub(r'\s+', '', s)  # 包括 NBSP/换行/制表
    s = s.replace('\u200b', '').replace('\ufeff', '')  # 零宽/ BOM
    return s

def _clean_numeric_token(raw: str) -> Tuple[str, bool]:
    """会计风格清洗；返回(clean, is_empty)"""
    if raw is None:
        return "", True
    s = str(raw)
    s = unicodedata.normalize("NFKC", s).strip()
    if s in _EMPTY_TOKENS:
        return "", True

    is_paren_neg = False
    if len(s) >= 2 and s.lstrip().startswith("(") and s.rstrip().endswith(")"):
        s = s.strip()
        s = s[1:-1].strip()
        is_paren_neg = True

    for sym in ("¥", "￥", "$"):
        s = s.replace(sym, "")
    s = s.replace("−", "-")  # U+2212
    s = s.replace(",", "").replace("，", "")
    s = _remove_all_spaces_and_controls(s)

    if s in _EMPTY_TOKENS or s == "":
        return "", True

    if is_paren_neg and not s.startswith("-"):
        if s.startswith("+"):
            s = s[1:]
        s = "-" + s

    return s, False

def _is_pure_number_after_clean(s: str) -> bool:
    return bool(_NUMERIC_RE.match(s))

def _unique_join(series: pd.Series, sep: str) -> str:
    seen = set()
    out: List[str] = []
    for v in series:
        s = _to_str(v)
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return sep.join(out)

def _decimal_sum(series: pd.Series) -> Decimal:
    """分组内 Decimal 求和（返回 Decimal，不在此处转字符串）。"""
    total = Decimal("0")
    for v in series:
        token, is_empty = _clean_numeric_token(_to_str(v))
        if is_empty or not token:
            continue
        try:
            if _is_pure_number_after_clean(token):
                total += Decimal(token)
        except (InvalidOperation, ValueError):
            continue
    # 在最终整理阶段再量化到两位，这里先原样返回
    return total

# ===== Excel IO =====
def list_sheets(xl_path: str) -> pd.ExcelFile:
    try:
        return pd.ExcelFile(xl_path, engine="openpyxl")
    except Exception:
        return pd.ExcelFile(xl_path)

def read_sheet_as_str(xl_path: str, sheet_name: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(xl_path, sheet_name=sheet_name, dtype=str, engine="openpyxl")
    except Exception:
        df = pd.read_excel(xl_path, sheet_name=sheet_name, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def choose_from_list(title: str, items: List[str]) -> int:
    print(title)
    for i, name in enumerate(items, start=1):
        print(f"{i}-{name}")
    while True:
        s = input("请输入编号：").strip()
        if not s.isdigit():
            print("请输入数字编号。"); continue
        idx = int(s) - 1
        if 0 <= idx < len(items):
            return idx
        print("编号超出范围，请重试。")

def _read_multiline_indices(max_len: int) -> List[int]:
    print("如有需要求和的字段：请输入对应的序号，分行输入；连续两次回车表示输入完毕。")
    selected: List[int] = []
    empty_streak = 0
    while True:
        s = input().strip()
        if s == "":
            empty_streak += 1
            if empty_streak >= 2:
                break
            continue
        empty_streak = 0
        if not s.isdigit():
            print("请输入纯数字序号。"); continue
        idx = int(s) - 1
        if not (0 <= idx < max_len):
            print("序号超出范围。"); continue
        if idx not in selected:
            selected.append(idx)
    return selected

def _validate_sum_columns(df: pd.DataFrame, cols: List[str]) -> Tuple[bool, List[str]]:
    failed = []
    for col in cols:
        ser = df[col].astype(str)
        ok = True
        bad_examples = []
        for raw in ser:
            token, is_empty = _clean_numeric_token(raw)
            if is_empty or token == "":
                continue
            if not _is_pure_number_after_clean(token):
                ok = False
                if DIAG_PRINT and len(bad_examples) < 5:
                    bad_examples.append((raw, token))
        if not ok:
            failed.append(col)
            if DIAG_PRINT and bad_examples:
                print(f"[诊断] 列《{col}》样本（原->清洗）：")
                for a, b in bad_examples:
                    print(f"  {repr(a)} -> {repr(b)}")
    return (len(failed) == 0, failed)

# ===== 聚合主逻辑 =====
def aggregate_dataframe(df: pd.DataFrame, key_col: str, sep: str, sum_cols: List[str]) -> pd.DataFrame:
    if key_col not in df.columns:
        raise ValueError(f"找不到聚合字段：{key_col}")

    df[key_col] = df[key_col].map(_to_str)

    # 统计每组行数（整数）
    counts = df.groupby(key_col, sort=False)[key_col].size().rename("被聚合行数").reset_index()

    # 聚合映射
    agg_funcs = {}
    for col in df.columns:
        if col == key_col:
            continue
        if col in sum_cols:
            agg_funcs[col] = _decimal_sum  # 返回 Decimal
        else:
            agg_funcs[col] = (lambda s, _sep=sep: _unique_join(s, _sep))  # 返回 str

    grouped = df.groupby(key_col, sort=False).agg(agg_funcs).reset_index()
    result = pd.merge(grouped, counts, on=key_col, how="left")

    # 列顺序：被聚合行数 + key + 原顺序其余列
    other_cols = [c for c in df.columns if c != key_col]
    result = result[["被聚合行数", key_col] + other_cols]

    # 整理数据类型：
    # - 求和列：Decimal -> 量化到两位 -> float（数值）
    # - 被聚合行数：整数
    # - 非求和列与 key_col：转为字符串
    result["被聚合行数"] = result["被聚合行数"].astype("int64")
    for col in other_cols:
        if col in sum_cols:
            # 若为空（可能整组无有效数），设为 0 或者保持为空 None（这里用 0.0，便于数值列统一）
            def _dec_to_float(x):
                if isinstance(x, Decimal):
                    return float(x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                if x is None or (isinstance(x, str) and x.strip() == ""):
                    return 0.0
                # 兜底：尽量转 Decimal
                try:
                    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                except Exception:
                    return 0.0
            result[col] = result[col].map(_dec_to_float)
        else:
            result[col] = result[col].map(_to_str)

    return result

def desktop_path() -> Path:
    p = Path.home() / "Desktop"
    return p if p.exists() else Path.cwd()

def save_with_formats(df: pd.DataFrame, out_path: Path, sheet_name: str,
                      sum_cols: List[str]) -> None:
    """
    写 Excel：
    - sum_cols 列：数值格式 0.00
    - “被聚合行数”：整数格式 0
    - 其他列：文本格式 '@'
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

        workbook  = writer.book
        ws = writer.sheets[sheet_name]

        fmt_text = workbook.add_format({"num_format": "@", "text_wrap": False})
        fmt_int  = workbook.add_format({"num_format": "0"})
        fmt_2dp  = workbook.add_format({"num_format": "0.00"})

        # 列宽与格式：分别设置，避免全表文本
        cols = list(df.columns)
        for i, col in enumerate(cols):
            if col == "被聚合行数":
                ws.set_column(i, i, 14, fmt_int)
            elif col in sum_cols:
                ws.set_column(i, i, 18, fmt_2dp)
            else:
                ws.set_column(i, i, 20, fmt_text)

        ws.freeze_panes(1, 0)

# ===== 主流程 =====
def main():
    print("=== Excel 聚合工具 ===")
    xl_path = input("请输入文件路径：").strip().strip('"')
    if not xl_path:
        print("未输入路径，程序结束。"); return
    if not os.path.exists(xl_path):
        print("路径不存在，请检查后重试。"); return

    print("正在扫描工作簿...")
    xl = list_sheets(xl_path)
    sheets = xl.sheet_names
    if not sheets:
        print("未找到任何工作表。"); return

    sheet_idx = choose_from_list("请选择工作表：", sheets)
    sheet_name = sheets[sheet_idx]
    print(f"已选择：{sheet_name}")

    print("正在读取数据（全列按文本）...")
    df = read_sheet_as_str(xl_path, sheet_name)
    if df.empty:
        print("工作表为空，无数据可处理。"); return

    cols = list(df.columns)
    col_idx = choose_from_list("请选择需要【按此列聚合】的字段：", cols)
    key_col = cols[col_idx]
    print(f"已选择按【{key_col}】聚合。")

    print("\n当前表头：")
    for i, name in enumerate(cols, start=1):
        print(f"{i}-{name}")
    print("\n如需对某些字段进行【求和】，请输入它们的序号，分行输入；不需要则直接连续两次回车。")

    # 选择“求和列”
    while True:
        idx_list = _read_multiline_indices(len(cols))
        idx_list = [i for i in idx_list if cols[i] != key_col]
        sum_cols = [cols[i] for i in idx_list]
        if not sum_cols:
            print("未选择求和列，将仅做唯一值拼接。")
            break
        ok, failed = _validate_sum_columns(df, sum_cols)
        if ok:
            print("求和列已确认：", "，".join(sum_cols))
            break
        else:
            print("以下字段包含非纯数值，无法求和：", "，".join(failed))
            print("请重新选择求和列（再次分行输入；连续两次回车结束）。")

    sep = _normalize_sep(input("\n请输入分隔符（例如 、 ； | 等）："))
    print(f"分隔符设为：{sep!r}")

    print("\n正在聚合数据（求和列=Decimal精确求和->两位小数；其余列=唯一值拼接）...")
    result = aggregate_dataframe(df, key_col=key_col, sep=sep, sum_cols=sum_cols)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = f"聚合结果_{ts}.xlsx"
    out_path = desktop_path() / out_file

    print(f"正在写出结果到桌面：{out_path}")
    save_with_formats(result, out_path, sheet_name="结果", sum_cols=sum_cols)

    print("\n完成！")
    print(f"原始行数：{len(df):,}  |  聚合后行数：{len(result):,}")
    print(f"文件已保存：{out_path}")

if __name__ == "__main__":
    pd.options.mode.copy_on_write = True
    main()
