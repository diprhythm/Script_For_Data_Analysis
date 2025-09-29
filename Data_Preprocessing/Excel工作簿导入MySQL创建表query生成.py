#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, math, re, warnings
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

warnings.filterwarnings('ignore', category=UserWarning)

# ========== 工具 ==========
def clean_path(path_str: str) -> str:
    return re.sub(r'[\u200e\u200f\u202a-\u202e]', '', path_str or "")

def _detect_excel_engine(file_path: str) -> str:
    lower = file_path.lower()
    if lower.endswith((".xlsx", ".xlsm")):
        return "openpyxl"
    elif lower.endswith(".xls"):
        return "xlrd"
    else:
        raise ValueError(f"不支持的 Excel 扩展名: {os.path.basename(file_path)}")

def input_files():
    print("请输入要导入 MySQL 的 Excel 文件路径，每行一个，最后输入空行结束：")
    files = []
    while True:
        try:
            line = input().strip()
        except EOFError:
            break
        if not line:
            break
        p = clean_path(line)
        if not os.path.isfile(p):
            print(f"警告：找不到文件 -> {p}")
        else:
            files.append(p)
    if not files:
        print("未输入任何有效文件路径，程序退出。")
        sys.exit(1)
    return files

def input_sheet():
    return input("请输入表名（若要导入所有 sheet，请输入 all）：\n").strip()

# ========== 并发任务 ==========
def _load_headers_for_file(file_path: str, sheet_name: str):
    engine = _detect_excel_engine(file_path)
    xl = pd.ExcelFile(file_path, engine=engine)
    sheets = xl.sheet_names if sheet_name.lower() == 'all' else [sheet_name]
    headers = None
    for s in sheets:
        if s not in xl.sheet_names:
            raise ValueError(f"文件 {os.path.basename(file_path)} 中不存在 sheet：{s}")
        df = xl.parse(s, nrows=0)
        cols = list(df.columns)
        if headers is None:
            headers = cols
        elif headers != cols:
            return None
    return tuple(headers or [])

def _gather_file_data(file_path: str, sheet_name: str, cols):
    engine = _detect_excel_engine(file_path)
    xl = pd.ExcelFile(file_path, engine=engine)
    sheets = xl.sheet_names if sheet_name.lower() == 'all' else [sheet_name]
    local = {c: [] for c in cols}
    for s in sheets:
        if s not in xl.sheet_names:
            raise ValueError(f"文件 {os.path.basename(file_path)} 中不存在 sheet：{s}")
        df = xl.parse(s, dtype=str)
        for c in cols:
            if c in df.columns:
                local[c].extend(df[c].dropna().astype(str).tolist())
    return local

# ========== 类型推断（不设上限） ==========
_NUMERIC_RE = re.compile(r'^-?\d+(?:\.\d+)?$')

def _suggest_column(col: str, vals):
    """按实际数据推断：日期 / decimal(整数位+小数位, 小数位) / varchar(N)"""
    vals = [v for v in vals if v is not None and str(v).strip() != ""]
    if not vals:
        return col, 'varchar(50)'

    sample = vals[:5000]  # 提速

    # 1) 日期检测
    try:
        pd.to_datetime(sample, errors='raise', infer_datetime_format=True)
        return col, 'datetime'
    except Exception:
        pass

    # 2) 数字检测：不设上限
    if all(_NUMERIC_RE.fullmatch(v) for v in sample):
        max_ip = 0
        max_dp = 0
        for v in sample:
            neg = v.startswith('-')
            vv = v[1:] if neg else v
            if '.' in vv:
                ip, dp = vv.split('.', 1)
                max_ip = max(max_ip, len(ip))
                max_dp = max(max_dp, len(dp))
            else:
                max_ip = max(max_ip, len(vv))
        prec = max_ip + max_dp
        scale = max_dp
        if prec <= scale:
            prec = scale + 1
        return col, f'decimal({prec},{scale})'

    # 3) 字符串
    max_len = max((len(v) for v in sample), default=1)
    length = max(1, math.ceil(max_len * 1.2))
    return col, f'varchar({length})'

def _round_to_scale(value_str: str, scale: int) -> str:
    """把字符串数值四舍五入到 scale 位，返回四舍五入后的字符串（不使用科学计数法）。"""
    # 使用 Decimal 更严谨；但为避免依赖，这里用 float + 格式化，已足够校验使用
    try:
        f = float(value_str)
    except Exception:
        # 让上层继续判定非数字
        return value_str
    if scale < 0:
        scale = 0
    fmt = "{:." + str(scale) + "f}"
    return fmt.format(round(f, scale))

def _validate_column(col: str, spec: str, vals):
    """
    校验逻辑：
    - datetime: 能否 parse
    - decimal(p,s): **先 round 到 s 位**，再检查整数位/小数位是否在(p,s)范围内
    - varchar(N): 长度是否超 N
    """
    errors = []
    t = spec.strip().lower()
    vals = [v for v in vals if v is not None and str(v).strip() != ""]

    if t == 'datetime':
        for v in vals:
            try:
                pd.to_datetime(v, errors='raise', infer_datetime_format=True)
            except Exception:
                errors.append(f"{col} 无法解析为 datetime: {v}")
                break

    elif t.startswith('decimal'):
        try:
            p, s = map(int, spec[spec.find('(')+1:spec.find(')')].split(','))
        except Exception:
            return [f"{col} 字段定义解析失败: {spec}"]
        if p <= 0 or s < 0:
            return [f"{col} 非法的 decimal 定义: {spec}"]
        if p <= s:
            return [f"{col} decimal 的 precision 必须大于 scale: {spec}"]

        for v in vals:
            if not _NUMERIC_RE.fullmatch(v):
                errors.append(f"{col} 存在非数字: {v}")
                break

            # 关键：先四舍五入到 s 位
            v_rounded = _round_to_scale(v, s)

            # 再按位数检查
            neg = v_rounded.startswith('-')
            vv = v_rounded[1:] if neg else v_rounded
            if '.' in vv:
                ip, dp = vv.split('.', 1)
                ip_len = len(ip)
                dp_len = len(dp)
            else:
                ip_len = len(vv)
                dp_len = 0

            if ip_len > (p - s):
                errors.append(f"{col} 整数位({ip_len}) 超出 decimal({p},{s}) 的上限({p - s}): {v}")
                break
            # 因为已 round 到 s，小数位不会超过 s，这里无需再报错

    elif t.startswith('varchar'):
        try:
            length = int(spec[spec.find('(')+1:spec.find(')')])
        except Exception:
            return [f"{col} 字段定义解析失败: {spec}"]
        for v in vals:
            if len(v) > length:
                errors.append(f"{col} 超出 varchar({length}) 长度: len={len(v)}")
                break
    else:
        errors.append(f"{col} 不支持的类型: {spec}")

    return errors

# ========== 主流程 ==========
def main():
    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
    files = input_files()
    sheet = input_sheet()

    print("开始校验表头一致性...")
    total_files = len(files)
    headers = []
    with ThreadPoolExecutor(max_workers=min(8, max(2, os.cpu_count() or 4))) as executor:
        futures = {executor.submit(_load_headers_for_file, f, sheet): f for f in files}
        for i, future in enumerate(as_completed(futures), 1):
            fpath = futures[future]
            try:
                res = future.result()
            except Exception as e:
                print(f"[表头校验] {i}/{total_files} 文件 '{os.path.basename(fpath)}': FAIL - {e}")
                continue
            status = 'OK' if res else 'FAIL'
            print(f"[表头校验] {i}/{total_files} 文件 '{os.path.basename(fpath)}': {status}")
            if res:
                headers.append(tuple(res))
    if not headers or len(set(headers)) != 1:
        print('表头不统一，请清洗数据后再试！')
        sys.exit(1)
    cols = list(headers[0])

    print("开始收集数据...")
    combined = defaultdict(list)
    with ThreadPoolExecutor(max_workers=min(8, max(2, os.cpu_count() or 4))) as executor:
        futures = {executor.submit(_gather_file_data, f, sheet, cols): f for f in files}
        for i, future in enumerate(as_completed(futures), 1):
            fpath = futures[future]
            try:
                local = future.result()
            except Exception as e:
                print(f"[收集数据] {i}/{total_files} 文件 '{os.path.basename(fpath)}' 失败: {e}")
                continue
            for c, lst in local.items():
                combined[c].extend(lst)
            print(f"[收集数据] {i}/{total_files} 文件 '{os.path.basename(fpath)}' 完成")

    print("开始智能生成类型建议（不设上限）...")
    sugg = {}
    total_cols = len(cols)
    with ThreadPoolExecutor(max_workers=min(8, max(2, os.cpu_count() or 4))) as executor:
        futures = {executor.submit(_suggest_column, c, combined[c]): c for c in cols}
        for i, future in enumerate(as_completed(futures), 1):
            col, spec = future.result()
            sugg[col] = spec
            print(f"[类型建议] {i}/{total_cols} 列 '{col}' => {spec}")

    path = os.path.join(desktop, '字段列表.txt')
    with open(path, 'w', encoding='utf-8') as f:
        for col in cols:
            f.write(f"{col} {sugg[col]}\n")
    print(f"字段列表已生成：{path}\n请编辑完成后按回车继续…")
    input()

    while True:
        print("开始校验字段列表...")
        lines = []
        with open(path, encoding='utf-8') as f:
            for L in f:
                line = L.strip()
                if not line:
                    continue
                parts = line.split(maxsplit=1)
                if len(parts) < 2:
                    print(f"格式行错误：{L}")
                    sys.exit(1)
                col, spec = parts[0].strip(), parts[1].strip()
                lines.append((col, spec))

        # 字段一致性检查
        orig_set = set(cols)
        now_set = set(c for c, _ in lines)
        if orig_set != now_set:
            miss = orig_set - now_set
            extra = now_set - orig_set
            if miss:
                print("字段列表缺少列：", ", ".join(miss))
            if extra:
                print("字段列表多出未知列：", ", ".join(extra))
            input('请修正字段列表.txt 后按回车重新校验…')
            continue

        errors = []
        total_specs = len(lines)
        with ThreadPoolExecutor(max_workers=min(8, max(2, os.cpu_count() or 4))) as executor:
            futures = {executor.submit(_validate_column, col, spec, combined[col]): col for col, spec in lines}
            for i, future in enumerate(as_completed(futures), 1):
                col = futures[future]
                errs = future.result()
                if errs:
                    errors.extend(errs)
                print(f"[校验] {i}/{total_specs} 列 '{col}' 完成, 错误 {len(errs)} 条")
        if not errors:
            break
        print('检测到以下不符合项：')
        for e in errors:
            print(' -', e)
        input('请修改字段列表.txt 后按回车重新校验…')

    tbl = input('格式校验通过！\n请输入要创建的表名：\n').strip()
    with open(path, 'a', encoding='utf-8') as f:
        f.write(f"\nCREATE TABLE `{tbl}` (\n")
        for i, (col, spec) in enumerate(lines):
            t = spec.strip().lower()
            if t == 'datetime':
                sqlt = 'DATETIME'
            elif t.startswith('decimal'):
                # 透传（不设上限）
                sqlt = 'DECIMAL' + spec[spec.find('('):spec.find(')')+1]
            elif t.startswith('varchar'):
                length = spec[spec.find('(')+1:spec.find(')')]
                sqlt = f'VARCHAR({length})'
            else:
                sqlt = spec.upper()
            comma = ',' if i < len(lines)-1 else ''
            f.write(f"  `{col}` {sqlt}{comma}\n")
        f.write(') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n')
    print(f"已在 {path} 末尾追加 CREATE TABLE 语句，任务完成！")

if __name__ == '__main__':
    main()
