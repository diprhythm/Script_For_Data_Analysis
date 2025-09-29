import os
import io
import sys
import msoffcrypto
import zipfile
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== 可调参数 =====
MAX_THREADS = 61         # “拉满”版
VERBOSE = True           # True: 每条密码实时打印；False: 仅里程碑打印
MILESTONE = 100          # VERBOSE=False 时，每尝试多少条打印一次

print_lock = threading.Lock()
found_event = threading.Event()  # 找到真密码后触发，其他线程尽快停止

REQUIRED_ZIP_ENTRIES = ("[Content_Types].xml", "xl/workbook.xml")

def is_valid_xlsx_bytes(buf: bytes) -> bool:
    """
    在内存中判断解密结果是否为合法 .xlsx（ZIP 包含关键文件）。
    """
    try:
        with zipfile.ZipFile(io.BytesIO(buf), 'r') as zf:
            names = set(zf.namelist())
            return all(entry in names for entry in REQUIRED_ZIP_ENTRIES)
    except Exception:
        return False

def try_password(pwd: str, encrypted_bytes: bytes, final_output: Path, invalid_log: Path):
    """
    尝试用单个密码解密（全部在内存中进行）。
    真成功：返回密码；伪成功：记录后返回 None；失败：返回 None。
    """
    if found_event.is_set():
        return None

    if VERBOSE:
        with print_lock:
            print(f"尝试密码：{pwd}", flush=True)

    try:
        # 解析并解密到内存
        office = msoffcrypto.OfficeFile(io.BytesIO(encrypted_bytes))
        office.load_key(password=pwd)
        out_mem = io.BytesIO()
        office.decrypt(out_mem)
        data = out_mem.getvalue()

        # 校验 ZIP 结构
        if is_valid_xlsx_bytes(data):
            # 真正解密成功，落盘
            with open(final_output, "wb") as f:
                f.write(data)
            found_event.set()
            return pwd
        else:
            # 伪成功：msoffcrypto 通过但 ZIP 结构不合法
            with print_lock:
                print(f"伪成功密码：{pwd}", flush=True)
            with open(invalid_log, "a", encoding="utf-8") as log_f:
                log_f.write(pwd + "\n")
            return None

    except Exception:
        # 密码错误或解密失败
        return None

def main():
    print("请输入文件夹路径（包含 passwords.txt 和 Excel 文件）：")
    base = Path(input("路径: ").strip().strip('"'))
    if not base.is_dir():
        print(" 无效目录")
        return

    pw_file = base / "passwords.txt"
    if not pw_file.exists():
        print(" 找不到 passwords.txt")
        return

    excel_name = input("请输入要解密的 Excel 文件名：").strip()
    enc_file = base / excel_name
    if not enc_file.exists():
        print(f" 找不到文件：{excel_name}")
        return

    final_output = base / (enc_file.stem + "_decrypted.xlsx")
    invalid_log = base / "invalid_success_log.txt"
    # 清空旧日志
    try:
        invalid_log.unlink(missing_ok=True)
    except Exception:
        pass

    # 读入密文到内存，减少每次 I/O
    try:
        encrypted_bytes = enc_file.read_bytes()
    except Exception as e:
        print(f" 读取 Excel 失败：{e}")
        return

    # 加载字典（去重+过滤空行）
    with open(pw_file, "r", encoding="utf-8", errors="ignore") as f:
        raw = [line.strip() for line in f]
    passwords = [p for p in dict.fromkeys(raw) if p]  # 保序去重

    print(f" 共加载 {len(passwords)} 条密码，{MAX_THREADS} 线程并行开始尝试\n")

    tried = 0
    found_password = None
    password_txt_path = base / f"{enc_file.stem}_password.txt"

    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as exe:
            futures = {
                exe.submit(try_password, pwd, encrypted_bytes, final_output, invalid_log): pwd
                for pwd in passwords
            }

            for future in as_completed(futures):
                result = future.result()
                tried += 1

                if not VERBOSE and tried % MILESTONE == 0:
                    with print_lock:
                        print(f"已尝试 {tried} 条...", flush=True)

                if result and not found_password:
                    # 记录成功密码（不立刻结束程序）
                    found_password = result
                    found_event.set()

                    # 保存密码到文本文件
                    try:
                        with open(password_txt_path, "w", encoding="utf-8") as pf:
                            pf.write(found_password + "\n")
                        with print_lock:
                            print(f"\n 解密成功！密码是：{found_password}")
                            print(f" 已导出解密文件：{final_output.name}")
                            print(f" 已保存密码到：{password_txt_path}")
                            print(" 正在收尾其它任务，请稍候……")
                    except Exception as e:
                        with print_lock:
                            print(f"\n 解密成功，但写入密码文件失败：{e}")

            # 到这里线程池已退出（任务都完成或早退）
            if not found_password:
                with print_lock:
                    print("\n 全部尝试完毕，未找到正确密码。")

    except KeyboardInterrupt:
        print("\n 中断运行，已尽量停止任务。")

    # 汇总伪密码
    if invalid_log.exists() and invalid_log.stat().st_size > 0:
        print(f"\n 已记录伪成功密码：{invalid_log.name}")
    else:
        print("\n 未检测到伪成功密码。")

    # 最终总结
    if found_password:
        print("\n 本次已成功找回密码，详情见保存的密码文件和解密后的 Excel。")
    print("程序结束。")

if __name__ == "__main__":
    main()
