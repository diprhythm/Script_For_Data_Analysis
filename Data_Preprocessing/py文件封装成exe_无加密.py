# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import shutil
import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

AUTHOR = "Noel&ChatGPT"
VERSION = "1.0.0.0"
# 常用库隐藏导入，确保打包后的 exe 能在 cmd 正常运行
DEFAULT_HIDDEN_IMPORTS = ["pandas", "numpy", "openpyxl", "xlsxwriter"]


def clean_path(path: str) -> str:
    """去除控制字符并规范化路径"""
    path = re.sub(r"[\u200e\u200f\u202a-\u202e]", "", path)
    return os.path.normpath(path.strip().strip('"'))


def generate_version_file(directory: str, exe_name: str) -> str:
    """生成版本信息文件，用于 PyInstaller 注入元数据"""
    content = f"""# UTF-8
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=({VERSION.replace('.', ',')}), prodvers=({VERSION.replace('.', ',')}),
    mask=0x3f, flags=0x0, OS=0x4, fileType=0x1, subtype=0x0, date=(0,0)
  ),
  kids=[
    StringFileInfo([
      StringTable(
        '040904b0',
        [
          StringStruct('CompanyName','{AUTHOR}'),
          StringStruct('FileDescription','{exe_name}'),
          StringStruct('FileVersion','{VERSION}'),
          StringStruct('InternalName','{exe_name}'),
          StringStruct('LegalCopyright','{AUTHOR}'),
          StringStruct('OriginalFilename','{exe_name}.exe'),
          StringStruct('ProductName','{exe_name}'),
          StringStruct('ProductVersion','{VERSION}')
        ]
      )
    ]),
    VarFileInfo([VarStruct('Translation',[1033,1200])])
  ]
)
"""
    vf = os.path.join(directory, f"{exe_name}_version.txt")
    with open(vf, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info("✔ 生成版本文件: %s", vf)
    return vf


def check_pyinstaller():
    """检查 PyInstaller 是否安装，否则自动安装"""
    try:
        import PyInstaller
        logger.info("检测到 PyInstaller")
    except ImportError:
        logger.info("未检测到 PyInstaller，正在安装...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        logger.info("PyInstaller 安装完成")


def build_executable(py_file: str):
    """打包任意 Python 脚本为单文件 exe，并注入作者信息"""
    py_file = clean_path(py_file)
    if not os.path.isfile(py_file) or not py_file.lower().endswith('.py'):
        logger.error("无效的脚本路径: %s", py_file)
        return

    work_dir, filename = os.path.split(py_file)
    exe_base = os.path.splitext(filename)[0]
    os.chdir(work_dir)

    # 生成版本信息文件
    ver_file = generate_version_file(work_dir, exe_base)

    # 构造 PyInstaller 命令
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--clean",
        "--noconfirm",
        "--log-level=INFO",
        f"--version-file={ver_file}"
    ]
    # 加入默认隐藏导入
    for mod in DEFAULT_HIDDEN_IMPORTS:
        cmd.append(f"--hidden-import={mod}")
        cmd.append(f"--collect-all={mod}")
    cmd.append(filename)

    logger.info("开始打包: %s", ' '.join(cmd))
    result = subprocess.run(cmd)

    # 移动可执行文件
    dist_exe = os.path.join(work_dir, 'dist', exe_base + '.exe')
    final_exe = os.path.join(work_dir, exe_base + '.exe')
    if result.returncode == 0 and os.path.exists(dist_exe):
        try:
            shutil.move(dist_exe, final_exe)
            logger.info("打包成功并移动到: %s", final_exe)
        except Exception as e:
            logger.error("移动失败: %s", e)
    else:
        logger.error("打包失败 (exit code %d)", result.returncode)

    # 清理中间文件
    for item in ['build', 'dist', '__pycache__', exe_base + '.spec', os.path.basename(ver_file)]:
        path = os.path.join(work_dir, item)
        try:
            if os.path.isdir(path): shutil.rmtree(path)
            elif os.path.isfile(path): os.remove(path)
        except Exception:
            logger.warning("清理失败: %s", path)


def main():
    check_pyinstaller()
    print("请输入要打包的 Python 脚本路径:")
    path = input('>>> ')
    build_executable(path)

if __name__ == '__main__':
    main()
