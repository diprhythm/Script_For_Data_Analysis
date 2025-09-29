#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shutil
import logging

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logging.info('已创建目标目录: %s', path)

def expand_path(p: str) -> str:
    return os.path.expanduser(os.path.expandvars(p))

def split_name_ext(name: str):
    """
    分离基名与扩展名：
    - 对文件：('a', '.txt')
    - 对没有扩展名的文件或对文件夹：('name', '')
    """
    base, ext = os.path.splitext(name)
    return base, ext

def next_unique_path(dest_dir: str, name: str) -> str:
    """
    在 dest_dir 下生成不重名的新路径：
    - 文件：a.txt -> a(1).txt, a(2).txt ...
    - 文件夹：dir -> dir(1), dir(2) ...
    """
    base, ext = split_name_ext(name)
    candidate = os.path.join(dest_dir, name)
    idx = 1
    while os.path.exists(candidate):
        candidate = os.path.join(dest_dir, f"{base}({idx}){ext}")
        idx += 1
    return candidate

def remove_if_exists(path: str):
    """覆盖模式下，若目标已存在，则删除（文件删文件，文件夹删整树）"""
    if not os.path.exists(path):
        return
    try:
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
        else:
            shutil.rmtree(path)
        logging.info('已删除已存在的目标: %s', path)
    except Exception as e:
        logging.error('删除目标失败 %s: %s', path, e)
        raise

def copy_item(src: str, dst_path: str, overwrite: bool):
    """
    将 src 复制到精确目标路径 dst_path（不是目录，而是最终文件/文件夹路径）
    - 文件：用 copy2
    - 文件夹：用 copytree
    - 覆盖模式：若存在，先删再复制
    """
    if os.path.isdir(src) and not os.path.isfile(src):
        # 目录
        if os.path.exists(dst_path):
            if overwrite:
                remove_if_exists(dst_path)
            else:
                # 调用方应已处理去重，这里直接报错以防逻辑遗漏
                raise FileExistsError(dst_path)
        shutil.copytree(src, dst_path)
        logging.info('已复制文件夹 %s -> %s', src, dst_path)
    elif os.path.isfile(src) or os.path.islink(src):
        # 文件/链接
        if os.path.exists(dst_path) and overwrite:
            # 文件覆盖交给 copy2 也可以，但先删更干净
            remove_if_exists(dst_path)
        shutil.copy2(src, dst_path)
        logging.info('已复制文件 %s -> %s', src, dst_path)
    else:
        logging.warning('未知类型，未处理: %s', src)

def move_item(src: str, dst_path: str, overwrite: bool):
    """
    将 src 移动到精确目标路径 dst_path。
    覆盖模式下，若目标存在，先删再移动，避免跨盘移动时的系统差异。
    """
    if os.path.exists(dst_path) and overwrite:
        remove_if_exists(dst_path)
    # 若目标路径指向已存在目录且未覆盖，则由调用方预先改名；这里直接移动
    shutil.move(src, dst_path)
    logging.info('已剪切 %s -> %s', src, dst_path)

def main():
    # 日志配置
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # 选择“复制”还是“剪切”
    choice = input(
        '1-复制文件/文件夹\n'
        '2-剪切文件/文件夹\n'
        '请选择操作（1 或 2）： '
    ).strip()
    if choice not in {'1', '2'}:
        print('无效的选项，程序退出。')
        return

    # 录入路径
    print('请逐行输入需要操作的文件或文件夹路径，输入空行后自动结束：')
    paths = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line is None:
            break
        path = line.strip()
        if path == '':
            print('\n路径列表录入完毕，请输入目标文件夹路径：')
            break
        paths.append(expand_path(path))

    if not paths:
        print('未录入任何路径，程序退出。')
        return

    # 读取并验证目标目录
    while True:
        dest = input().strip()
        if not dest:
            print('目标路径不能为空，请重新输入：')
            continue
        dest = expand_path(dest)
        try:
            ensure_dir(dest)
            break
        except Exception as e:
            logging.error('创建目录失败 %s: %s', dest, e)
            print('请再次输入有效的目标路径：')

    # 重名策略：None=未决定；True=保留所有(去重)；False=覆盖
    keep_all_on_dup = None

    # 逐个处理
    for src in paths:
        if not os.path.exists(src):
            logging.error('路径未找到: %s', src)
            continue

        name = os.path.basename(src.rstrip(os.sep))
        dest_path = os.path.join(dest, name)
        is_conflict = os.path.exists(dest_path)

        # 如遇到第一次冲突，询问一次并记住策略
        if is_conflict and keep_all_on_dup is None:
            while True:
                ans = input('存在文件名重复的数据，是否保留所有（Y/N）： ').strip().upper()
                if ans in {'Y','N'}:
                    keep_all_on_dup = (ans == 'Y')
                    break
                print('仅接受 Y 或 N，请重新输入。')

        # 根据策略生成最终目标路径 & 执行动作
        try:
            if choice == '1':  # 复制
                if keep_all_on_dup:
                    # 保留所有：冲突则生成不重名路径
                    if is_conflict:
                        dest_path = next_unique_path(dest, name)
                    copy_item(src, dest_path, overwrite=False)
                else:
                    # 覆盖：冲突则直接覆盖（文件覆盖/目录先删再拷）
                    copy_item(src, dest_path, overwrite=True)
            else:  # 剪切
                if keep_all_on_dup:
                    if is_conflict:
                        dest_path = next_unique_path(dest, name)
                    move_item(src, dest_path, overwrite=False)
                else:
                    move_item(src, dest_path, overwrite=True)
        except Exception as e:
            logging.error('处理失败 %s -> %s: %s', src, dest_path, e)

if __name__ == '__main__':
    main()
