"""调度脚本共用的小工具（2026-10-25），只用标准库（lab、DICC 上用系统的 python3）。"""
import json
import os


def write_json(path, obj, indent=1):
    """整份写到临时文件、落盘以后再换过去：断电、被杀时文件要么是旧的、要么是新的，不会只写一半。"""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(obj, f, ensure_ascii=False, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
