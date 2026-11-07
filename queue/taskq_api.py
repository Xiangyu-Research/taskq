"""插件用的核心接口：做法、中继插件只 import 这个模块，不 import 核心的其他模块（recipes、relay_plugins、spec…）。
插件文件要导出哪些名字见 cfg.py 开头。

  REQUIRED              做法的参数默认值写它 = 任务必须给
  RelayError            中继插件准备输入、核对结果出错时报它
  walk(p)               文件夹 p 下的文件（相对 p，跟着软链接走），排好序
  expand(root, rel)     数据目录下的一个输入 → (files, 视图文件夹或 None)
  sha256(p)             文件的 sha256
  REPO                  使用者的仓库根（任务命令里的相对路径相对它）
  rel(*parts)           调度代码里的文件相对 REPO 的路径
  listed_inputs(spec, root)  任务说明的 inputs_cmd 真跑一次（{data} = root）→ 它打印的输入；没写 inputs_cmd → []
只用标准库和 cfg.py、common.py（listed_inputs 用时才载入 spec.py）。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cfg as _cfg  # noqa: E402
import common as C  # noqa: E402

REPO = _cfg.REPO
rel = _cfg.rel


REQUIRED = object()


class RelayError(Exception):
    pass


def walk(p):
    """文件夹 p 下的文件（相对 p，跟着软链接走），排好序。"""
    out = []
    for dp, dns, fns in os.walk(p, followlinks=True):
        dns.sort()
        out += [os.path.relpath(os.path.join(dp, f), p) for f in sorted(fns)]
    return out


def expand(root, rel):
    """数据目录下的一个输入 → (files, 视图文件夹或 None)：文件夹里的文件都放进去、整个文件夹当视图；单个文件不当视图。"""
    p = os.path.join(root, rel)
    if os.path.isdir(p):
        fs = walk(p)
        if not fs:
            raise RelayError(f"输入 {rel} 是空的")
        return [(f"{rel}/{f}", os.path.realpath(os.path.join(p, f))) for f in fs], rel
    if os.path.isfile(p):
        return [(rel, os.path.realpath(p))], None
    raise RelayError(f"hub 上没有输入 {rel}")


def sha256(p):
    return C.sha256(p)


def listed_inputs(spec, root):
    if not spec.get("inputs_cmd"):
        return []
    import spec as S
    try:
        return S.listed_inputs(spec["inputs_cmd"], root, spec.get("vars"))
    except S.SpecError as e:
        raise RelayError(f"{spec['key']}：{e}")
