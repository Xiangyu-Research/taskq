"""The core interface for plugins: recipes and relay plugins import only this module, never other core modules (recipes,
relay_plugins, spec, ...). Which names a plugin file exports is described at the top of cfg.py.

  REQUIRED              write it as a recipe param default = the task must give it
  RelayError            relay plugins raise it when preparing inputs or checking results fails
  walk(p)               files under dir p (relative to p, following symlinks), sorted
  expand(root, rel)     one input under the data dir -> (files, view dir or None)
  sha256(p)             sha256 of a file
  REPO                  the user's repo root (relative paths in task commands are relative to it)
  rel(*parts)           path of a scheduler file relative to REPO
  listed_inputs(spec, root)  run the spec's inputs_cmd once ({data} = root) -> the inputs it prints; no inputs_cmd -> []
Standard library plus cfg.py and common.py only (spec.py is loaded when listed_inputs is used)."""
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
    """Files under dir p (relative to p, following symlinks), sorted."""
    out = []
    for dp, dns, fns in os.walk(p, followlinks=True):
        dns.sort()
        out += [os.path.relpath(os.path.join(dp, f), p) for f in sorted(fns)]
    return out


def expand(root, rel):
    """One input under the data dir -> (files, view dir or None): every file in a dir goes in and the whole dir becomes a view; a
    single file is not a view."""
    p = os.path.join(root, rel)
    if os.path.isdir(p):
        fs = walk(p)
        if not fs:
            raise RelayError(f"input {rel} is empty")
        return [(f"{rel}/{f}", os.path.realpath(os.path.join(p, f))) for f in fs], rel
    if os.path.isfile(p):
        return [(rel, os.path.realpath(p))], None
    raise RelayError(f"no input {rel} on the hub")


def sha256(p):
    return C.sha256(p)


def listed_inputs(spec, root):
    if not spec.get("inputs_cmd"):
        return []
    import spec as S
    try:
        return S.listed_inputs(spec["inputs_cmd"], root, spec.get("vars"))
    except S.SpecError as e:
        raise RelayError(f"{spec['key']}: {e}")
