#!/bin/bash
# Link this checkout's skills (skills/taskq-*) into ~/.claude/skills so Claude Code finds them in every project. Repeating is fine:
# new skills are linked, links to skills this checkout no longer has are removed, and nothing else there is touched (a real directory
# or a link to somewhere else with the same name is reported and kept, exit 1). No ~/.claude on this machine: nothing to do.
#   bash skills/link.sh
set -euo pipefail
shopt -s nullglob
SRC=$(cd "$(dirname "$(readlink -f "$0")")" && pwd -P)
DEST=$HOME/.claude/skills
[[ -d $HOME/.claude ]] || exit 0
mkdir -p "$DEST"
rc=0
for d in "$SRC"/taskq-*/; do
    want=${d%/}
    name=${want##*/}
    at=$DEST/$name
    [[ -f $want/SKILL.md ]] || continue
    if [[ -L $at ]]; then
        cur=$(readlink "$at")
        [[ $cur == "$want" ]] && continue
        [[ $cur == "$SRC"/* ]] || { echo "skills: $at points to $cur, kept" >&2; rc=1; continue; }
    elif [[ -e $at ]]; then
        echo "skills: $at exists and is not a link, kept" >&2
        rc=1
        continue
    fi
    ln -sfn "$want" "$at"
    echo "skills: linked $name"
done
for at in "$DEST"/taskq-*; do
    [[ -L $at && ! -e $at && $(readlink "$at") == "$SRC"/* ]] || continue
    rm "$at"
    echo "skills: removed ${at##*/}"
done
exit $rc
