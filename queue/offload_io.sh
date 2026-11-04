#!/bin/bash
# box 和别的机器之间传一步的输入、结果（2026-10-22）：advance.py 按 envs/policy.py 的 PLACE 把步骤放到别处时用，在 box 的 wait 组跑：
#   offload_io.sh push <机器> <相对数据目录的路径>...   box → 那边：文件夹经 scripts/taskq/sync/tunnel_push.sh，文件经 tunnel.py push，再用 rsync 补齐、核对
#   offload_io.sh pull <机器> <相对数据目录的文件夹>     那边 → box：rsync，再按校验和核对一遍，不一致就以非 0 退出
# 只新增 / 更新，不删任何一边的文件。
set -euo pipefail
OP=${1:?用法：offload_io.sh push|pull <机器> <路径>...} HOST=${2:?缺机器}
shift 2
(( $# > 0 )) || { echo "至少给一个路径" >&2; exit 2; }
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
M="python3 ${TASKQ_CONFIG:-$REPO/envs}/machines.py"
source "$CORE/queue/legacy_names.sh" map
H=$($M get "$HOST" ssh)
[[ -n $H ]] || { echo "envs/machines.py 里没有可以连过去的机器 $HOST" >&2; exit 2; }
RD=$($M get "$HOST" data) LD=${TASKQ_DATA:-$($M get . data)}
RD=${RD/#\~\//}
for p in "$@"; do [[ $p != /* && $p != *..* && $p == */* ]] || { echo "路径要相对数据目录、至少两层：$p" >&2; exit 2; }; done

if [[ $OP == push ]]; then
    files=()
    for p in "$@"; do
        p=${p%/}
        if [[ -d $LD/$p ]]; then
            d=$(dirname "$p")
            bash "$CORE/sync/tunnel_push.sh" "$HOST" "$LD/$d" "$RD/$d" "$(basename "$p")"
        elif [[ -f $LD/$p ]]; then
            files+=("$p")
        else
            echo "box 上没有 $LD/$p" >&2; exit 1
        fi
    done
    if (( ${#files[@]} )); then
        echo "$(date '+%F %T') rsync ${#files[@]} 个文件到 $HOST"
        printf '%s\n' "${files[@]}" > "$LD/.offload_files_$$"
        python3 "$CORE/sync/tunnel.py" push "$HOST" "$LD" "$RD" "$LD/.offload_files_$$" || true
        rm -f "$LD/.offload_files_$$"
        (cd "$LD" && rsync -aRLK --partial "${files[@]}" "$H:$RD/")
        diff=$(cd "$LD" && rsync -aRLKc -n --out-format='%n' "${files[@]}" "$H:$RD/")
        diff=$(grep -v '/$' <<< "$diff" || true)
        [[ -z $diff ]] || { echo "核对不一致：$diff" >&2; exit 1; }
    fi
    echo "$(date '+%F %T') 传完"
elif [[ $OP == pull ]]; then
    (( $# == 1 )) || { echo "pull 只给一个文件夹" >&2; exit 2; }
    p=${1%/}
    mkdir -p "$LD/$p"
    echo "$(date '+%F %T') 从 $HOST 拉回 $p"
    # 先拷子文件夹（各测试集的结果），最上层的文件（MANIFEST.json、.infer_done）最后拷：先到的话 box 上就像已经做完了（10/24）
    python3 "$CORE/sync/tunnel.py" rsync -a --partial --include='/*/' --exclude='/*' "$H:$RD/$p/" "$LD/$p/"
    python3 "$CORE/sync/tunnel.py" rsync -a --partial "$H:$RD/$p/" "$LD/$p/"
    diff=$(rsync -ac -n --out-format='%n' "$H:$RD/$p/" "$LD/$p/")
    diff=$(grep -v '/$' <<< "$diff" || true)
    [[ -z $diff ]] || { echo "核对不一致：$diff" | head -20 >&2; exit 1; }
    echo "$(date '+%F %T') 拉回、核对完：$(du -sh "$LD/$p" | cut -f1)"
else
    echo "用法：offload_io.sh push|pull <机器> <路径>..." >&2; exit 2
fi
