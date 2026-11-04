#!/bin/bash
# 大批数据从 box 传到别的机器（lab：box → lab 的 ssh 单路只有约 1 MB/s；DICC：经 VPN 的 ssh 和别的传输一起跑时会被挤住）：
# 经 box 的 Cloudflare 隧道，由 scripts/taskq/sync/tunnel.py push 传（那边下载；DICC 上 1 GB 以上的在 CPU 节点上下载）。数据会经过 Cloudflare。
#
# 在 box 上运行（会跑几十分钟，放在 tmux 里或后台）：
#   scripts/taskq/sync/tunnel_push.sh <机器> <box 上的源目录> <那边的目标目录（相对 home）> <子目录>...
# 例（外部集的推理输入）：
#   scripts/taskq/sync/tunnel_push.sh lab /data/others/BRAIN_TUMOR/nnUNet/raw/RQ1_external BRAIN_TUMOR/nnUNet/raw/RQ1_external \
#       MEN/imagesTs MET-UCSF/imagesTs MET-Yale/imagesTs PEDs/imagesTs SSA/imagesTs UCSF/imagesTs UPENN/imagesTs
#
# 做法和安全措施（传输本身的见 scripts/sync/README.md）：
# - 软链接在 box 上解析，那边存成真实文件。那边已经存在、大小和修改时间相同的文件跳过，所以中断后重新运行会接着传。
# - 最后两边各算 MD5 逐个文件核对（顺着软链接读），列出不一致的文件。
# - 传之前在那边每个子目录里放一个 .incomplete，核对一致后才删：中途出错留下的半份数据，refill.py 不会当成"数据在"。
set -euo pipefail

HOST=${1:?用法：tunnel_push.sh <机器> <源目录> <目标目录> <子目录>...}
SRC=${2:?用法：tunnel_push.sh <机器> <源目录> <目标目录> <子目录>...}
DEST=${3:?用法：tunnel_push.sh <机器> <源目录> <目标目录> <子目录>...}
shift 3
CORE=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)  # 调度代码的根；使用者的仓库根 REPO（TASKQ_REPO，没设时同 cfg.py）
REPO=${TASKQ_REPO:-$CORE}; [[ -z ${TASKQ_REPO:-} && $CORE == */scripts/taskq ]] && REPO=${CORE%/scripts/taskq}
H=$(python3 "${TASKQ_CONFIG:-$REPO/envs}/machines.py" get "$HOST" ssh)
[[ -n $H ]] || { echo "envs/machines.py 里没有可以传过去的机器 $HOST" >&2; exit 2; }
(( $# > 0 )) || { echo "至少给一个子目录" >&2; exit 2; }
SUBS=("$@")
SRC=$(cd "$SRC" && pwd)
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT

(cd "$SRC" && find -L "${SUBS[@]}" -type f | sort) > "$W/files.txt"
N=$(wc -l < "$W/files.txt")
(( N > 0 )) || { echo "没有文件" >&2; exit 1; }
MARKS=$(printf " ~/$DEST/%q/.incomplete" "${SUBS[@]}")
ssh "$H" "for f in $MARKS; do mkdir -p \"\$(dirname \$f)\" && touch \$f; done"
python3 "$CORE/sync/tunnel.py" push "$HOST" "$SRC" "$DEST" "$W/files.txt"

# 核对：两边各算 MD5（顺着软链接读，那边是软链接也算一致），列出那边缺少或内容不同的文件
echo "$(date '+%F %T') 用校验和核对（下面列出的是不一致的文件，没有列出 = 一致）"
BAD=$(diff <(cd "$SRC" && xargs -d '\n' -a "$W/files.txt" md5sum) \
           <(ssh "$H" "cd $(printf %q "$DEST") && xargs -d '\n' md5sum 2> /dev/null" < "$W/files.txt") \
      | sed -n 's/^< [0-9a-f]*  //p' || true)
if [[ -n $BAD ]]; then
    echo "$BAD" | head -20
    echo "不一致：$(echo "$BAD" | wc -l) 个（重新运行本脚本会补传大小不对的文件；大小对但内容不对的，先在 $HOST 上删掉再运行）"
    exit 1
fi
ssh "$H" "rm -f $MARKS"
echo "$(date '+%F %T') 全部 $N 个文件一致"
