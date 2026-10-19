#!/bin/bash
# 大批数据从 box 传到 ssh 很慢的机器（现在是 lab，box → lab 的 ssh 单路只有约 1 MB/s）：box 临时开文件服务，经 box 的
# Cloudflare 隧道（固定地址，envs/machines.py 里 hub 的 transfer），那边用 curl 并行下载。数据会经过 Cloudflare。
#
# 在 box 上运行（会跑几十分钟，放在 tmux 里或后台）：
#   scripts/sync/tunnel_push.sh <机器> <box 上的源目录> <那边的目标目录（相对 home）> <子目录>...
# 例（外部集的推理输入）：
#   scripts/sync/tunnel_push.sh lab /data/others/BRAIN_TUMOR/nnUNet/raw/RQ1_external BRAIN_TUMOR/nnUNet/raw/RQ1_external \
#       MEN/imagesTs MET-UCSF/imagesTs MET-Yale/imagesTs PEDs/imagesTs SSA/imagesTs UCSF/imagesTs UPENN/imagesTs
#
# 做法和安全措施：
# - 文件服务（python http.server）只在传输期间开，监听 docker 网络的网关地址，只有 traefik 转过来（10/19 起；
#   之前用 trycloudflare.com 临时隧道，10/19 晚新地址解析不到）。同一时间只跑一个传输，后来的排队等。
# - 数据放在随机 32 位字符的路径下面，根目录只返回 "ok"，看不到文件列表。
# - 软链接在 box 上解析，那边存成真实文件。那边已经存在、大小相同的文件跳过，所以中断后重新运行会接着传。
# - 结束时（包括出错、Ctrl-C）一定关掉文件服务，删掉那边的临时文件。
# - 最后用 rsync 校验和逐个文件核对那边和 box（只传校验和），列出不一致的文件。
# - 传之前在那边每个子目录里放一个 .incomplete，核对一致后才删：中途出错留下的半份数据，refill.py 不会当成"数据在"。
# - 那边的下载进程没了、又没写"完成"，或者连续 1 小时连不上那边，就出错退出（不一直等）。
set -euo pipefail

HOST=${1:?用法：tunnel_push.sh <机器> <源目录> <目标目录> <子目录>...}
SRC=${2:?用法：tunnel_push.sh <机器> <源目录> <目标目录> <子目录>...}
DEST=${3:?用法：tunnel_push.sh <机器> <源目录> <目标目录> <子目录>...}
shift 3
REPO=$(git -C "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" rev-parse --show-toplevel)
H=$(python3 "$REPO/envs/machines.py" get "$HOST" ssh)
[[ -n $H ]] || { echo "envs/machines.py 里没有可以传过去的机器 $HOST" >&2; exit 2; }
(( $# > 0 )) || { echo "至少给一个子目录" >&2; exit 2; }
SUBS=("$@")
SRC=$(cd "$SRC" && pwd)
NPAR=8
read -r URL BIND < <(python3 "$REPO/envs/machines.py" get "$(python3 "$REPO/envs/machines.py" names hub)" transfer)

W=$(mktemp -d)
cleanup() {
    [[ -n ${HTTP_PID:-} ]] && kill "$HTTP_PID" 2> /dev/null
    ssh "$H" "rm -f /tmp/tunnel_fetch_$$.sh /tmp/tunnel_files_$$.tsv /tmp/tunnel_fetch_$$.log" 2> /dev/null || true
    rm -rf "$W"
    echo "$(date '+%F %T') 文件服务已关闭，临时文件已删除"
}
trap cleanup EXIT

# 文件清单：相对路径 + 字节数（软链接按目标算）
(cd "$SRC" && find -L "${SUBS[@]}" -type f -printf '%p\t%s\n' | sort) > "$W/files.tsv"
N=$(wc -l < "$W/files.tsv")
echo "$(date '+%F %T') 要传 $N 个文件，$(awk -F'\t' '{s+=$2} END {printf "%.2f GB", s/1e9}' "$W/files.tsv")"
(( N > 0 )) || { echo "没有文件" >&2; exit 1; }

mkdir -p ~/.cache/phd
exec 9> ~/.cache/phd/tunnel_push.lock
flock -n 9 || { echo "$(date '+%F %T') 另一个传输在跑，等它结束"; flock 9; }
TOKEN=$(head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32)
mkdir "$W/serve"
echo ok > "$W/serve/index.html"
ln -s "$SRC" "$W/serve/$TOKEN"
python3 -m http.server "${BIND##*:}" --bind "${BIND%:*}" --directory "$W/serve" > "$W/http.log" 2>&1 &
HTTP_PID=$!
UP=
for _ in $(seq 30); do
    [[ $(curl -s -m 10 "$URL/" || true) == ok ]] && { UP=1; break; }
    sleep 2
done
[[ -n $UP ]] || { echo "$URL 连不到文件服务（文件服务、traefik 的路由或防火墙）：" >&2; tail -5 "$W/http.log" >&2; exit 1; }
echo "$(date '+%F %T') 文件服务已开（路径只在本次运行内有效）"

# 那边：按清单并行下载
BASE="$URL/$TOKEN"
scp -q "$W/files.tsv" "$H:/tmp/tunnel_files_$$.tsv"
ssh "$H" "cat > /tmp/tunnel_fetch_$$.sh" << EOF
#!/bin/bash
mkdir -p ~/$DEST && cd ~/$DEST
fetch() {  # fetch <相对路径> <字节数>
    [[ -f \$1 && \$(stat -c %s "\$1") == \$2 ]] && return 0
    curl -sf --retry 5 --retry-all-errors --create-dirs -o "\$1" "$BASE/\$1" || echo "失败 \$1"
}
export -f fetch
tr '\t' ' ' < /tmp/tunnel_files_$$.tsv | xargs -P $NPAR -L 1 bash -c 'fetch "\$0" "\$1"'
echo "完成 \$(date)"
EOF
MARKS=$(printf " ~/$DEST/%q/.incomplete" "${SUBS[@]}")
ssh "$H" "for f in $MARKS; do mkdir -p \"\$(dirname \$f)\" && touch \$f; done"
ssh "$H" "nohup bash /tmp/tunnel_fetch_$$.sh > /tmp/tunnel_fetch_$$.log 2>&1 < /dev/null &"
T0=$(date +%s) LAST_OK=$(date +%s)
while :; do
    # [/]tmp：不让 pgrep 匹配到这条命令自己所在的 shell
    st=$(ssh "$H" "if grep -q 完成 /tmp/tunnel_fetch_$$.log; then echo done; elif pgrep -f '[/]tmp/tunnel_fetch_$$.sh' > /dev/null; then echo run; else echo dead; fi") || st=
    [[ $st == done ]] && break
    [[ $st == dead ]] && { echo "$(date '+%F %T') $HOST 上的下载进程没了、没有写完成：" >&2; ssh "$H" "tail -5 /tmp/tunnel_fetch_$$.log" >&2 || true; exit 1; }
    [[ -n $st ]] && LAST_OK=$(date +%s)
    (( $(date +%s) - LAST_OK < 3600 )) || { echo "$(date '+%F %T') 连续 1 小时连不上 $HOST" >&2; exit 1; }
    sleep 60
    GOT=$(ssh "$H" "cd ~/$DEST && du -sbL ${SUBS[*]} 2> /dev/null | awk '{s+=\$1} END {print s+0}'") || GOT=0
    echo "$(date '+%F %T') 已下载 $(awk -v g="$GOT" -v t="$(( $(date +%s) - T0 ))" 'BEGIN {printf "%.1f GB，平均 %.1f MB/s", g/1e9, g/1e6/t}')"
done
FAIL=$(ssh "$H" "grep -c '^失败' /tmp/tunnel_fetch_$$.log" || true)
echo "$(date '+%F %T') 下载结束，用时 $(( ($(date +%s) - T0) / 60 )) 分钟，失败 ${FAIL:-0} 个"
kill "$HTTP_PID"; HTTP_PID=

# 核对：rsync 只比较校验和，不传数据；列出那边缺少或内容不同的文件
echo "$(date '+%F %T') 用校验和核对（下面列出的是不一致的文件，没有列出 = 一致）"
BAD=$(cd "$SRC" && rsync -rLc -n --itemize-changes -R "${SUBS[@]}" "$H:$DEST/" | grep -v '^\.' || true)
if [[ -n $BAD ]]; then
    echo "$BAD" | head -20
    echo "不一致：$(echo "$BAD" | wc -l) 个（重新运行本脚本会补传大小不对的文件；大小对但内容不对的，先在 $HOST 上删掉再运行）"
    exit 1
fi
ssh "$H" "rm -f $MARKS"
echo "$(date '+%F %T') 全部 $N 个文件一致"
