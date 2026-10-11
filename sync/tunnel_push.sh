#!/bin/bash
# 大批数据从 box 传到 lab：box 临时开一个 Cloudflare 隧道，lab 用 curl 并行下载（约 25 MB/s；普通 ssh 只有 1–2 MB/s）。
# 数据会经过 Cloudflare：每次使用前都要用户明确同意。
#
# 在 box 上运行（会跑几十分钟，放在 tmux 里或后台）：
#   scripts/sync/tunnel_to_lab.sh <box 上的源目录> <lab 上的目标目录（相对 home）> <子目录>...
# 例（外部集的推理输入）：
#   scripts/sync/tunnel_to_lab.sh /data/others/BRAIN_TUMOR/nnUNet/raw/RQ1_external BRAIN_TUMOR/nnUNet/raw/RQ1_external \
#       MEN/imagesTs MET-UCSF/imagesTs MET-Yale/imagesTs PEDs/imagesTs SSA/imagesTs UCSF/imagesTs UPENN/imagesTs
#
# 做法和安全措施：
# - 文件服务（python http.server）只监听 box 本机，隧道是 cloudflared 的临时隧道（随机 trycloudflare.com 地址，不用账号）。
# - 数据放在随机 32 位字符的路径下面，根目录只返回 "ok"，看不到文件列表。
# - 软链接在 box 上解析，lab 上存成真实文件。lab 上已经存在、大小相同的文件跳过，所以中断后重新运行会接着传。
# - 结束时（包括出错、Ctrl-C）一定关掉隧道和文件服务，删掉 cloudflared 和 lab 上的临时文件。
# - 最后用 rsync 校验和逐个文件核对 lab 和 box（只传校验和），列出不一致的文件。
set -euo pipefail

SRC=${1:?用法：tunnel_to_lab.sh <源目录> <lab 目标目录> <子目录>...}
DEST=${2:?用法：tunnel_to_lab.sh <源目录> <lab 目标目录> <子目录>...}
shift 2
(( $# > 0 )) || { echo "至少给一个子目录" >&2; exit 2; }
SUBS=("$@")
SRC=$(cd "$SRC" && pwd)
NPAR=8
PORT=$(( 20000 + RANDOM % 20000 ))

W=$(mktemp -d)
cleanup() {
    [[ -n ${CF_PID:-} ]] && kill "$CF_PID" 2> /dev/null
    [[ -n ${HTTP_PID:-} ]] && kill "$HTTP_PID" 2> /dev/null
    ssh lab "rm -f /tmp/tunnel_fetch_$$.sh /tmp/tunnel_files_$$.tsv /tmp/tunnel_fetch_$$.log" 2> /dev/null || true
    rm -rf "$W"
    echo "$(date '+%F %T') 隧道和文件服务已关闭，临时文件已删除"
}
trap cleanup EXIT

# 文件清单：相对路径 + 字节数（软链接按目标算）
(cd "$SRC" && find -L "${SUBS[@]}" -type f -printf '%p\t%s\n' | sort) > "$W/files.tsv"
N=$(wc -l < "$W/files.tsv")
echo "$(date '+%F %T') 要传 $N 个文件，$(awk -F'\t' '{s+=$2} END {printf "%.2f GB", s/1e9}' "$W/files.tsv")"
(( N > 0 )) || { echo "没有文件" >&2; exit 1; }

curl -sfL -o "$W/cloudflared" https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
chmod +x "$W/cloudflared"
TOKEN=$(head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32)
mkdir "$W/serve"
echo ok > "$W/serve/index.html"
ln -s "$SRC" "$W/serve/$TOKEN"
python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$W/serve" > "$W/http.log" 2>&1 &
HTTP_PID=$!
"$W/cloudflared" tunnel --no-autoupdate --url "http://127.0.0.1:$PORT" > "$W/cf.log" 2>&1 &
CF_PID=$!
URL=
for _ in $(seq 30); do
    URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "$W/cf.log" | head -1) || true
    [[ -n $URL ]] && [[ $(curl -s -m 10 "$URL/" || true) == ok ]] && break
    URL=; sleep 2
done
[[ -n $URL ]] || { echo "隧道没有建起来：" >&2; tail -5 "$W/cf.log" >&2; exit 1; }
echo "$(date '+%F %T') 隧道已建立（地址和路径只在本次运行内有效）"

# lab 端：按清单并行下载
BASE="$URL/$TOKEN"
scp -q "$W/files.tsv" "lab:/tmp/tunnel_files_$$.tsv"
ssh lab "cat > /tmp/tunnel_fetch_$$.sh" << EOF
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
ssh lab "nohup bash /tmp/tunnel_fetch_$$.sh > /tmp/tunnel_fetch_$$.log 2>&1 < /dev/null &"
T0=$(date +%s)
until ssh lab "grep -q 完成 /tmp/tunnel_fetch_$$.log"; do
    sleep 60
    GOT=$(ssh lab "cd ~/$DEST && du -sbL ${SUBS[*]} 2> /dev/null | awk '{s+=\$1} END {print s+0}'") || GOT=0
    echo "$(date '+%F %T') 已下载 $(awk -v g="$GOT" -v t="$(( $(date +%s) - T0 ))" 'BEGIN {printf "%.1f GB，平均 %.1f MB/s", g/1e9, g/1e6/t}')"
done
FAIL=$(ssh lab "grep -c '^失败' /tmp/tunnel_fetch_$$.log" || true)
echo "$(date '+%F %T') 下载结束，用时 $(( ($(date +%s) - T0) / 60 )) 分钟，失败 ${FAIL:-0} 个"
kill "$CF_PID" "$HTTP_PID"; CF_PID= HTTP_PID=

# 核对：rsync 只比较校验和，不传数据；列出 lab 上缺少或内容不同的文件
echo "$(date '+%F %T') 用校验和核对（下面列出的是不一致的文件，没有列出 = 一致）"
BAD=$(cd "$SRC" && rsync -rLc -n --itemize-changes -R "${SUBS[@]}" "lab:$DEST/" | grep -v '^\.' || true)
if [[ -n $BAD ]]; then
    echo "$BAD" | head -20
    echo "不一致：$(echo "$BAD" | wc -l) 个（重新运行本脚本会补传大小不对的文件；大小对但内容不对的，先在 lab 上删掉再运行）"
    exit 1
fi
echo "$(date '+%F %T') 全部 $N 个文件一致"
