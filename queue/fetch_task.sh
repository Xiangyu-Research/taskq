#!/bin/bash
# 把某台机器（envs/machines.py 里的名字，如 dicc、lab）上训完的一个任务拉回 box（2026-10-18，取代 fetch_fold.sh）。
# 由 box 上的 scripts/queue/advance.py 在那边训完以后放进 wait 组：
#   q wait --label fetch:<标签> -- 'bash scripts/queue/fetch_task.sh <机器> <key>'
# 结果文件夹、训完的标志、不拉的文件、上一级里还要拉的文件都按任务说明（scripts/queue/spec.py lookup <key>：实验表或训练队列）：
#   out、done、fetch.exclude、fetch.extra（如模型文件夹里推理要用的配置文件）、fetch.also（另外要拉的文件夹，
#   相对数据目录，先于 out 拉；如五折集成的预测）、log。
# 那边还没训完时每 10 分钟查一次，每小时记一行最新训练日志的情况（pueue log 里能看到）；不设等待上限，没进展由健康检查提醒。
# 只新增 / 更新，不删 box 上的文件；box 上已经训完（done 都有）时直接结束。训完的标志最后拉。拉回失败重试 3 次。
set -euo pipefail
HOST=${1:?用法：fetch_task.sh <机器> <key>} KEY=${2:?用法：fetch_task.sh <机器> <key>}
REPO=$(git -C "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" rev-parse --show-toplevel)
M="python3 ${TASKQ_CONFIG:-$REPO/envs}/machines.py"
source "$REPO/scripts/queue/legacy_names.sh" map
SSH=$($M get "$HOST" ssh)
[[ -n $SSH ]] || { echo "envs/machines.py 里没有可以拉回的机器 $HOST" >&2; exit 2; }
RDATA=$($M get "$HOST" data) LDATA=${TASKQ_DATA:-$($M get . data)}
SPEC=$(python3 "$REPO/scripts/queue/spec.py" lookup "$KEY") || { echo "找不到任务 $KEY（实验表、训练队列里都没有）" >&2; exit 2; }
# 从说明里取：out；done 的 shell 判断（在 out 里跑）；不拉的文件（每行一个）；上一级里要拉的文件（每行一个）；训练日志的 glob
sp() { python3 -c '
import json, shlex, sys
s = json.loads(sys.stdin.read()); f = sys.argv[1]
if f == "out": print(s["out"])
elif f == "test": print(" && ".join("{ " + " || ".join("test -e " + shlex.quote(a) for a in d.split("|")) + "; }" for d in s["done"]))
elif f in ("exclude", "extra", "also"): print("\n".join((s.get("fetch") or {}).get(f, [])))
elif f == "donefiles": print("\n".join(a for d in s["done"] for a in d.split("|")))
elif f == "log": print(s.get("log") or "")
' "$1" <<< "$SPEC"; }
OUT=$(sp out) TEST=$(sp test) LOGPAT=$(sp log)
mapfile -t EXCL < <(sp exclude | grep . || true)
mapfile -t EXTRA < <(sp extra | grep . || true)
mapfile -t ALSO < <(sp also | grep . || true)
mapfile -t DONEF < <(sp donefiles | grep . || true)
SRC=$RDATA/$OUT DST=$LDATA/$OUT PARENT=$(dirname "$OUT")
done_here() { [[ -d $DST ]] && (cd "$DST" && eval "$TEST"); }
if done_here; then
    echo "$(date '+%F %T') box 上已经有训完的 $KEY，不用拉"; exit 0
fi
# 那边的训练进度：最新的训练日志多久前更新、最后一行
PROGRESS="f=\$(ls -t $SRC/$LOGPAT 2> /dev/null | head -1)
if [[ -z \$f ]]; then echo 还没有训练日志; else
echo \"\$(basename \$f) \$(( (\$(date +%s) - \$(stat -c %Y \$f)) / 60 )) 分钟前更新：\$(tail -n 1 \$f | cut -c1-120)\"; fi"
echo "$(date '+%F %T') 等 $HOST 上 $KEY 训完"
n=0
until ssh -o BatchMode=yes -o ConnectTimeout=30 "$SSH" "cd $SRC 2> /dev/null && $TEST"; do
    if (( n % 6 == 0 )); then echo "$(date '+%F %T') $HOST：$(ssh -o BatchMode=yes -o ConnectTimeout=30 "$SSH" "$PROGRESS" 2> /dev/null || echo 连不上)"; fi
    n=$((n + 1))
    sleep "${FETCH_TEST_POLL:-600}"
done
echo "$(date '+%F %T') 训完了，拉回 → $DST"
mkdir -p "$DST"
EX=() EXD=()
for x in ${EXCL[@]+"${EXCL[@]}"}; do EX+=(--exclude="$x"); done
# 训完的标志最后拉：先拉了标志的话，拉到一半时 box 上就像已经训完了，advance.py 会提前排评测（10/23）；
# 第一遍也不拉最上层的文件（推理的 MANIFEST.json、训练的 checkpoint 等），子文件夹拉完再拉（10/24）
for x in ${DONEF[@]+"${DONEF[@]}"}; do EXD+=(--exclude="/$x"); done
EXD+=(--include='/*/' --exclude='/*')
for i in 1 2 3; do
    ok=1
    for x in ${EXTRA[@]+"${EXTRA[@]}"}; do
        rsync -rlt --partial --update "$SSH:$RDATA/$PARENT/$x" "$LDATA/$PARENT/" || ok=0
    done
    for x in ${ALSO[@]+"${ALSO[@]}"}; do
        mkdir -p "$LDATA/$x" && python3 "$REPO/scripts/sync/tunnel.py" rsync -rlt --partial --update "$SSH:$RDATA/$x/" "$LDATA/$x/" || ok=0
    done
    (( ok )) && python3 "$REPO/scripts/sync/tunnel.py" rsync -rlt --partial --update ${EX[@]+"${EX[@]}"} ${EXD[@]+"${EXD[@]}"} "$SSH:$SRC/" "$DST/" \
        && python3 "$REPO/scripts/sync/tunnel.py" rsync -rlt --partial --update ${EX[@]+"${EX[@]}"} "$SSH:$SRC/" "$DST/" && break
    (( i == 3 )) && { echo "拉回失败 3 次" >&2; exit 1; }
    sleep "${FETCH_TEST_RETRY:-300}"
done
done_here || { echo "拉回以后 box 上还是没训完的标志（$TEST）" >&2; exit 1; }
echo "$(date '+%F %T') 拉回完成（$(du -sh "$DST" | cut -f1)）"
