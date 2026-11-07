#!/bin/bash
# archive_task.sh 的测试：store（数据目录 TASKQ_DATA）、备份机都是临时目录，假 ssh 在本机跑，机器表里不走隧道，不连网。
#   bash queue/test_archive_task.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'chmod -R u+w "${T:?}" 2> /dev/null; rm -rf "${T:?}"' EXIT
ok=1
check() { if eval "$2"; then echo "通过：$1"; else echo "失败：$1"; ok=0; fi; }

mkdir -p "$T/config" "$T/bin" "$T/home"
cp -r "$TASKQ_CONFIG/." "$T/config/"
python3 - "$T" << 'EOF'
import sys
t = sys.argv[1]
p = f"{t}/config/machines.py"
s = open(p).read()
inj = f"""
import socket
MACHINES["hub"].update(hostname=re.escape(socket.gethostname().split(".")[0]), data={t + '/hub'!r})
MACHINES["site-slurm"].update(ssh="t-backup", backup={t + '/bk'!r}, tunnel=())
"""
open(p, "w").write(s + inj)
EOF
cat > "$T/bin/ssh" << 'EOF'
#!/bin/bash
while [[ ${1:-} == -* ]]; do case $1 in -o|-p|-i|-l) shift 2 ;; *) shift ;; esac; done
case ${1:-} in t-backup) ;; *) echo "假 ssh：不认识的机器 ${1:-}" >&2; exit 255 ;; esac
shift
cd "$HOME" && exec bash -c "$*"
EOF
# 假 rsync：$T/fail 里写 error（往备份机传时断了）或 silent（说传完了其实没传）；核对（-n）照常
cat > "$T/bin/rsync" << EOF
#!/bin/bash
mode=\$(cat "$T/fail" 2> /dev/null)
dry=0
for a in "\$@"; do [[ \$a == --dry-run || \$a =~ ^-[a-zA-Z]*n[a-zA-Z]*\$ ]] && dry=1; done
if [[ -n \$mode && \${@: -1} == t-backup:* && \$dry == 0 ]]; then
    [[ \$mode == silent ]] && exit 0
    echo "假 rsync：传到一半断了" >&2; exit 12
fi
exec $(command -v rsync) "\$@"
EOF
chmod +x "$T/bin/ssh" "$T/bin/rsync"
export TASKQ_CONFIG=$T/config HOME=$T/home PATH=$T/bin:$PATH TASKQ_DATA=$T/hub TASKQ_STATE=$T/state TASKQ_DIR=$T/taskq \
       TUNNEL_URL=http://127.0.0.1:9 TUNNEL_SSH=$T/bin/ssh

KEY=D101_alpha/V_save/0 OUT=results/D101_alpha/V_save__PlanM__full/frag_0 SAFE=D101_alpha__V_save__0
GOOD=$(head -c 20000 /dev/urandom | base64)
# 归档的说明：hub 解析好、经 site_ops 的 archive 送到 store 的那份（spec.sendable）
SPEC=$(cd "$HERE" && python3 -c 'import sys, json; sys.path.insert(0, "."); import spec as S; print(json.dumps(S.sendable(S.lookup(sys.argv[1]))))' "$KEY")
NOARC=$(python3 -c 'import sys, json; s = json.loads(sys.argv[1]); s["archive"] = False; print(json.dumps(s))' "$SPEC")
# 每个场景重新搭：store 上训完拉回的任务，备份机是空的
world() {
    rm -rf "$T/hub" "$T/bk" "$T/fail"
    mkdir -p "$T/hub/$OUT/check" "$T/hub/logs/tasks" "$T/bk"
    echo '{"a": 1}' > "$T/hub/$OUT/check/summary.json"
    echo val > "$T/hub/$OUT/check/val.dat"
    echo "$GOOD" > "$T/hub/$OUT/final.ckpt"
    echo log > "$T/hub/$OUT/train_log_1.txt"
    echo plans > "$T/hub/$(dirname "$OUT")/plans.json"
    echo seg > "$T/hub/logs/tasks/${SAFE}__w-1_part1.log"
    echo other > "$T/hub/logs/tasks/${SAFE}__x__w-1_part1.log"  # key 更长的别的任务（D101_alpha/V_save/0/x）
    touch -d '2026-11-01 10:00' "$T/hub/$OUT/final.ckpt" "$T/hub/$OUT/check/val.dat"
}
half() {  # 备份机上 final.ckpt 只有半份（中断的传输留下的），别的都在
    mkdir -p "$T/bk/$OUT"
    cp -a "$T/hub/$OUT/." "$T/bk/$OUT/"
    head -c 1000 "$T/hub/$OUT/final.ckpt" > "$T/bk/$OUT/final.ckpt"
    touch -d '2026-11-02 10:00' "$T/bk/$OUT/final.ckpt"
}
A() { bash "$HERE/archive_task.sh" --spec "$SPEC" "$@"; }

world
A --dry-run > "$T/o0" 2>&1; rc=$?
check "dry-run：退出码 0，不往备份机写" "(( rc == 0 )) && [[ -z \$(ls -A '$T/bk') ]]"

world
A > "$T/o1" 2>&1; rc=$?
check "正常：退出码 0，备份机和 store 一样；key 更长的别的任务的分段日志不带上" "(( rc == 0 )) && diff -r '$T/hub/$OUT' '$T/bk/$OUT' > /dev/null && cmp -s '$T/hub/logs/tasks/${SAFE}__w-1_part1.log' '$T/bk/logs/tasks/${SAFE}__w-1_part1.log' && [[ -f $T/bk/results/D101_alpha/V_save__PlanM__full/plans.json && ! -e $T/bk/logs/tasks/${SAFE}__x__w-1_part1.log ]]"

world
bash "$HERE/archive_task.sh" hub "$KEY" > "$T/o2" 2>&1; rc=$?
check "不是 --spec 的用法：退出 2，不往备份机写" "(( rc == 2 )) && grep -q 用法 '$T/o2' && [[ -z \$(ls -A '$T/bk') ]]"
bash "$HERE/archive_task.sh" --spec "$NOARC" > "$T/o2" 2>&1; rc=$?
check "说明里不归档（archive 关）：退出 2，不往备份机写" "(( rc == 2 )) && [[ -z \$(ls -A '$T/bk') ]]"
bash "$HERE/archive_task.sh" --spec '{"key": 1' > "$T/o2" 2>&1; rc=$?
check "说明不是 JSON：退出 2" "(( rc == 2 )) && grep -q 说明不对 '$T/o2'"
rm "$T/hub/$OUT/final.ckpt"
A > "$T/o2" 2>&1; rc=$?
check "store 上还没拉回完整（缺 done 的文件）：退出 1，不往备份机写" "(( rc == 1 )) && [[ -z \$(ls -A '$T/bk') ]]"

world; half
A > "$T/o3" 2>&1; rc=$?
check "备份机上有半份同名文件：补齐，核对一致，退出 0" "(( rc == 0 )) && cmp -s '$T/hub/$OUT/final.ckpt' '$T/bk/$OUT/final.ckpt'"

world; half; echo error > "$T/fail"
A > "$T/o4" 2>&1; rc=$?
check "半份同名文件补传失败：退出非 0" "(( rc != 0 ))"

world; half; rm "$T/bk/$OUT/check/val.dat"; echo silent > "$T/fail"
A > "$T/o5" 2>&1; rc=$?
check "备份机缺文件、有半份文件，补传没报错却没传上：核对查出来，退出 1" "(( rc == 1 )) && grep -q '不一致' '$T/o5' && grep -q 'val.dat' '$T/o5' && grep -q 'final.ckpt' '$T/o5'"

# store 上经 site_ops.py 的 archive 跑（hub 的 relay.py archive 走这条）：数据目录是参数里的 data，不看外面的 TASKQ_DATA
world
OPIN=$(python3 -c 'import sys, json; print(json.dumps(dict(spec=json.loads(sys.argv[1]), data=sys.argv[2])))' "$SPEC" "$T/hub")
OPOUT=$(TASKQ_DATA=$T/nosuch python3 "$HERE/site_ops.py" archive <<< "$OPIN" 2> "$T/o6"); rc=$?
check "site_ops.py archive：备份、核对，结果一行 @@JSON true" "(( rc == 0 )) && [[ \$OPOUT == '@@JSON true' ]] && diff -r '$T/hub/$OUT' '$T/bk/$OUT' > /dev/null"
echo error > "$T/fail"; rm -rf "$T/bk"; mkdir -p "$T/bk"
TASKQ_DATA=$T/nosuch python3 "$HERE/site_ops.py" archive <<< "$OPIN" > "$T/o7" 2>&1; rc=$?
check "site_ops.py archive：传输失败 → 退出非 0、说归档没成" "(( rc != 0 )) && grep -q 归档没成 '$T/o7'"

(( ok )) && echo "全部通过" || { echo "有失败"; exit 1; }
