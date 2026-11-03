#!/bin/bash
# archive_task.sh 的测试（2026-11-03）：box、训练机、备份机都是临时目录，假 ssh 在本机跑，机器表里不走隧道，不连网。
#   bash scripts/queue/test_archive_task.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'chmod -R u+w "$T" 2> /dev/null; rm -rf "$T"' EXIT
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
MACHINES["box"].update(hostname=re.escape(socket.gethostname().split(".")[0]), data={t + '/box'!r})
MACHINES["lab"].update(ssh="t-src", data={t + '/lab'!r}, tunnel=())
MACHINES["dicc"].update(ssh="t-backup", data={t + '/bk'!r}, tunnel=())
"""
mark = 'if __name__ == "__main__":'
assert mark in s
open(p, "w").write(s.replace(mark, inj + "\n" + mark))
EOF
cat > "$T/bin/ssh" << 'EOF'
#!/bin/bash
while [[ ${1:-} == -* ]]; do case $1 in -o|-p|-i|-l) shift 2 ;; *) shift ;; esac; done
case ${1:-} in t-src|t-backup) ;; *) echo "假 ssh：不认识的机器 ${1:-}" >&2; exit 255 ;; esac
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
export TASKQ_CONFIG=$T/config HOME=$T/home PATH=$T/bin:$PATH TASKQ_DATA=$T/box TASKQ_STATE=$T/state TASKQ_DIR=$T/taskq \
       TUNNEL_URL=http://127.0.0.1:9 TUNNEL_SSH=$T/bin/ssh

KEY=D101_alpha/V_save/0 OUT=results/D101_alpha/V_save__PlanM__full/frag_0 SAFE=D101_alpha__V_save__0
GOOD=$(head -c 20000 /dev/urandom | base64)
# 每个场景重新搭三处：box、训练机内容一样（训练机多一个不拉回的 latest.ckpt），备份机是空的
world() {
    rm -rf "$T/box" "$T/lab" "$T/bk" "$T/home/archive_prune.log" "$T/fail"
    local d
    for d in box lab; do
        mkdir -p "$T/$d/$OUT/check" "$T/$d/logs/tasks"
        echo '{"a": 1}' > "$T/$d/$OUT/check/summary.json"
        echo val > "$T/$d/$OUT/check/val.dat"
        echo "$GOOD" > "$T/$d/$OUT/final.ckpt"
        echo log > "$T/$d/$OUT/train_log_1.txt"
        echo plans > "$T/$d/$(dirname "$OUT")/plans.json"
        touch -d '2026-11-01 10:00' "$T/$d/$OUT/final.ckpt" "$T/$d/$OUT/check/val.dat"
    done
    echo seg > "$T/lab/logs/tasks/${SAFE}_lab-1_part1.log"
    echo latest > "$T/lab/$OUT/latest.ckpt"
    mkdir -p "$T/bk"
}
half() {  # 备份机上 final.ckpt 只有半份（中断的传输留下的），别的都在
    mkdir -p "$T/bk/$OUT"
    cp -a "$T/box/$OUT/." "$T/bk/$OUT/"
    head -c 1000 "$T/box/$OUT/final.ckpt" > "$T/bk/$OUT/final.ckpt"
    touch -d '2026-11-02 10:00' "$T/bk/$OUT/final.ckpt"
}
A="bash $HERE/archive_task.sh lab $KEY"
kept() { [[ -f $T/lab/$OUT/final.ckpt && -f $T/lab/$OUT/check/val.dat && ! -e $T/lab/$OUT/ARCHIVED ]]; }

world
$A --dry-run > "$T/o0" 2>&1; rc=$?
check "dry-run：退出码 0，什么都不删、不往备份机写" "(( rc == 0 )) && kept && [[ -z \$(ls -A '$T/bk') ]] && grep -q '会删 3 个文件' '$T/o0'"

world
$A > "$T/o1" 2>&1; rc=$?
check "正常：退出码 0，备份机和 box 一样" "(( rc == 0 )) && diff -r '$T/box/$OUT' '$T/bk/$OUT' > /dev/null && cmp -s '$T/box/logs/tasks/${SAFE}_lab-1_part1.log' '$T/bk/logs/tasks/${SAFE}_lab-1_part1.log' && [[ -f $T/bk/results/D101_alpha/V_save__PlanM__full/plans.json ]]"
check "正常：删了训练机上 drop 的文件、写 ARCHIVED，留下 keep 的" "[[ ! -e $T/lab/$OUT/final.ckpt && ! -e $T/lab/$OUT/latest.ckpt && ! -e $T/lab/$OUT/check/val.dat && -f $T/lab/$OUT/check/summary.json && -f $T/lab/$OUT/train_log_1.txt && -f $T/lab/$OUT/ARCHIVED ]] && grep -q '3 个文件' '$T/home/archive_prune.log'"

world; half
$A > "$T/o2" 2>&1; rc=$?
check "备份机上有半份同名文件：补齐成完整的再删" "(( rc == 0 )) && cmp -s '$T/box/$OUT/final.ckpt' '$T/bk/$OUT/final.ckpt' && [[ -f $T/lab/$OUT/ARCHIVED ]]"

world; half; echo error > "$T/fail"
$A > "$T/o3" 2>&1; rc=$?
check "半份同名文件补传失败：退出非 0、不删训练机上的、没有 ARCHIVED" "(( rc != 0 )) && kept"

world; half; rm "$T/bk/$OUT/check/val.dat"; echo silent > "$T/fail"
$A > "$T/o4" 2>&1; rc=$?
check "备份机缺文件、有半份文件，补传没报错却没传上：核对查出来，退出 1、不删、没有 ARCHIVED" "(( rc == 1 )) && kept && grep -q '不一致' '$T/o4' && grep -q 'val.dat' '$T/o4' && grep -q 'final.ckpt' '$T/o4'"

world
echo changed > "$T/box/$OUT/check/val.dat"; touch -d '2026-11-01 10:00' "$T/box/$OUT/check/val.dat"
$A > "$T/o5" 2>&1; rc=$?
check "box 的副本和训练机内容不一样：退出 1、不删" "(( rc == 1 )) && kept && grep -q '按校验和' '$T/o5'"

world; half
bash "$HERE/archive_task.sh" box "$KEY" > "$T/o6" 2>&1; rc=$?
check "机器是 box 本身：半份补齐，核对一致，退出 0" "(( rc == 0 )) && cmp -s '$T/box/$OUT/final.ckpt' '$T/bk/$OUT/final.ckpt'"
world; half; echo silent > "$T/fail"
bash "$HERE/archive_task.sh" box "$KEY" > "$T/o7" 2>&1; rc=$?
check "机器是 box 本身：没传上 → 核对查出来，退出 1" "(( rc == 1 )) && grep -q '不一致' '$T/o7'"

(( ok )) && echo "全部通过" || { echo "有失败"; exit 1; }
