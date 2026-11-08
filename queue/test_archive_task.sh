#!/bin/bash
# Tests for archive_task.sh: the store (data dir TASKQ_DATA) and backup machine are temp dirs, fake ssh runs locally, the
# machine table has no tunnel, nothing goes online.
#   bash queue/test_archive_task.sh
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'chmod -R u+w "${T:?}" 2> /dev/null; rm -rf "${T:?}"' EXIT
ok=1
check() { if eval "$2"; then echo "PASS:$1"; else echo "FAIL:$1"; ok=0; fi; }

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
case ${1:-} in t-backup) ;; *) echo "fake ssh: unknown machine ${1:-}" >&2; exit 255 ;; esac
shift
cd "$HOME" && exec bash -c "$*"
EOF
# fake rsync: $T/fail holds error (transfer to the backup broke) or silent (claims done but transfers nothing); checks (-n) work as usual
cat > "$T/bin/rsync" << EOF
#!/bin/bash
mode=\$(cat "$T/fail" 2> /dev/null)
dry=0
for a in "\$@"; do [[ \$a == --dry-run || \$a =~ ^-[a-zA-Z]*n[a-zA-Z]*\$ ]] && dry=1; done
if [[ -n \$mode && \${@: -1} == t-backup:* && \$dry == 0 ]]; then
    [[ \$mode == silent ]] && exit 0
    echo "fake rsync: transfer broke halfway" >&2; exit 12
fi
exec $(command -v rsync) "\$@"
EOF
chmod +x "$T/bin/ssh" "$T/bin/rsync"
export TASKQ_CONFIG=$T/config HOME=$T/home PATH=$T/bin:$PATH TASKQ_DATA=$T/hub TASKQ_STATE=$T/state TASKQ_DIR=$T/taskq \
       TUNNEL_URL=http://127.0.0.1:9 TUNNEL_SSH=$T/bin/ssh

KEY=D101_alpha/V_save/0 OUT=results/D101_alpha/V_save__PlanM__full/frag_0 SAFE=D101_alpha__V_save__0
GOOD=$(head -c 20000 /dev/urandom | base64)
# archive spec: the one the hub resolved and sent to the store through site_ops archive (spec.sendable)
SPEC=$(cd "$HERE" && python3 -c 'import sys, json; sys.path.insert(0, "."); import spec as S; print(json.dumps(S.sendable(S.lookup(sys.argv[1]))))' "$KEY")
NOARC=$(python3 -c 'import sys, json; s = json.loads(sys.argv[1]); s["archive"] = False; print(json.dumps(s))' "$SPEC")
# set up each scenario again: a task trained and fetched on the store, backup machine empty
world() {
    rm -rf "$T/hub" "$T/bk" "$T/fail"
    mkdir -p "$T/hub/$OUT/check" "$T/hub/logs/tasks" "$T/bk"
    echo '{"a": 1}' > "$T/hub/$OUT/check/summary.json"
    echo val > "$T/hub/$OUT/check/val.dat"
    echo "$GOOD" > "$T/hub/$OUT/final.ckpt"
    echo log > "$T/hub/$OUT/train_log_1.txt"
    echo plans > "$T/hub/$(dirname "$OUT")/plans.json"
    echo seg > "$T/hub/logs/tasks/${SAFE}__w-1_part1.log"
    echo other > "$T/hub/logs/tasks/${SAFE}__x__w-1_part1.log"  # another task with a longer key (D101_alpha/V_save/0/x)
    touch -d '2026-11-01 10:00' "$T/hub/$OUT/final.ckpt" "$T/hub/$OUT/check/val.dat"
}
half() {  # final.ckpt is only half there on the backup (left by an interrupted transfer), everything else is there
    mkdir -p "$T/bk/$OUT"
    cp -a "$T/hub/$OUT/." "$T/bk/$OUT/"
    head -c 1000 "$T/hub/$OUT/final.ckpt" > "$T/bk/$OUT/final.ckpt"
    touch -d '2026-11-02 10:00' "$T/bk/$OUT/final.ckpt"
}
A() { bash "$HERE/archive_task.sh" --spec "$SPEC" "$@"; }

world
A --dry-run > "$T/o0" 2>&1; rc=$?
check "dry-run: exit 0, nothing written to the backup" "(( rc == 0 )) && [[ -z \$(ls -A '$T/bk') ]]"

world
A > "$T/o1" 2>&1; rc=$?
check "normal: exit 0, backup matches the store; a longer-key task's part log is not included" "(( rc == 0 )) && diff -r '$T/hub/$OUT' '$T/bk/$OUT' > /dev/null && cmp -s '$T/hub/logs/tasks/${SAFE}__w-1_part1.log' '$T/bk/logs/tasks/${SAFE}__w-1_part1.log' && [[ -f $T/bk/results/D101_alpha/V_save__PlanM__full/plans.json && ! -e $T/bk/logs/tasks/${SAFE}__x__w-1_part1.log ]]"

world
bash "$HERE/archive_task.sh" hub "$KEY" > "$T/o2" 2>&1; rc=$?
check "usage other than --spec: exit 2, nothing written to the backup" "(( rc == 2 )) && grep -q usage '$T/o2' && [[ -z \$(ls -A '$T/bk') ]]"
bash "$HERE/archive_task.sh" --spec "$NOARC" > "$T/o2" 2>&1; rc=$?
check "archive off in the spec: exit 2, nothing written to the backup" "(( rc == 2 )) && [[ -z \$(ls -A '$T/bk') ]]"
bash "$HERE/archive_task.sh" --spec '{"key": 1' > "$T/o2" 2>&1; rc=$?
check "spec is not JSON: exit 2" "(( rc == 2 )) && grep -q 'bad spec' '$T/o2'"
rm "$T/hub/$OUT/final.ckpt"
A > "$T/o2" 2>&1; rc=$?
check "not fully fetched on the store (missing a done file): exit 1, nothing written to the backup" "(( rc == 1 )) && [[ -z \$(ls -A '$T/bk') ]]"

world; half
A > "$T/o3" 2>&1; rc=$?
check "half a file with the same name on the backup: completed and verified, exit 0" "(( rc == 0 )) && cmp -s '$T/hub/$OUT/final.ckpt' '$T/bk/$OUT/final.ckpt'"

world; half; echo error > "$T/fail"
A > "$T/o4" 2>&1; rc=$?
check "completing a half file fails: exit non-zero" "(( rc != 0 ))"

world; half; rm "$T/bk/$OUT/check/val.dat"; echo silent > "$T/fail"
A > "$T/o5" 2>&1; rc=$?
check "backup missing a file and holding a half file, transfer silently did nothing: verify catches it, exit 1" "(( rc == 1 )) && grep -q 'differs' '$T/o5' && grep -q 'val.dat' '$T/o5' && grep -q 'final.ckpt' '$T/o5'"

# run through site_ops.py archive on the store (the hub's relay.py archive uses this): the data dir is the data argument, not the outer TASKQ_DATA
world
OPIN=$(python3 -c 'import sys, json; print(json.dumps(dict(spec=json.loads(sys.argv[1]), data=sys.argv[2])))' "$SPEC" "$T/hub")
OPOUT=$(TASKQ_DATA=$T/nosuch python3 "$HERE/site_ops.py" archive <<< "$OPIN" 2> "$T/o6"); rc=$?
check "site_ops.py archive: backups and verifies, one @@JSON true line" "(( rc == 0 )) && [[ \$OPOUT == '@@JSON true' ]] && diff -r '$T/hub/$OUT' '$T/bk/$OUT' > /dev/null"
echo error > "$T/fail"; rm -rf "$T/bk"; mkdir -p "$T/bk"
TASKQ_DATA=$T/nosuch python3 "$HERE/site_ops.py" archive <<< "$OPIN" > "$T/o7" 2>&1; rc=$?
check "site_ops.py archive: transfer fails -> non-zero exit, says archive failed" "(( rc != 0 )) && grep -q 'archive failed' '$T/o7'"

(( ok )) && echo "all pass" || { echo "failed"; exit 1; }
