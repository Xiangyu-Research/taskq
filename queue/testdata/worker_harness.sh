# Fake training and helpers shared by worker tests (worker_start.sh -> consume.py): create $T, set TASKQ_DIR, TASKQ_DATA and TASKQ_MACHINE, then source this
# WORKER_TEST_ONLY="<cases...>": run only these cases (want); all when unset
export TASKQ_POLL=1 TASKQ_IDLE_MAX=2 TASKQ_PATIENCE=0 TASKQ_MIN_LEFT=0 TASKQ_ADD_AFTER=0 TASKQ_ERR_MAX=2
# Workers read how many trainings they run at once (slots) from the machine table: slots_cfg N prints a config dir, the
# fixture config with every site-pueue worker and every site-slurm / site-slurm2 jobs entry at slots=N (made once per N);
# FIXTURE_CFG is the config in use without it (the fixture, or a bundle's packaged copy; no slots set: default 1). Cases set TASKQ_CONFIG=$(slots_cfg N) for N.
FIXTURE_CFG=${TASKQ_CONFIG:-$(cd "$HERE" && python3 -c "import cfg; print(cfg.config_dir())")} || exit 1   # in a bundle: its packaged config
if [[ -z ${TASKQ_CONFIG:-} ]]; then IN_BUNDLE=1; else IN_BUNDLE=; fi   # test_relay_bundle runs a few cases inside a bundle without TASKQ_CONFIG
slots_cfg() {  # slots_cfg <slots> [<cpus of site-pueue's CPU worker>]; made in a temp dir and moved in whole (cases started in the background may ask at the same time)
    local d=$T/slots_cfg$1_${2:-} t
    if [[ $1 == 1 && -z ${2:-} && $IN_BUNDLE == 1 ]]; then  # the packaged table sets no slots: leave the config for the worker to find (.taskq.toml)
        echo ""; return
    fi
    if [[ ! -d $d ]]; then
        t=$(mktemp -d "$T/slots_cfg.XXXXXX") && cp -r "$FIXTURE_CFG/." "$t/"
        printf 'import importlib.util as _u
_s = _u.spec_from_file_location("_fx_machines", %s)
_m = _u.module_from_spec(_s)
_s.loader.exec_module(_m)
globals().update({k: v for k, v in vars(_m).items() if not k.startswith("__")})
for _w in MACHINES["site-pueue"]["workers"]:
    _w["slots"] = %d
    if _w.get("cpus") and %s:
        _w["cpus"] = %s
for _n in ("site-slurm", "site-slurm2"):
    for _j in MACHINES[_n]["jobs"].values():
        _j["slots"] = %d
' "'$FIXTURE_CFG/machines.py'" "$1" "${2:-0}" "${2:-0}" "$1" > "$t/machines.py"
        mv -T "$t" "$d" 2> /dev/null || rm -rf "$t"
    fi
    echo "$d"
}
# with the fixture config: cases run at 2 by default; in a bundle (no TASKQ_CONFIG) the worker finds the packaged config
# itself (.taskq.toml), as on an executor, and runs at its default 1
if [[ -z $IN_BUNDLE ]]; then export TASKQ_CONFIG=$(slots_cfg 2); fi
export WORKER_TEST_GPU="50 10 20000 81920"   # 50% utilisation over the last 10 minutes, training uses 20GB
export TASKQ_GPU_PAD_BASE=$T/nopad TMPDIR=$T/tmp   # keep the GPU pad and local copies out of /tmp job paths
export TASKQ_GPU_PAD=0   # worker_start.sh starts no real GPU pad (the handshake is simulated by fake files under TASKQ_GPU_PAD_BASE)
mkdir -p "$TASKQ_DIR" "$TASKQ_DATA" "$TMPDIR"
touch "$TASKQ_DIR/ENABLED"
cat > "$T/fake_train.sh" << 'EOT'
#!/bin/bash
# fake training: argument is the task name; record start, end and env, sleep ${FAKE_SECS:-1} seconds, then write the done file
out=$TASKQ_DATA/runs/$1
mkdir -p "$out"
# install the stop handler before recording start (a test may send a stop as soon as it sees start)
if [[ ${FAKE_IGNORE:-} == "$1" ]]; then  # ignore stop signals
    trap 'echo "term $1" >> "$FAKE_LOG"' TERM
else
    trap 'echo "term $1" >> "$FAKE_LOG"; exit 143' TERM
fi
echo "start $(date +%s.%N) $1 ${DEMO_ACTIVE_VENV:-none} ${CONDA_DEFAULT_ENV:-none} cpus=${TASK_CPUS:-}" >> "$FAKE_LOG"
[[ ${FAKE_IGNORE:-} == "$1" ]] && while :; do sleep 1; done
if [[ ${FAKE_ESCAPE:-} == "$1" ]]; then  # leave a process that will not stop: same session, another process group, ignores SIGTERM, exits after FAKE_ESCAPE_SECS
    python3 -c 'import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
if os.fork():
    os._exit(0)
os.setpgid(0, 0)
open(sys.argv[1], "w").write(str(os.getpid()))
time.sleep(float(sys.argv[2]))' "$FAKE_LOG.escape" "${FAKE_ESCAPE_SECS:-20}"
    sleep 120 & wait $!
elif [[ ${FAKE_BUSY:-} == "$1" ]]; then  # keep writing the output dir without logging (like probability sparsification)
    for _ in $(seq "${FAKE_BUSY_SECS:-10}"); do touch "$out/progress"; sleep 1; done
elif [[ ${FAKE_HANG:-} == "$1" ]]; then
    sleep 120 & wait $!
elif [[ ${FAKE_EAGER:-} == "$1" ]]; then  # write the done file as soon as the child (sleep) ends, however it ended; next to it a 30-level process chain:
    # stopping the tree from the leaves hits sleep first and the training itself only later (only stopping the whole group avoids the done file)
    sleep 60 & c=$!
    bash "$FAKE_CHAIN" 30 &
    wait $c
    : > "$out/DONE"
    echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
    exit 0
else
    sleep "${FAKE_SECS:-1}" & wait $!
fi || { echo "term $1" >> "$FAKE_LOG"; exit 143; }  # the child stopped first (the tree stops from the leaves): treat as a stop, write no done file
[[ " ${FAKE_FAIL:-} " == *" $1 "* ]] && { echo "fail $1" >> "$FAKE_LOG"; exit 1; }  # FAKE_FAIL: space-separated task names
touch "$out/DONE"
echo "end $(date +%s.%N) $1" >> "$FAKE_LOG"
EOT
chmod +x "$T/fake_train.sh"
cat > "$T/chain.sh" << 'EOT'
#!/bin/bash
# a process chain $1 levels deep; the leaf writes chain.ready then sleeps 60 seconds (for FAKE_EAGER)
if (( $1 > 0 )); then bash "$0" $(( $1 - 1 )); else touch "$(dirname "$0")/chain.ready"; sleep 60; fi
EOT
export FAKE_CHAIN=$T/chain.sh
export FAKE_LOG=$T/train.log
fails=0
check() { if eval "$2"; then echo "PASS:$1"; else echo "FAIL:$1"; fails=$((fails + 1)); fi; }
waitfor() { local i; for i in $(seq "$1"); do eval "$2" && return 0; sleep 1; done; return 1; }
want() { [[ -z ${WORKER_TEST_ONLY:-} || " $WORKER_TEST_ONLY " == *" $1 "* ]]; }
table() {  # each line: task name env [device]
    : > "$TASKQ_DIR/tasks.jsonl"
    local i=0 name env dev
    while read -r name env dev; do
        i=$((i + 1))
        python3 - "$TASKQ_DIR/tasks.jsonl" "$i" "$name" "$env" "$T/fake_train.sh" "${dev:-gpu}" << 'EOT'
import json, sys
p, i, name, env, fake, dev = sys.argv[1:]
t = dict(order=int(i), nice=0, key=name, exp=None, recipe="command",
         params=dict(key=name, cmd=f"bash {fake} {name}", env=env, out=f"runs/{name}", done=["DONE"], device=dev))
open(p, "a").write(json.dumps(t) + "\n")
EOT
    done
}
reset() { : > "$T/train.log"; rm -f "$T/chain.ready"; rm -rf "$TASKQ_DATA/runs" "$TASKQ_DIR/claims" "$TASKQ_DIR/failures" "$TASKQ_DIR/released" "$TASKQ_DIR/stopped"; }
