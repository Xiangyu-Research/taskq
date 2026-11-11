#!/bin/bash
# consume.py（消费者）和插件的模拟测试（11/5）：换代码包时接管在跑的任务、旧 bash worker 换过来、时段、等卡空、Slurm、使用者的插件
#   bash queue/test_consume.sh        WORKER_TEST_ONLY="h old" 只跑这几个情况
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CORE=$(cd "$HERE/.." && pwd)
source "$HERE/testdata/fixture.sh"
T=$(mktemp -d)
trap 'rm -rf "${T:?}"' EXIT
export TASKQ_DIR=$T/tq TASKQ_DATA=$T/data TASKQ_MACHINE=slurm-login
source "$HERE/testdata/worker_harness.sh"
unset TASKQ_GPU_PAD_BASE   # 常驻的 worker 没有显存填充
bundle() {  # 代码包 a、b（完整的核心）；b 的 worker_start.sh 换成只打一行再转交（不碰 conda）
    rm -rf "$T/code"; mkdir -p "$T/code/a/scripts/taskq" "$T/code/b/scripts/taskq"
    cp -r "$CORE/." "$T/code/a/scripts/taskq/"; cp -r "$CORE/." "$T/code/b/scripts/taskq/"
    printf '#!/bin/bash\necho "新包的 start"\nexec bash "$(dirname "$0")/dicc_worker.sh"\n' > "$T/code/b/scripts/taskq/queue/worker_start.sh"
    ln -sfn a "$T/code/current"
}
nclaims() { ls "$TASKQ_DIR/claims" 2> /dev/null | grep -vc '\.'; }

# h. 换代码包时手上有在训的：exec 新包，新进程接着管，训完、失败的退出码都在新进程里收到并交还
if want h; then
reset
table << EOF
t/h base
t/f base
EOF
bundle
(WORKER_ID=pueue-host-h TASKQ_SLOTS=3 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 FAKE_SECS=8 FAKE_FAIL=t/f \
  exec bash "$T/code/a/scripts/taskq/queue/dicc_worker.sh") > "$T/wh.out" 2>&1 &
wpid=$!
waitfor 30 '[[ $(grep -c "开训" $T/wh.out) == 2 ]]'
ln -sfn b "$T/code/current"
waitfor 30 'grep -q "接着管 t/f" $T/wh.out'
check "换包：在训的两个交给新进程" 'grep -q "换成新代码 b 接着跑（在训的 2 个交给新代码接着管）" $T/wh.out && grep -q "接着管 t/h" $T/wh.out && [[ $(nclaims) == 2 ]] && kill -0 $wpid'
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "换包：训完的、失败的退出码在新进程里收到，交还" '[[ $rc == 0 ]] && sed -n "/新包的 start/,\$p" $T/wh.out | grep -q "训完 t/h" && sed -n "/新包的 start/,\$p" $T/wh.out | grep -q "t/f 训练失败（run_task.sh 退出码 1）" && [[ $(nclaims) == 0 && -e $TASKQ_DIR/failures/t__f ]]'
check "换包：新进程记的代码是新包" 'grep -q "，代码 b$" $T/wh.out'
fi

# retry. 失败交还后同一个 worker 隔 FAIL_RETRY 再领，第二次失败后队列记为 failed、不再领（11/11：lab1 只有一个常驻 worker，
#   别人占卡导致推理失败一次后，原来永久不领，任务挂了 5 天）
if want retry; then
reset
table << EOF
t/r base
EOF
(WORKER_ID=pueue-host-r TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=6 TASKQ_EARLY_FAIL=0 TASKQ_FAIL_RETRY=2 FAKE_FAIL=t/r \
  exec bash "$HERE/dicc_worker.sh") > "$T/wr.out" 2>&1 &
wpid=$!
waitfor 60 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "失败后隔一会儿再领，第二次失败后不再领" '[[ $rc == 0 && $(grep -c "^fail t/r$" $FAKE_LOG) == 2 && $(nclaims) == 0 ]] && grep -q "t/r 本作业 0 分钟后再领" $T/wr.out'
fi

# hs. 新包自检不过：留在旧包，接着领
if want hs; then
reset
table << EOF
t/s base
t/s2 base
EOF
bundle
printf 'raise RuntimeError("坏了")\n' > "$T/code/b/scripts/taskq/queue/consumers/slurm.py"
(WORKER_ID=pueue-host-s TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 FAKE_SECS=3 \
  exec bash "$T/code/a/scripts/taskq/queue/dicc_worker.sh") > "$T/whs.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "开训 t/s（" $T/whs.out'
ln -sfn b "$T/code/current"
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "自检不过：记一句，留在旧包接着领、训" '[[ $rc == 0 ]] && grep -q "新代码包 b 自检没过（.*坏了），留在 a" $T/whs.out && grep -q "训完 t/s2" $T/whs.out && ! grep -q "新包的 start" $T/whs.out'
fi

# hi. 空着时换代码包：空闲的起点交给新进程，总共空满 IDLE_EXIT 就退出，不从换包时重新算（11/11：box 的 CPU worker
#   每半小时换一次包、一直不退出，占着 cpu 组唯一的位置）
if want hi; then
reset
table < /dev/null
bundle
t0=$(date +%s)
(WORKER_ID=pueue-host-i TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=14 \
  exec bash "$T/code/a/scripts/taskq/queue/dicc_worker.sh") > "$T/whi.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "没有能领的任务" $T/whi.out'
waitfor 10 '(( $(date +%s) - t0 >= 7 ))'
ln -sfn b "$T/code/current"
waitfor 20 'grep -q "新包的 start" $T/whi.out'
t1=$(date +%s)
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
t2=$(date +%s)
check "空着换包：空闲接着算，总共满 IDLE_EXIT 退出" '[[ $rc == 0 ]] && grep -q "接着算空闲：已经空了" $T/whi.out && sed -n "/新包的 start/,\$p" $T/whi.out | grep -q "退出（有备好的任务时下次检查再开）" && (( t2 - t1 < 11 && t2 - t0 < 22 ))'
fi

# old. 11/5 前的 bash worker 带着停不下来的训练（TASKQ_ZOMBIES）exec 到新包：consume.py 照样认
if want old; then
reset
table << EOF
t/z base
EOF
bundle
git -C "$CORE" show 5097cdf:queue/dicc_worker.sh > "$T/code/a/scripts/taskq/queue/dicc_worker.sh" 2> /dev/null || { echo "跳过：没有 5097cdf"; exit 77; }
mkdir -p "$T/binz"; printf '#!/bin/bash\nexit 0\n' > "$T/binz/pkill"; chmod +x "$T/binz/pkill"
(WORKER_ID=pueue-host-z TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 TASKQ_STALL_MAX=4 TASKQ_GIVEUP=3 TASKQ_BEAT=1 \
  FAKE_ESCAPE=t/z FAKE_ESCAPE_SECS=25 PATH=$T/binz:$PATH exec bash "$T/code/a/scripts/taskq/queue/dicc_worker.sh") > "$T/wold.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "还有停不下来的训练" $T/wold.out'
ln -sfn b "$T/code/current"
waitfor 20 'grep -q "接着跟停不下来的 t/z" $T/wold.out'
check "旧 worker exec 过来：新的 consume.py 接着跟停不下来的" 'sed -n "/新包的 start/,\$p" $T/wold.out | grep -q "接着跟停不下来的 t/z" && [[ -e $TASKQ_DIR/claims/t__z ]]'
waitfor 40 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "旧 worker exec 过来：停下了才交还、再退出" '[[ $rc == 0 ]] && grep -q "停不下来的 t/z 终于停下了，交还" $T/wold.out && [[ ! -e $TASKQ_DIR/claims/t__z ]]'
[[ -s $FAKE_LOG.escape ]] && kill "$(cat "$FAKE_LOG.escape")" 2> /dev/null
fi

# win. 时段（policy.SHIFT）：时段外不领、退出；时段里领；时段结束时停下手上的、交还、退出 0
if want win; then
reset
table << EOF
t/w base
EOF
mkdir -p "$T/wcfg"; cp -r "$TASKQ_CONFIG/." "$T/wcfg/"
# machines.py 按自己的位置找假的 conda：照原来的位置读
printf 'import importlib.util as _u
_s = _u.spec_from_file_location("_fx_machines", %s)
_m = _u.module_from_spec(_s)
_s.loader.exec_module(_m)
globals().update({k: v for k, v in vars(_m).items() if not k.startswith("__")})
' \
  "'$TASKQ_CONFIG/machines.py'" > "$T/wcfg/machines.py"
printf '\nSHIFT = {"site-pueue2": ("21:00", "09:00", None, None)}\n' >> "$T/wcfg/policy.py"
at() { python3 -c 'import datetime as dt, sys; tz = dt.timezone(dt.timedelta(hours=8)); d = dt.datetime.now(tz).date()
print(dt.datetime.combine(d, dt.time(*map(int, sys.argv[1].split(":"))), tz).timestamp() + 86400 * int(sys.argv[2]))' "$@"; }
at 12:00 0 > "$T/now"
(TASKQ_CONFIG=$T/wcfg TASKQ_TEST_NOW=$T/now WORKER_ID=wd2c85f TASKQ_IDLE_POLL=1 exec bash "$HERE/dicc_worker.sh") > "$T/win0.out" 2>&1
rc=$?
check "时段外开的（按 WORKER_ID 反查到 site-pueue2）：不领，退出 0" '[[ $rc == 0 ]] && grep -q "不在时段里（21:00–09:00），退出" $T/win0.out && [[ ! -s $T/train.log ]]'
at 22:00 0 > "$T/now"
(TASKQ_CONFIG=$T/wcfg TASKQ_TEST_NOW=$T/now TASKQ_SITE=site-pueue2 WORKER_ID=w-test TASKQ_IDLE_POLL=1 FAKE_SECS=60 \
  exec bash "$HERE/dicc_worker.sh") > "$T/win.out" 2>&1 &
wpid=$!
waitfor 30 'grep -q "^start .* t/w" $T/train.log'
at 09:01 1 > "$T/now"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "时段结束：停下手上的、交还、退出 0" '[[ $rc == 0 ]] && grep -q "时段结束（21:00–09:00），停训练、交还任务" $T/win.out && grep -q "^term t/w" $T/train.log && [[ $(nclaims) == 0 ]] && ! grep -q "^end .* t/w" $T/train.log'
fi

# idle. 等卡空（gpu_idle）：别人刚用过 / 正占着卡时不领（不退出），空了才领；run_task.sh 见到 TASKQ_ADMITTED=1 不再等卡
if want idle; then
reset
table << EOF
t/i base
EOF
mkdir -p "$T/bini" "$T/gpulog"
printf '#!/bin/bash\n[[ "$*" == *compute-apps* && -e %q ]] && echo "1, 900"\nexit 0\n' "$T/busy" > "$T/bini/nvidia-smi"; chmod +x "$T/bini/nvidia-smi"
f=$T/gpulog/$(date +gpu_%Y-%m.csv)
echo "$(date -d '-5 min' '+%F %T'),1,50,9000,24576,someone,4242,9000,python" > "$f"
(WORKER_ID=pueue-host-i CUDA_VISIBLE_DEVICES=1 TASK_GPU_IDLE=60 GPU_LOG_DIR=$T/gpulog TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 \
  PATH=$T/bini:$PATH exec bash "$HERE/dicc_worker.sh") > "$T/widle.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "卡 1 空着，别人 .* 还在用，空满 60 分钟再跑（已等 0 分钟）" $T/widle.out'
sleep 4
check "别人刚用过卡：不领、也不因为空闲退出" 'kill -0 $wpid && [[ ! -s $T/train.log && $(nclaims) == 0 ]]'
: > "$f"; touch "$T/busy"
waitfor 20 'grep -q "卡 1 被别人占着，等它空出来" $T/widle.out'
check "卡被别人的进程占着：不领" '[[ ! -s $T/train.log && $(nclaims) == 0 ]]'
rm -f "$T/busy"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "卡空了：领、训完" '[[ $rc == 0 ]] && grep -q "训完 t/i" $T/widle.out'
touch "$T/busy"
run() { TASK_JOBTAG=x TASK_KEY=t/r CUDA_VISIBLE_DEVICES=1 RUN_TASK_GPU_POLL=1 PATH=$T/bini:$PATH \
    timeout 8 bash "$HERE/run_task.sh" command key=t/r "cmd=bash $T/fake_train.sh t/r" env=base out=runs/t/r 'done=["DONE"]' device=gpu > "$T/rt.out" 2>&1; }
run; rc=$?
check "run_task.sh 手动跑：卡被占着就等" '[[ $rc == 124 ]] && grep -q "卡 1 被别人占着" $T/rt.out'
TASKQ_ADMITTED=1 run; rc=$?
check "run_task.sh 由消费者开（TASKQ_ADMITTED=1）：不再等卡" '[[ $rc == 0 ]] && ! grep -q "卡 1 被别人占着" $T/rt.out && grep -q "^end .* t/r" $T/train.log'
fi

# slurm. Slurm 作业：作业号当名字，按核数和内存定同时几个，时限查不到之后再查，本地暂存目录交给 run_task.sh，结束时删掉
if want slurm; then
reset
python3 - "$TASKQ_DIR/tasks.jsonl" "$T" << 'EOT'
import json, sys
p, t = sys.argv[1:]
cmd = f'echo "$TASKQ_STAGE_ROOT $TASKQ_ADMITTED" > {t}/stage; mkdir -p "$TASKQ_DATA/runs/t/s"; touch "$TASKQ_DATA/runs/t/s/DONE"'
open(p, "w").write(json.dumps(dict(order=1, nice=0, key="t/s", exp=None, recipe="command",
                                   params=dict(key="t/s", cmd=cmd, env="base", out="runs/t/s", done=["DONE"], device="gpu"))) + "\n")
EOT
eval "$(python3 "$HERE/proto.py" sh)"
mkdir -p "$TMPDIR/${TQ_STAGE_PREFIX}900300" "$T/bins"; printf '#!/bin/bash\nexit 1\n' > "$T/bins/squeue"; chmod +x "$T/bins/squeue"
(unset TASKQ_SLOTS; SLURM_JOB_ID=900300 SLURM_MEM_PER_NODE=65536 TASKQ_IDLE_MAX=2 TASKQ_GPU_PAD_BASE=$T/nopad PATH=$T/bins:$PATH \
  exec bash "$HERE/dicc_worker.sh") > "$T/wslurm.out" 2>&1
rc=$?
n=$(python3 -c "import os; print(len(os.sched_getaffinity(0)))")
slots=$(( n >= 24 ? 2 : 1 ))
check "Slurm：作业号当名字，按核数和内存定同时几个，时限查不到" '[[ $rc == 0 ]] && grep -q "作业 900300 @ .*最多同时 $slots 个，CPU $n，内存 65536M.*时限 查不到，之后再查" $T/wslurm.out && grep -q "训完 t/s" $T/wslurm.out'
check "Slurm：本地暂存目录（TASKQ_STAGE_ROOT）、TASKQ_ADMITTED 交给 run_task.sh，作业结束时删掉" '[[ $(cat $T/stage) == "$TMPDIR/${TQ_STAGE_DIR_PREFIX}900300 1" && ! -e $TMPDIR/${TQ_STAGE_DIR_PREFIX}900300 ]]'
fi

# plug. 使用者的插件文件夹里的 consumers.py（CONSUMERS）：钩子照样调用；admit 挡住时不领
if want plug; then
reset
table << EOF
t/p base
EOF
mkdir -p "$T/plug"; cp -r "$TASKQ_PLUGINS/." "$T/plug/"
cat > "$T/plug/consumers.py" << 'EOT'
import os
CONSUMERS = {"demo": {"setup": lambda w: w.say("使用者的插件到了"),
                      "admit": lambda w: None if os.path.exists(os.environ["DEMO_OPEN"]) else "门关着"}}
EOT
(TASKQ_PLUGINS=$T/plug DEMO_OPEN=$T/open WORKER_ID=pueue-host-p TASKQ_SLOTS=1 TASKQ_IDLE_POLL=1 TASKQ_IDLE_EXIT=2 exec bash "$HERE/dicc_worker.sh") > "$T/wplug.out" 2>&1 &
wpid=$!
waitfor 20 'grep -q "使用者的插件到了" $T/wplug.out'
sleep 4
check "使用者的插件：admit 挡住时不领（也不退出）" 'kill -0 $wpid && [[ ! -s $T/train.log ]]'
touch "$T/open"
waitfor 30 '! kill -0 $wpid 2> /dev/null'; wait $wpid; rc=$?
check "使用者的插件：放行后领、训完" '[[ $rc == 0 ]] && grep -q "训完 t/p" $T/wplug.out'
fi

# unit. 插件的判断（不开 worker）：gpu_share 什么时候领第二个、gpu_pad 让位、resident 的 slots、读不了时段时不领也不停手上的
if want unit; then
python3 - "$HERE" "$T" << 'EOT' > "$T/unit.out" 2>&1
import importlib.util, os, sys, time, types
here, t = sys.argv[1:]
sys.path.insert(0, here)
def load(n):
    s = importlib.util.spec_from_file_location("c_" + n, f"{here}/consumers/{n}.py"); m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
share, pad, res = load("gpu_share"), load("gpu_pad"), load("resident")
said = []
w = types.SimpleNamespace(busy=lambda: 1, changed=time.time(), ADD_AFTER=900, UTIL_MAX=90, gpucsv="", nvsel=[], pad_now=lambda: 0, say=said.append)
r = [share.admit(w)]
w.changed = 0
for g in ("50 3 20000 81920", "95 10 20000 81920", "50 10 60000 81920", "50 10 20000 81920"):
    os.environ["WORKER_TEST_GPU"] = g; r.append(share.admit(w))
w.pad_now = lambda: 30000; os.environ["WORKER_TEST_GPU"] = "50 10 60000 81920"; r.append(share.admit(w))
w.busy = lambda: 0; r.append(share.admit(w))
assert r == ["刚开始，先看看", "GPU 记录不够", "GPU 使用率 95%", "显存不够（训练占 60000MiB / 81920MiB）", None, None, None], r
assert said == ["GPU 使用率 50%、训练占 20000MiB / 81920MiB，再领一个", "GPU 使用率 50%、训练占 30000MiB / 81920MiB，再领一个"], said
b = t + "/pad"
open(b + ".mib", "w").write("0"); said.clear(); pad.make_room(b, said.append)
assert os.path.exists(b + ".start") and not said
open(b + ".mib", "w").write("5000"); orig = time.sleep; time.sleep = lambda s: None; pad.make_room(b, said.append); time.sleep = orig
assert said and "30 秒内没回应" in said[0], said
w = types.SimpleNamespace(ncpu=64, mget=lambda f: "3")
os.environ.pop("TASKQ_SLOTS", None); os.environ["TASKQ_CPUS"] = "16"; os.environ["CUDA_VISIBLE_DEVICES"] = "1"
res.setup(w)
assert (w.ncpu, w.slots, w.nvsel, w.tag_each) == (16, 3, ["-i", "1"], True), vars(w)
win = load("window")
def broken():
    raise ImportError("坏了")
win._shift = broken; os.environ["TASKQ_SITE"] = "site-pueue2"; said.clear(); stops = []
w = types.SimpleNamespace(say=said.append, runs={1: 1}, zomb={}, shutdown=lambda *a: stops.append(a))
assert win.active(w) is True and "读到之前不领" in said[0], said
assert win.admit(w) and win.admit(w).startswith("读不了时段"), win.admit(w)
win.tick(w); assert not stops, stops
print("ok")
EOT
check "插件：gpu_share 的判断、gpu_pad 让位、resident 的核数和 slots、读不了时段" 'grep -qx ok $T/unit.out || { cat $T/unit.out; false; }'
fi

(( fails == 0 )) && echo "全部通过" || { echo "$fails 项失败"; exit 1; }
