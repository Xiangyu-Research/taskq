#!/bin/bash
# legacy_names.sh 的测试（2026-11-02）：新名设了不被旧名盖掉；只有旧名时取旧名；都没有时为空；map 不 export；export 给出旧名。
#   bash scripts/taskq/queue/test_legacy_names.sh
HERE=$(cd "$(dirname "$0")" && pwd)
L=$HERE/legacy_names.sh
fails=0
ok() { if [[ $2 == "$3" ]]; then echo "通过：$1"; else echo "失败：$1（得到「$2」，应为「$3」）"; fails=$((fails + 1)); fi; }
run() { env -i PATH=/usr/bin:/bin "$@" bash -c "source '$L' map; $CHECK"; }

CHECK='echo "$TASKQ_DATA|$TASKQ_GPU_PAD|$TASKQ_GPU_PAD_BASE|$TASKQ_TEST_FAKEBIN"'
ok "新名旧名都设了：新名不变" "$(run TASKQ_DATA=/new BRAIN_TUMOR=/old TASKQ_GPU_PAD=1 RQ1_GPU_PAD=0)" "/new|1||"
ok "只有旧名：取旧名的值" "$(run BRAIN_TUMOR=/old RQ1_GPU_PAD=0 RQ1_GPU_PAD_BASE=/b RQ1_TEST_FAKEBIN=/f)" "/old|0|/b|/f"
ok "新名是空的：取旧名的值" "$(run TASKQ_DATA= BRAIN_TUMOR=/old)" "/old|||"
ok "都没有：为空" "$(run)" "|||"
CHECK='bash -c "echo \${TASKQ_DATA:-无}"'
ok "map 只设 shell 变量，不 export" "$(run BRAIN_TUMOR=/old)" "无"
CHECK='echo "${_ln:-}${_n:-}${_o:-}"'
ok "map 不留下临时变量" "$(run BRAIN_TUMOR=/old)" ""

out=$(env -i PATH=/usr/bin:/bin TASKQ_DATA=/new bash -c "source '$L' export; bash -c 'echo \$BRAIN_TUMOR'")
ok "export：TASKQ_DATA 同时 export 成旧名" "$out" "/new"

echo
[[ $fails -eq 0 ]] && echo "全部通过" || echo "$fails 项失败"
exit $((fails > 0))
