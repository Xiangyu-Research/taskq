# 10/31 改名前的旧名（Python 的在 cfg.py 的 LEGACY）：DICC 上 10/31 前交的作业都结束、分析脚本都改读 TASKQ_DATA 后删
#   source legacy_names.sh map      新名没设时取旧名的值（只设 shell 变量，不 export）
#   source legacy_names.sh export   TASKQ_DATA 同时 export 成旧名 BRAIN_TUMOR
if [[ ${1:-} == export ]]; then
    export BRAIN_TUMOR=$TASKQ_DATA
else
    for _ln in TASKQ_DATA=BRAIN_TUMOR TASKQ_TEST_FAKEBIN TASKQ_GPU_PAD \
               TASKQ_GPU_PAD_{BASE,SCRIPT,SWITCH,STARTED,FLOOR_MIB,TARGET_MIB,RETRY_DELAY}; do
        _n=${_ln%%=*} _o=${_ln#*=}
        [[ $_o == "$_ln" ]] && _o=RQ1_${_n#TASKQ_}
        [[ -n ${!_n:-} || -z ${!_o:-} ]] || printf -v "$_n" %s "${!_o}"
    done
    unset _ln _n _o
fi
