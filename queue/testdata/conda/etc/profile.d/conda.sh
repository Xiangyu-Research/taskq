conda() {
    if [[ ${1:-} == activate ]]; then export CONDA_DEFAULT_ENV=${2:-base}; fi
    return 0
}
