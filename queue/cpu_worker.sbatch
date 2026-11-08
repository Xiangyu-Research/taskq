#!/bin/bash
#SBATCH --nodes=1
# CPU worker job: same as worker.sbatch (squeue recognises a CPU worker by the file name)
WORKER_DEVICE=cpu exec bash "$1"
