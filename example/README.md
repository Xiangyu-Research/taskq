# Example project

A minimal user project: how to write `.taskq.toml`, a machine table, `policy.py`, plugins and an experiment table.

| file | contents |
|---|---|
| `.taskq.toml` | where config and plugins are |
| `config/machines.py` | three machines: `hub` (this machine, also the store), pueue executor `gpubox` (one GPU worker, one CPU worker), Slurm executor `cluster` (both executors have fake hostnames and cannot be reached) |
| `config/policy.py` | all automatic parts off; the `summary` step only goes to `gpubox`; state in `~/.local/state/taskq-example/` so other projects are untouched |
| `plugins/recipes.py` | recipe `count`: count to `n`, with checkpoint, progress and a done marker (`bin/count.sh`) |
| `exps/count/experiments.py` | two experiments expanded by `seed` into 5 tasks; each task runs a `summary` step afterwards (`bin/summary.sh`, pure CPU) |

The fixed test config [`../queue/testdata/`](../queue/testdata/) shows every field in use; field tables are in
[`../queue/README.md`](../queue/README.md)'s "Switches and config".

## Try it

Run from the repo root (`--project example` may be omitted inside `example/`); none of these contact another machine:

```bash
mkdir -p ~/taskq-example/data                     # the store's data dir (hub's data in config/machines.py)
taskq --project example check                     # check config, plugins, experiment table, every task and step, executor bundles
taskq --project example exp list                  # experiments
taskq --project example exp tasks                 # expanded tasks
taskq --project example add --exp small           # add to the hub queue (~/.local/state/taskq-example/taskq)
taskq --project example status                    # hub queue
taskq --project example show count/small/0        # how one task would run
taskq --project example site bundle hub           # push a bundle to the hub's root (~/taskq-example/root): store ops run from it
taskq --project example plan                      # what this round would do: both executors unreachable, nothing sent
taskq --project example rm count/small/0 count/small/1
```
