#!/bin/bash
# 任务队列的 worker：11/5 起转交给 consume.py（消费者核心，插件在 consumers/），说明见 README.md 的"消费者"。
# 文件名留着：worker_start.sh、代码包里的老入口、测试和手动跑都用它。
exec python3 "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/consume.py" "$@"
