#!/bin/bash

LOG_FILE="fail_log.txt"
> "$LOG_FILE"  # 清空旧日志

for i in $(seq 1 50); do
    echo "===== Run #$i ====="
    
    # 把本次测试的输出先捕获到一个变量
    output=$(time go test 2>&1)
    status=$?

    if [ $status -ne 0 ]; then
        echo "❌ Test failed at run #$i. Saving log and exiting..."
        echo "===== Run #$i FAILED =====" >> "$LOG_FILE"
        echo "$output" >> "$LOG_FILE"
        exit 1
    fi
done

echo "✅ All 50 tests passed successfully."
