#!/bin/bash

LOG_FILE="fail_log.txt"
> "$LOG_FILE"  # 清空旧日志

for i in $(seq 1 200); do
    echo "===== Run #$i ====="
    
    # 把本次测试的输出先捕获到一个变量
    output=$(time go test -run TestRestartSubmit4A -race 2>&1)
    status=$?

    if [ $status -ne 0 ]; then
        echo "❌ Test failed at run #$i. Saving log and exiting..."
        echo "===== Run #$i FAILED =====" >> "$LOG_FILE"
        echo "$output" >> "$LOG_FILE"
        exit 1
    fi
    echo "$output"
done

echo "✅ All 200 tests passed successfully."
