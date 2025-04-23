#!/bin/bash

for i in $(seq 1 200); do
    echo "===== Run #$i ====="
    
    # 执行测试命令
    go test -run 3A -race

    # 检查上一条命令是否成功
    if [ $? -ne 0 ]; then
        echo "❌ Test failed at run #$i. Exiting..."
        exit 1
    fi
done

echo "✅ All 200 tests passed successfully."
