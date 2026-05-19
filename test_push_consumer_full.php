<?php
/**
 * PushConsumer 完整消息收发测试脚本
 * 
 * 此脚本演示完整的消息收发流程：
 * 1. 启动生产者发送消息
 * 2. 启动消费者接收消息
 * 
 * 配置:
 * - 服务端点: 120.0.0.1:8081
 * - Topic: TestTopic
 * - Consumer Group: GID-normal-consumer_topic-normal
 */

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/ProducerOptimized.php';
require_once __DIR__ . '/PushConsumer.php';
require_once __DIR__ . '/ConsumeResult.php';
require_once __DIR__ . '/Logger.php';

use Apache\Rocketmq\ProducerOptimized;
use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\ConsumeResult;

// 配置参数
$endpoints = '127.0.0.1:8081';
$topic = 'TestTopic';
$consumerGroup = 'GID-normal-consumer_topic-normal';

echo "=== PushConsumer 完整消息收发测试 ===\n";
echo "服务端点: {$endpoints}\n";
echo "Topic: {$topic}\n";
echo "Consumer Group: {$consumerGroup}\n\n";

// 第一步：发送消息
echo "步骤 1: 发送测试消息到 {$topic}\n";
echo str_repeat("-", 50) . "\n";

try {
    $producer = new ProducerOptimized($endpoints, [
        'topics' => [$topic],
        'maxAttempts' => 3,
        'requestTimeout' => 3000,
    ]);

    $producer->start();
    echo "✓ 生产者启动成功\n\n";

    // 发送5条测试消息
    $sentMessages = [];
    for ($i = 1; $i <= 5; $i++) {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $sysProps = new SystemProperties();
        $sysProps->setTag('test-tag');
        $sysProps->setKeys(['test-key-' . $i]);

        $messageBody = "测试消息 #{$i} - 时间戳: " . date('Y-m-d H:i:s');
        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($messageBody);
        $message->setSystemProperties($sysProps);

        try {
            $result = $producer->send($message);
            $messageId = $result['messageId'] ?? 'unknown';
            echo "  ✓ 发送消息 #{$i}: {$messageBody}\n";
            echo "    Message ID: {$messageId}\n";
            
            $sentMessages[] = [
                'index' => $i,
                'body' => $messageBody,
                'messageId' => $messageId,
                'keys' => ['test-key-' . $i]
            ];
        } catch (\Throwable $e) {
            echo "  ✗ 发送消息 #{$i} 失败: " . $e->getMessage() . "\n";
        }
        
        // 短暂延迟，确保消息顺序
        usleep(100000); // 100ms
    }

    $producer->shutdown();
    echo "\n✓ 生产者关闭，共发送 " . count($sentMessages) . " 条消息\n\n";

} catch (\Exception $e) {
    echo "✗ 生产者操作失败: " . $e->getMessage() . "\n";
    exit(1);
}

// 第二步：接收消息
echo "步骤 2: 启动消费者接收消息\n";
echo str_repeat("-", 50) . "\n";

// 用于跟踪接收到的消息
$receivedMessages = [];
$messageCount = 0;
$targetMessageCount = 5;
$consumerStarted = false;

$consumer = new PushConsumer($endpoints, $consumerGroup, [
    'subscriptionExpressions' => [$topic => '*'],
    'messageListener' => function($messageView) use (&$receivedMessages, &$messageCount, $targetMessageCount) {
        $body = $messageView->getBody() ?? '';
        $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
        $keys = method_exists($messageView, 'getKeys') ? $messageView->getKeys() : [];
        
        echo "  ✓ 收到消息: {$body}\n";
        echo "    - Message ID: {$messageId}\n";
        if (!empty($keys)) {
            echo "    - Keys: " . implode(', ', $keys) . "\n";
        }
        
        $receivedMessages[] = [
            'body' => $body,
            'messageId' => $messageId,
            'keys' => $keys,
            'timestamp' => time()
        ];
        
        $messageCount++;
        
        // 如果收到目标数量的消息，可以考虑停止（在实际应用中可能需要更复杂的逻辑）
        if ($messageCount >= $targetMessageCount) {
            echo "\n已收到预期的 {$targetMessageCount} 条消息\n";
        }
        
        return ConsumeResult::SUCCESS;
    },
    'scanIntervalSeconds' => 2,
    'receiveBatchSize' => 10,
]);

// 设置信号处理以便优雅退出
pcntl_signal(SIGTERM, function() use ($consumer) {
    echo "\n收到终止信号，正在关闭消费者...\n";
    $consumer->shutdown();
    exit(0);
});

pcntl_signal(SIGINT, function() use ($consumer) {
    echo "\n收到中断信号，正在关闭消费者...\n";
    $consumer->shutdown();
    exit(0);
});

echo "启动消费者监听...\n";
echo "注意: 消费者将持续运行直到手动停止 (Ctrl+C)\n";
echo "或者在收到 {$targetMessageCount} 条消息后自动停止（如果实现相应逻辑）\n\n";

// 启动消费者（阻塞式）
try {
    $consumer->start();
} catch (\Exception $e) {
    echo "✗ 消费者启动失败: " . $e->getMessage() . "\n";
    exit(1);
}

// 注意: 由于start()是阻塞的，以下代码在消费者停止前不会执行
echo "\n=== 测试结果摘要 ===\n";
echo "发送消息数: " . count($sentMessages) . "\n";
echo "接收消息数: " . count($receivedMessages) . "\n";

if (count($receivedMessages) > 0) {
    echo "\n接收到的消息详情:\n";
    foreach ($receivedMessages as $index => $msg) {
        echo "  [" . ($index + 1) . "] {$msg['body']}\n";
        echo "      ID: {$msg['messageId']}\n";
        if (!empty($msg['keys'])) {
            echo "      Keys: " . implode(', ', $msg['keys']) . "\n";
        }
    }
    
    // 检查是否所有发送的消息都被接收
    $allReceived = true;
    foreach ($sentMessages as $sentMsg) {
        $found = false;
        foreach ($receivedMessages as $recvMsg) {
            if ($sentMsg['body'] === $recvMsg['body']) {
                $found = true;
                break;
            }
        }
        if (!$found) {
            $allReceived = false;
            echo "  ✗ 未找到消息: {$sentMsg['body']}\n";
        }
    }
    
    if ($allReceived && count($sentMessages) === count($receivedMessages)) {
        echo "\n✓ 所有发送的消息都已成功接收!\n";
    } else {
        echo "\n⚠ 部分消息可能未被接收或存在额外消息。\n";
    }
}

echo "\n=== 测试完成 ===\n";
