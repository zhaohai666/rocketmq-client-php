<?php
/**
 * PushConsumer 消息消费者测试脚本
 * 
 * 用于从指定Topic接收并消费消息
 * 
 * 配置:
 * - 服务端点: 120.0.0.1:8081
 * - Topic: TestTopic
 * - Consumer Group: GID-normal-consumer_topic-normal
 */

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/PushConsumer.php';
require_once __DIR__ . '/ConsumeResult.php';
require_once __DIR__ . '/Logger.php';

use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\ConsumeResult;

// 配置参数
$endpoints = '127.0.0.1:8081';
$topic = 'TestTopic';
$consumerGroup = 'GID-normal-consumer_topic-normal';

echo "=== PushConsumer 消息消费者测试 ===\n";
echo "服务端点: {$endpoints}\n";
echo "Topic: {$topic}\n";
echo "Consumer Group: {$consumerGroup}\n\n";

// 用于跟踪接收到的消息
$receivedMessages = [];
$messageCount = 0;

$consumer = new PushConsumer($endpoints, $consumerGroup, [
    'subscriptionExpressions' => [$topic => '*'],
    'messageListener' => function($messageView) use (&$receivedMessages, &$messageCount, $topic) {
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
echo "按 Ctrl+C 停止消费者\n\n";

// 启动消费者（阻塞式）
try {
    $consumer->start();
} catch (\Exception $e) {
    echo "消费者启动失败: " . $e->getMessage() . "\n";
    exit(1);
}
