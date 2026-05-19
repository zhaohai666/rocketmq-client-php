<?php
/**
 * PushConsumer 消息生产者测试脚本
 * 
 * 用于向指定Topic发送测试消息
 * 
 * 配置:
 * - 服务端点: 120.0.0.1:8081
 * - Topic: TestTopic
 */

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/ProducerOptimized.php';
require_once __DIR__ . '/Logger.php';

use Apache\Rocketmq\ProducerOptimized;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

// 配置参数
$endpoints = '127.0.0.1:8081';
$topic = 'TestTopic';

echo "=== PushConsumer 消息生产者测试 ===\n";
echo "服务端点: {$endpoints}\n";
echo "Topic: {$topic}\n\n";

try {
    $producer = new ProducerOptimized($endpoints, [
        'topics' => [$topic],
        'maxAttempts' => 3,
        'requestTimeout' => 3000,
    ]);

    $producer->start();
    echo "生产者启动成功\n\n";

    // 发送5条测试消息
    echo "开始发送5条测试消息...\n";
    for ($i = 1; $i <= 5; $i++) {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $sysProps = new SystemProperties();
        $sysProps->setTag('test-tag');
        $sysProps->setKeys(['test-key-' . $i]);

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody("测试消息 #{$i} - 时间戳: " . date('Y-m-d H:i:s'));
        $message->setSystemProperties($sysProps);

        try {
            $result = $producer->send($message);
            echo "  ✓ 发送消息 #{$i} 成功, messageId=" . $result['messageId'] . "\n";
        } catch (\Throwable $e) {
            echo "  ✗ 发送消息 #{$i} 失败: " . $e->getMessage() . "\n";
        }
        
        // 短暂延迟，确保消息顺序
        usleep(100000); // 100ms
    }

    $producer->shutdown();
    echo "\n生产者关闭，消息发送完成\n";
    echo "现在可以启动消费者来接收这些消息。\n";

} catch (\Exception $e) {
    echo "生产者操作失败: " . $e->getMessage() . "\n";
    exit(1);
}
