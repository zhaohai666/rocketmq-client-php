#!/usr/bin/env php
<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Unified test runner for PHP RocketMQ client.
 *
 * Usage: php run_all_tests.php [test_name]
 *   Without arguments: run all tests
 *   With argument: run specific test (e.g., MessageIdCodecTest)
 */

require_once __DIR__ . '/TestRunner.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\Logger;
use Apache\Rocketmq\Test\TestRunner;

// Reset logger state before tests
Logger::close();

$testDir = __DIR__;
$allTests = [
    'MessageIdCodecTest',
    'MessageIdImplTest',
    'LoggerTest',
    'ResourceTest',
    'ConsumeResultTest',
    'MessageTest',
    'MessageBuilderTest',
    'PublishingLoadBalancerTest',
    'TransactionTest',
    'TransactionExtendedTest',
    'StandardConsumeServiceTest',
    'FifoConsumeServiceTest',
    'ConsumeServiceExtendedTest',
    'ProcessQueueTest',
    'RetryPolicyTest',
    'PushConsumerTest',
    'SimpleConsumerTest',
    'EncodingTest',
    'MessageViewIntegrityTest',
    'MessageViewExtendedTest',
    'UtilitiesTest',
    'StatusCheckerTest',
    'ProducerValidationTest',
    'ConsumeTaskTest',
    'SubscriptionSettingsTest',
    'LitePushConsumerTest',
    'LiteSimpleConsumerTest',
    'ClientSessionTelemetryTest',
    'SignatureTest',
];

$startTime = microtime(true);
$totalPassed = 0;
$totalFailed = 0;
$failedTests = [];

// Parse command line arguments
$filter = $argv[1] ?? null;

echo "========================================\n";
echo "PHP RocketMQ Client Tests\n";
echo "========================================\n\n";

foreach ($allTests as $testName) {
    if ($filter && stripos($testName, $filter) === false) {
        continue;
    }

    $testFile = $testDir . '/' . $testName . '.php';
    if (!file_exists($testFile)) {
        echo "SKIPPED: {$testName} (file not found)\n\n";
        continue;
    }

    // Reset test runner counters
    TestRunner::$passed = 0;
    TestRunner::$failed = 0;

    try {
        include $testFile;
    } catch (\Throwable $e) {
        echo "ERROR: {$testName} - " . $e->getMessage() . "\n\n";
        $failedTests[] = $testName;
        continue;
    }

    if (TestRunner::$failed > 0) {
        $failedTests[] = $testName;
    }

    $totalPassed += TestRunner::$passed;
    $totalFailed += TestRunner::$failed;

    echo "\n";
}

$elapsed = number_format((microtime(true) - $startTime) * 1000, 0);

echo "========================================\n";
echo "Test Results Summary\n";
echo "========================================\n";
echo "Passed: {$totalPassed}\n";
echo "Failed: {$totalFailed}\n";
echo "Time: {$elapsed}ms\n";

if (!empty($failedTests)) {
    echo "\nFailed tests:\n";
    foreach ($failedTests as $name) {
        echo "  - {$name}\n";
    }
    exit(1);
}

echo "\nAll tests passed!\n";
exit(0);
