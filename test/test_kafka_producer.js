// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

'use strict';

var test = require('tape');
var KafkaProducer = require('../kafka-rest-client/kafka_producer');

var KafkaRestProxyServer = require('./lib/kafka-rest-proxy-server');

function onConnect(err) {
    /* eslint-disable no-undef,no-console,block-scoped-var */
    if (!err) {
        console.log('KafkaRestClient connected to kafka');
    } else {
        console.log('KafkaRestClient could not connect to kafka');
    }
    /* eslint-enable no-undef,no-console,block-scoped-var */
}

test('Kafka producer could write with produce.', function testKafkaProducer(assert) {
    var server = new KafkaRestProxyServer(4444);
    server.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);

    assert.equal(producer.restClient.enable, false);
    producer.produce('testTopic0', 'Important message', onSuccessResponse);
    producer.logLine('testTopic1', 'Important message', onSuccessResponse);
    producer.logLine('testTopic10', 'Important message', onTopicNotFoundError);

    function onSuccessResponse(err, res) {
        assert.equal(producer.restClient.enable, true);
        assert.equal(err, null);
        assert.equal(res, '{ version : 1, Status : SENT, message : {}}');
    }

    function onTopicNotFoundError(err, res) {
        assert.equal(producer.restClient.enable, true);
        assert.throws(err, new Error('Topics Not Found.'));
        assert.equal(res, undefined);
    }
    /* eslint-disable no-undef,block-scoped-var */
    setTimeout(function stopTest1() {
        server.stop();
        producer.close();
    }, 1000);
    /* eslint-enable no-undef,block-scoped-var */
    assert.end();
});

test('Kafka producer handle unavailable proxy.', function testKafkaProducerHandleUnavailableProxy(assert) {
    var configs = {
        proxyHost: 'localhost',
        proxyPort: 5555,
        proxyRefreshTime: 0
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);
    assert.equal(producer.restClient.enable, false);
    function onClientNotEnalbeError(err, res) {
        assert.throws(err, new Error('Kafka Rest Client is not enabled yet.'));
        assert.equal(res, undefined);
    }
    producer.logLine('avro650', 'Important message', onClientNotEnalbeError);
    producer.close();
    assert.end();
});

test('Kafka producer refresh.', function testKafkaProducerTopicRefresh(assert) {
    var server2 = new KafkaRestProxyServer(6666);
    server2.start();

    var configs = {
        proxyHost: 'localhost',
        proxyPort: 6666,
        proxyRefreshTime: 1000
    };

    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);
    assert.equal(producer.restClient.topicDiscoveryTimes, 0);
    /* eslint-disable no-undef,block-scoped-var */
    setTimeout(function wait1() {
        assert.equal(producer.restClient.topicDiscoveryTimes, 1);
    }, 1000);
    setTimeout(function wait2() {
        assert.equal(producer.restClient.topicDiscoveryTimes, 2);
    }, 2000);
    setTimeout(function stopTest2() {
        producer.close();
        server2.stop();
    }, 3000);
    /* eslint-enable no-undef,block-scoped-var */
    assert.end();
});

test('Test get whole msg', function testKafkaProducerGetWholeMsgFunction(assert) {
    var configs = {
        proxyHost: 'localhost',
        proxyPort: 8888,
        proxyRefreshTime: 0,
        shouldAddTopicToMessage: true
    };
    var testTimeStamp = Date.now() / 1000.0;
    var hostName = require('os').hostname();
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);
    var testTopic = 'testTopic0';
    var testMsg = 'testMsg0';
    var wholeMsg = producer.getWholeMsg(testTopic, testMsg, testTimeStamp);

    // console.log(wholeMsg);
    assert.equal(wholeMsg.host, hostName);
    assert.equal(wholeMsg.msg, testMsg);
    assert.equal(wholeMsg.topic, testTopic);
    assert.equal(wholeMsg.ts, testTimeStamp);
    assert.end();
    producer.close();
});
