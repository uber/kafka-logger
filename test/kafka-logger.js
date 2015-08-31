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

var test = require('tape');

var KafkaLogger = require('../index.js');

function fakeProber(listener) {
    return {
        probe: function (thunk) {
            listener('thunk');
            thunk();
        }
    };
}

function fakeKafkaClient(listener) {
    return {
        produce: function (msg, meta, cb) {
            listener('message', msg, meta);
            if (cb) {
                cb();
            }
        }
    };
}

test('KafkaLogger can log messages', function (assert) {
    var messages = [];
    var logger = new KafkaLogger({
        kafkaClient: fakeKafkaClient(function (t, msg, meta) {
            messages.push([msg, meta]);
        }),
        topic: 'my-topic'
    });

    logger.log('warn', 'hello');
    logger.log('error', 'oops', { foo: 'bar' });

    assert.equal(messages.length, 2);
    assert.equal(messages[0][0], 'my-topic');
    assert.equal(messages[0][1].level, 'warn');
    assert.equal(messages[0][1].msg, 'hello');
    assert.equal(messages[1][0], 'my-topic');
    assert.equal(messages[1][1].level, 'error');
    assert.equal(messages[1][1].msg, 'oops');

    assert.end();
});

test('KafkaLogger writes to a prober', function (assert) {
    var messages = [];
    var probes = [];
    var logger = new KafkaLogger({
        kafkaClient: fakeKafkaClient(function (t, msg, meta) {
            messages.push([msg, meta]);
        }),
        kafkaProber: fakeProber(function (type) {
            probes.push(type);
        }),
        topic: 'my-topic-2'
    });

    logger.log('info', 'oh hai', {}, function () {});
    logger.log('error', new Error('oops'), {}, function () {});

    assert.equal(messages.length, 2);
    assert.equal(probes.length, 2);

    assert.deepEqual(probes, ['thunk', 'thunk']);
    assert.equal(messages[0][0], 'my-topic-2');
    assert.equal(messages[0][1].level, 'info');
    assert.equal(messages[0][1].msg, 'oh hai');
    assert.equal(messages[1][0], 'my-topic-2');
    assert.equal(messages[1][1].level, 'error');
    assert.equal(messages[1][1].msg.message, 'oops');

    assert.end();
});

test('logger adds properties', function (assert) {
    var messages = [];
    var logger = new KafkaLogger({
        topic: 'foobar',
        kafkaClient: fakeKafkaClient(function (t, msg, meta) {
            messages.push([msg, meta]);
        }),
        properties: {
            regionName: 'New_York'
        }
    });

    logger.log('info', 'oh hai');

    assert.equal(messages.length, 1);
    assert.equal(messages[0][1].msg, 'oh hai');
    assert.equal(messages[0][1].regionName, 'New_York');

    assert.end();
});

test('invalid circular json meta', function (assert) {
    var messages = [];
    var logger = new KafkaLogger({
        topic: 'foobar',
        kafkaClient: fakeKafkaClient(function (t, msg, meta) {
            messages.push([msg, meta]);
        })
    });

    var meta = {};
    meta.meta = meta;
    logger.log('info', 'oh hai', meta);

    assert.equal(messages.length, 1);
    assert.equal(messages[0][0], 'foobar');
    assert.equal(messages[0][1].msg, 'oh hai');

    assert.end();
});

test('test probe failures trigger failureHandler', function (assert) {
    var prober = {
        probe: function (thunk, bypass) {
            // side effect behind thunk is invalid. call bypass
            process.nextTick(function () {
                bypass(new Error('oops!'));
            });
        }
    };

    var logger = new KafkaLogger({
        topic: 'foobar',
        kafkaClient: fakeKafkaClient(),
        kafkaProber: prober,
        failureHandler: function (err) {
            assert.equal(err.message, 'oops!');

            assert.end();
        }
    });

    logger.log('info', 'oh hai');
});
