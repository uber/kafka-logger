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
var KafkaRestProxyServer = require('./lib/kafka-rest-proxy-server');

test('KafkaLogger writes to a real kafka server', function (assert) {
    var server = new KafkaRestProxyServer(4444, function (msg) {
        server.emit('msg', msg);
    });

    server.start();

    var isDisabledFlag = false;
    var logger = new KafkaLogger({
        topic: 'testTopic0',
        proxyHost: 'localhost',
        proxyPort: 4444,
        blacklistMigrator: true,
        blacklistMigratorUrl: 'localhost:2222',
        isDisabled: function () {
            return isDisabledFlag;
        }
    });

    server.once('msg', function (msg) {
        assert.ok(msg);

        isDisabledFlag = true;
        logger.log('error', 'some message', {}, function () {
            isDisabledFlag = false;

            server.removeListener('msg', failure);
            logger.log('error', 'some message');
            server.once('msg', function (msg) {
                assert.ok(msg);

                logger.destroy();
                server.stop();
                assert.end();
            });
        });

        server.on('msg', failure);

        function failure(msg) {
            assert.ok(false, 'wrote a message');
        }
    });

    logger.log('error', 'some message');
});
