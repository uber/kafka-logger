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
var KafkaServer = require('./lib/kafka-server.js');

test('KafkaLogger writes to a real kafka server', function (assert) {
    var server = KafkaServer(function (error, msg) {
        assert.equal(msg.topic, 'test-topic');

        var message = msg.messages[0];
        assert.equal(message.payload.level, 'error');
        assert.equal(message.payload.msg, 'some message');

        server.close();
        logger.destroy();
        assert.end();
    });

    var logger = new KafkaLogger({
        topic: 'test-topic',
        leafHost: 'localhost',
        leafPort: server.port
    });

    logger.log('error', 'some message');
});

var KafkaRestProxyServer = require('./lib/kafka-rest-proxy-server');
test('KafkaLogger double writes to a real kafka server', function (assert) {
  var server = KafkaServer(function (error, msg) {
      assert.equal(msg.topic, 'testTopic0');

      var message = msg.messages[0];
      assert.equal(message.payload.level, 'error');
      assert.equal(message.payload.msg, 'some message');
      server.close();        
  });
  
  var restServer = new KafkaRestProxyServer(4444, function (msg) {
    assert.equal(msg.host ,require('os').hostname());
    assert.equal(msg.level,'error');
    assert.equal(msg.msg , 'some message'); 
  });
  
  restServer.start();

  var logger = new KafkaLogger({
      topic: 'testTopic0',
      leafHost: 'localhost',
      leafPort: server.port,
      proxyHost: 'localhost',
      proxyPort: 4444,
      blacklistMigrator: true,
      blacklistMigratorUrl: 'localhost:2222'
  });

  logger.log('error', 'some message');
  
  setTimeout(function stopRestServer() {
    restServer.stop();
    logger.destroy();
    assert.end();
  }, 1000);
  
});
