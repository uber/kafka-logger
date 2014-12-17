var test = require('tape');

var KafkaLogger = require('../index.js');
var KafkaServer = require('./lib/kafka-server.js');

test('KafkaLogger writes to a real kafka server', function (assert) {
    var server = KafkaServer(function (msg) {
        assert.equal(msg.topic, 'test-topic');

        var message = msg.messages[0];
        assert.equal(message.payload.level, 'error');
        assert.equal(message.payload.msg, 'some message {}');

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
