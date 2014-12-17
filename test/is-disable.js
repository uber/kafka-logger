var test = require('tape');

var KafkaLogger = require('../index.js');
var KafkaServer = require('./lib/kafka-server.js');

test('KafkaLogger writes to a real kafka server', function (assert) {
    var server = KafkaServer(function (msg) {
        server.emit('msg', msg);
    });

    var isDisabledFlag = false;
    var logger = new KafkaLogger({
        topic: 'test-topic',
        leafHost: 'localhost',
        leafPort: server.port,
        isDisabled: function () {
            return isDisabledFlag;
        }
    });

    logger.log('error', 'some message');
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
                server.close();
                assert.end();
            });
        });

        server.on('msg', failure);

        function failure(msg) {
            assert.ok(false, 'wrote a message');
        }
    });
});
