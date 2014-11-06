var assert = require('assert');
var net = require('net');
var uuid = require('uuid');
var NodeSol = require('uber-nodesol-write').NodeSol;
var test = global.it;

var KafkaLogger = require('../index.js');

var HOST = 'localhost';
var PORT = 2181;

test('KafkaLogger writes to a real kafka server', function (end) {
    var socket = net.connect({ host: HOST, port: PORT });

    socket.once('error', function () {
        console.log('warning: not running kafka test, kafka is down.');
        assert.ok(true);
        end();
    });

    socket.once('connect', function () {
        var logger = new KafkaLogger({
            topic: 'test-topic',
            host: HOST,
            port: PORT
        });

        var client = new NodeSol({ host: HOST, port: PORT });
        client.connect(function (err) {
            /*jshint camelcase: false*/
            assert.ifError(err);

            // fetch producer to get NodeSol client to behave
            client.get_producer('test-topic');

            client.create_consumer(uuid(), 'test-topic', {}, function (stream) {
                if (!stream) {
                    return assert.fail('no stream');
                }

                stream.once('data', function (chunk) {
                    var obj = JSON.parse(String(chunk));
                    assert.equal(obj.level, 'error');
                    assert.equal(obj.msg, 'some message {}');

                    end();
                });

                logger.log('error', 'some message');
            });
        });
    });
});
