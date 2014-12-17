/* jshint forin: false */
var util = require('util');
var Transport = require('winston').Transport;
var NodeSol = require('uber-nodesol-write').NodeSol;
var hostName = require('os').hostname();
var extend = require('xtend');

function KafkaLogger(options) {
    if (!(this instanceof KafkaLogger)) {
        return new KafkaLogger(options);
    }

    var self = this;

    function onConnect(err) {
        if (!err) {
            if (self.logger) {
                self.logger.info('KafkaClient connected to kafka');
            }
            self.connected = true;
            self._flush();
        } else {
            if (self.logger) {
                self.logger.warn('KafkaClient could not connect to kafka');
            }
            // connection failed, purge queue.
            self.initQueue.length = 0;
        }
    }

    options = options || {};

    Transport.call(this, options);
    this.topic = options.topic || 'unknown';
    this.leafHost = options.leafHost || 'localhost';
    this.leafPort = options.leafPort || 9093;
    this.logger = options.logger;
    this.properties = options.properties || {};
    this.dateFormats = options.dateFormats || { isodate: 'iso' };
    this.peerId = options.hasOwnProperty('peerId') ? options.peerId : -1;
    this.workerId = options.hasOwnProperty('workerId') ? options.workerId : -1;
    this.logTemplate = {
        host: hostName,
        level: "LOGLEVELHERE",
        msg: "MESSAGEHERE"
    };
    for (var dateFormat in this.dateFormats) {
        this.logTemplate[dateFormat] = this.dateFormats[dateFormat];
    }
    for (var property in this.properties) {
        this.logTemplate[property] = this.properties[property];
    }

    this.kafkaProber = options.kafkaProber || null;
    this.failureHandler = options.failureHandler || null;
    this.kafkaClient = options.kafkaClient || null;

    this.connected = true;
    this.initQueue = [];
    this.initTime = null;

    if (!this.kafkaClient) {
        this.connected = false;
        this.initTime = Date.now();
        this.kafkaClient = new NodeSol({
            leafHost: this.leafHost, leafPort: this.leafPort
        });
        this.kafkaClient.connect(onConnect);
    }
}

util.inherits(KafkaLogger, Transport);

KafkaLogger.prototype.name = 'KafkaLogger';

KafkaLogger.prototype.destroy = function destroy() {
    var producer = this.kafkaClient.get_producer(this.topic);

    if (producer && producer.connection &&
        producer.connection.connection &&
        producer.connection.connection._connection
    ) {
        producer.connection.connection._connection.destroy();
    }
};

KafkaLogger.prototype._flush = function _flush() {
    while (this.initQueue.length > 0) {
        var tuple = this.initQueue.shift();
        produceMessage(this, tuple[0], tuple[1]);
    }
};

KafkaLogger.prototype.log = function(level, msg, meta, callback) {
    var logMessage = extend(this.logTemplate);
    meta = meta || {};

    var d = new Date();
    for (var dateFormat in this.dateFormats) {
        switch(this.dateFormats[dateFormat]) {
        case 'epoch':
            logMessage[dateFormat] = Math.floor(d.getTime() / 1000);
            break;
        case 'jsepoch':
            logMessage[dateFormat] = d.getTime();
            break;
        case 'pyepoch':
            logMessage[dateFormat] = d.getTime() / 1000;
            break;
        case 'iso':
            /* falls through */
        default:
            logMessage[dateFormat] = d.toISOString();
            break;
        }
    }
    logMessage.level = level;
    try {
        logMessage.msg = msg + ' ' + JSON.stringify(meta);
    } catch (e) {
        logMessage.msg = msg + ' bad meta object of type ' +
                (typeof (meta) === 'object' ? meta.constructor.name :
                typeof (meta)) + ' ' + e.message;
    }

    if (!this.connected && Date.now() < this.initTime + 5000) {
        return this.initQueue.push([logMessage, callback]);
    } else if (this.connected && this.initQueue.length) {
        this._flush();
    }

    produceMessage(this, logMessage, callback)
};

function produceMessage(self, logMessage, callback) {
    var failureHandler = self.failureHandler

    if (self.kafkaProber) {
        var thunk = self.kafkaClient.produce.bind(self.kafkaClient,
            self.topic, logMessage);
        self.kafkaProber.probe(thunk, onFailure);
    } else {
        self.kafkaClient.produce(self.topic, logMessage, callback);
    }

    function onFailure(err) {
        if (failureHandler) {
            failureHandler(err, logMessage);
        }

        if (callback && typeof callback === 'function') {
            callback(err);
        }
    }
}

module.exports = KafkaLogger;
