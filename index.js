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

/* jshint forin: false */
var util = require('util');
var Transport = require('winston-uber').Transport;
var NodeSol = require('nodesol-write').NodeSol;
var KafkaRestClient = require('kafka-rest-client');
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


    function onKafkaRestClientConnect(err) {
        if (!err) {
            if (self.logger) {
                self.logger.info('KafkaRestClient connected to kafka');
            }
            self.kafkaRestClientConnected = true;
            self._flush();
        } else {
            if (self.logger) {
                self.logger.warn('KafkaRestClient could not connect to kafka');
            }
        }
    }

    options = options || {};

    Transport.call(this, options);
    this.topic = options.topic || 'unknown';
    this.leafHost = options.leafHost;
    this.leafPort = options.leafPort;
    this.proxyHost = options.proxyHost || 'localhost';
    if ('proxyPort' in options && options.proxyPort) {
        this.proxyPort = options.proxyPort;
    }
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
    this.isDisabled = options.isDisabled || null;

    this.connected = true;
    this.kafkaRestClientConnected = false;
    this.initQueue = [];
    this.initTime = null;
    this.statsd = options.statsd || null;
    if (!this.kafkaRestClient) {
        if (this.proxyPort) {
            var kafkaRestClientOptions = {
                proxyHost: this.proxyHost,
                proxyPort: this.proxyPort,
                statsd: this.statsd
            };
            if ('maxRetries' in options) {
                kafkaRestClientOptions['maxRetries'] = options.maxRetries;
            }
            if ('statsd' in options) {
                kafkaRestClientOptions['statsd'] = options.statsd;
            }
            if (options.batching) {
                kafkaRestClientOptions['batching'] = options.batching;
            }
            if (options.batchingWhitelist) {
                kafkaRestClientOptions['batchingWhitelist'] = options.batchingWhitelist;
            }
            this.kafkaRestClient = new KafkaRestClient(kafkaRestClientOptions);
            this.kafkaRestClient.connect(onKafkaRestClientConnect);
        }
    } else {
        this.kafkaRestClientConnected = true;
    }

    if (!this.kafkaClient && this.leafHost && this.leafPort) {
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
    if (this.kafkaClient) {
        var producer = this.kafkaClient.get_producer(this.topic);

        if (producer && producer.connection &&
            producer.connection.connection &&
            producer.connection.connection._connection
        ) {
            producer.connection.connection._connection.destroy();
        }
    }

    if (this.kafkaRestClient) {
        this.kafkaRestClient.close();
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
    var timestamp = d.getTime();
    for (var dateFormat in this.dateFormats) {
        switch(this.dateFormats[dateFormat]) {
        case 'epoch':
            logMessage[dateFormat] = Math.floor(timestamp / 1000);
            break;
        case 'jsepoch':
            logMessage[dateFormat] = timestamp;
            break;
        case 'pyepoch':
            logMessage[dateFormat] = timestamp / 1000;
            break;
        case 'iso':
            /* falls through */
        default:
            logMessage[dateFormat] = d.toISOString();
            break;
        }
    }
    if (!logMessage.ts) {
        logMessage.ts = timestamp;
    }
    logMessage.level = level;
    logMessage.msg = msg;
    logMessage.fields = meta;

    if ((!this.connected || (this.kafkaRestClient && !this.kafkaRestClientConnected))  && Date.now() < this.initTime + 5000) {
        return this.initQueue.push([logMessage, callback]);
    } else if (this.connected && this.initQueue.length) {
        this._flush();
    }

    produceMessage(this, logMessage, callback)
};

function produceMessage(self, logMessage, callback) {
    if (self.isDisabled && self.isDisabled()) {
        if (callback) {
            process.nextTick(callback);
        }
        return;
    }

    var failureHandler = self.failureHandler

    if (self.kafkaClient) {
        if (self.kafkaProber) {
            var thunk = self.kafkaClient.produce.bind(self.kafkaClient,
                self.topic, logMessage);
            self.kafkaProber.probe(thunk, onFailure);
        } else {
            self.kafkaClient.produce(self.topic, logMessage, callback);
        }
    }

    if (self.kafkaRestClientConnected) {
        self.kafkaRestClient.produce(self.topic, JSON.stringify(logMessage), logMessage.ts);
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
