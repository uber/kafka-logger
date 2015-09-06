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

var url = require('url');
var http = require('http');
var lbPool = require('lb_pool');

var supportContentType = {
    'binary': 'application/vnd.kafka.binary.v1',
    'json': 'application/vnd.kafka.json.v1+json'
};

var emptyFunction = function EmptyFunction() {
};

// Kafka 8 Rest Proxy Client.
// Initialization: takes host and port and send a GET call to get all the
// <topic, host:port> mapping.
// Produce: for sending request, forkafka client will send a POST call
function KafkaRestClient(proxyHost, proxyPort, refreshTime, callback) {
    var self = this;
    callback = callback || emptyFunction;
    self.enable = false;
    self.connecting = false;
    self.proxyHost = proxyHost;
    self.urlToHttpClientMapping = {};
    self.proxyPort = proxyPort;
    self.requestUrl = self.proxyHost + ':' + self.proxyPort;
    self.mainHttpClient = new lbPool.Pool(http, [self.requestUrl]);
    self.proxyRefreshTime = refreshTime;
    self.topicDiscoveryTimes = 0;

    self.discoverTopics(function discoverTopicsCallback(err, isDone) {
        if (err || !isDone) {
            self.enable = false;
            callback(new Error('Failed to discover topics mapping'));
        } else {
            self.enable = true;
            callback();
        }
    });
    self.scheduleTopicMapRefresh();
}

KafkaRestClient.prototype.scheduleTopicMapRefresh = function scheduleTopicMapRefresh() {
    var self = this;
    if (self.proxyRefreshTime > 0) {
        // console.log('Trying to schedule topic refresh job every: ' +
        // self.proxyRefreshTime / 1000 + ' seconds.');
        /* eslint-disable no-undef,block-scoped-var */
        self.refreshIntervalId = setInterval(function refreshTopics() {
            self.discoverTopics(function refreshTopicsCallback(err, isDone) {
                if (err === null && isDone) {
                    self.enable = true;
                }
            });
        }, self.proxyRefreshTime);
        /* eslint-enable no-undef,block-scoped-var */
    }
};

KafkaRestClient.prototype.discoverTopics = function discoverTopics(callback) {
    var self = this;
    self.connecting = true;
    this.getTopicRequestBody(self.proxyHost, self.proxyPort, function updateTopicToUrlMapping(err, res) {
        if (err) {
            self.connecting = false;
            callback(err, false);
        } else {
            self.connecting = false;
            self.enable = true;
            self.cachedTopicToUrlMapping = self.getTopicToUrlMapping(res);
            self.topicDiscoveryTimes++;
            callback(err, true);
        }
    });
};

KafkaRestClient.prototype.getTopicRequestBody = function getTopicRequestBody(proxyHost, proxyPort, callback) {
    var self = this;
    self.mainHttpClient.get('/topics', function handleGetCall(err, res, body) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, body);
        }
    });
};

KafkaRestClient.prototype.getTopicToUrlMapping = function getTopicToUrlMapping(body) {
    var urlToTopicsJson = JSON.parse(body);
    var topicToUrlMapping = {};
    var urlArray = Object.keys(urlToTopicsJson);
    for (var i = 0; i < urlArray.length; i++) {
        var reqUrl = urlArray[i];
        var topicsArray = urlToTopicsJson[reqUrl];
        for (var j = 0; j < topicsArray.length; j++) {
            topicToUrlMapping[topicsArray[j]] = reqUrl;
        }
    }
    return topicToUrlMapping;
};

// ProduceMessage {
// string topic,
// binary message,
// string type,
// long timeStamp
// }
KafkaRestClient.prototype.produce = function produce(produceMessage, callback) {
    var self = this;
    if (self.enable) {
        if (produceMessage.topic in self.cachedTopicToUrlMapping) {
            var pathUrl = self.cachedTopicToUrlMapping[produceMessage.topic];
            var httpClient;
            if (self.urlToHttpClientMapping[pathUrl] === undefined) {
                self.urlToHttpClientMapping[pathUrl] = new lbPool.Pool(http, [pathUrl]);
            }
            httpClient = self.urlToHttpClientMapping[pathUrl];

            // http post call
            // console.log('Trying to produce msg: ' + message + ',to topic: ' +
            // topic + ', through url: ' + pathUrl);
            var reqOpts = url.parse('http://' + pathUrl);
            reqOpts.method = 'POST';
            reqOpts.path = '/topics/' + produceMessage.topic;
            reqOpts.headers = {
                'Content-Type': supportContentType[produceMessage.type],
                'TimeStamp': produceMessage.timeStamp.toString()
            };
            httpClient.post(reqOpts, produceMessage.message, function handlePostCall(err, res, body) {
                if (err) {
                    callback(err);
                } else {
                    callback(null, body);
                }
            });

        } else {
            return callback(new Error('Topics Not Found.'));
        }
    } else if (self.connecting) {
        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function waitForInitDone() {
            self.produce(produceMessage, callback);
        }, 100);
        /* eslint-enable no-undef,block-scoped-var */
    } else {
        callback(new Error('Kafka Rest Client is not enabled yet.'));
    }

};

KafkaRestClient.prototype.close = function close() {
    // Trying to close kafka rest client, need to cancel timer scheduler.
    var self = this;
    self.enable = false;
    self.connecting = false;
    if (self.proxyRefreshTime > 0) {
        // Trying to cancel TimeInterval
        /* eslint-disable no-undef,block-scoped-var */
        clearInterval(self.refreshIntervalId);
        /* eslint-enable no-undef,block-scoped-var */
        self.proxyFreshTime = 0;
    }
    self.mainHttpClient.close();
    Object.keys(self.urlToHttpClientMapping).forEach(function closeHttpClient(reqUrl) {
        self.urlToHttpClientMapping[reqUrl].close();
    });
};

module.exports = KafkaRestClient;
