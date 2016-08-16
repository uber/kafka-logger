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
var util = require('util');
var http = require('http');

function KafkaRestProxyServer(port, assertion) {
    var self = this;
    self.listenPort = port;
    self.sockets = {};
    self.nextSocketId = 0;
    self.assertion = assertion;
    http.Server.call(this, this.handle);
}

util.inherits(KafkaRestProxyServer, http.Server);

KafkaRestProxyServer.prototype.handle = function handle(req, res) {
    var self = this;
    self.started = false;
    var messages = {
        'localhost:4444': ['testTopic0', 'testTopic1', 'testTopic2', 'testTopic3'],
        'localhost:5555': ['testTopic4', 'testTopic5', 'testTopic6', 'testTopic7']
    };
    if (req.method === 'GET') {
        res.end(JSON.stringify(messages));
    } else if (req.method === 'POST') {

        var body = '';
        req.on('data', function (data) {
            data = data.slice(8, data.length);
            body += data;
        });
        req.on('end', function () {
            self.assertion(JSON.parse(body))
        });
        if (req.headers.timestamp) {
            res.end('{ version : 1, Status : SENT, message : {}}');
        } else {
            res.end('Not found timestamp field in request header!');
        }
    }
};

KafkaRestProxyServer.prototype.start = function start() {
    var self = this;
    this.listen(self.listenPort, function started() {
        self.started = true;
        // console.log('Listening for HTTP requests on port %d.',
        // self.listenPort);
    });
    this.on('connection', function connect(socket) {
        // Add a newly connected socket
        var socketId = self.nextSocketId++;
        self.sockets[socketId] = socket;
        // console.log('socket', socketId, 'opened');

        // Remove the socket when it closes
        socket.on('close', function close() {
            // console.log('socket', socketId, 'closed');
            delete self.sockets[socketId];
        });
    });
};

KafkaRestProxyServer.prototype.stop = function stop() {
    var self = this;
    self.started = false;
    self.close(function close() {
        // console.log('Stopped listening.');
    });
    // Destroy all open sockets
    for (var socketId in self.sockets) {
        if (self.sockets.hasOwnProperty(socketId)) {
            // console.log('socket', socketId, 'destroyed');
            self.sockets[socketId].destroy();
        }
    }
};

module.exports = KafkaRestProxyServer;
