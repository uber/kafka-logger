# winston-kafka

A [kafka]() transport for [winston]()

## Usage

```js
var winston = require('winston');

winston.transports.Kafka = require('winston-kafka');

winston.add(winston.transports.Kafka, options);
```

The Kafka transport currently uses [nodesol]() which uses [prozess]() internally. Use of prozess directly is ongoing. The Kafka transport takes the following options:

* `topic` - The Kafka topic to publish to.
* `host` - The Kafka host to publish to.
* `port` - The Kafka port to publish to.
* `properties` - Top-level properties that should be added to the JSON object published to the kafka topic; useful if multiple processes use the same topic
* `dateFormats` - An object of date formats to use; keys are the names of the keys the format should be added to, values are the names of the formats (useful for cross-language usage of the logs to reduce transforms on the consumers). These formats are: `epoch` (time in sec since Jan 1, 1970), `jsepoch` (time in ms since Jan 1, 1970), `pyepoch` (time in sec since Jan 1, 1970, but floating point with ms resolution), `iso` (ISO datestring format)

## Install

```sh
npm install winston winston-kafka
```

## Testing

```sh
npm test
```

There is a `kafka.js` that will talk to kafka if it is running and just
    gets skipped if its not running.

To run kafka run zookeeper & kafka with `npm run start-zk` and
    `npm run start-kafka`

## License (MIT)

Copyright (C) 2013 by Uber Technologies, Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

