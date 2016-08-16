# kafka-logger

A [kafka](http://kafka.apache.org/) transport for [winston](https://github.com/winstonjs/winston)

## Usage

```js
var winston = require('winston');

winston.transports.Kafka = require('kafka-logger');

winston.add(winston.transports.Kafka, options);
```

The Kafka transport currently uses [node-kafka-rest-client](). The Kafka transport takes the following options:

* `topic` - The Kafka topic to publish to.
* `proxyHost` - The Kafka REST Proxy host to publish to.
* `proxyPort` - The Kafka REST Proxy port to publish to.
* `properties` - Top-level properties that should be added to the JSON object published to the kafka topic; useful if multiple processes use the same topic
* `dateFormats` - An object of date formats to use; keys are the names of the keys the format should be added to, values are the names of the formats (useful for cross-language usage of the logs to reduce transforms on the consumers). These formats are: `epoch` (time in sec since Jan 1, 1970), `jsepoch` (time in ms since Jan 1, 1970), `pyepoch` (time in sec since Jan 1, 1970, but floating point with ms resolution), `iso` (ISO datestring format)

## Install

```sh
npm install winston kafka-logger
```

## Testing

```sh
npm test
```

There is a `kafka.js` that will talk to kafka if it is running and just
    gets skipped if its not running.

To run kafka run zookeeper & kafka with `npm run start-zk` and
    `npm run start-kafka`

## MIT Licenced
