type WinstonTransport := {
    log: (level: String, msg: String, meta: Object, callback: Callback<>)
}

type KafkaClient := {
    produce: (topic: String, msg: Object, callback: Callback<>) => void
}

type Prober := {
    probe: (thunk: (Callback<>) => void) => void
}

winston-kafka := ({
    topic: String,
    host: String,
    port: Number,
    properties?: Object,
    dateFormats?: Object,
    peerId?: Number,
    workerId?: Number,
    kafkaProber?: Prober,
    failureHandler?: (Error) => void,
    kafkaClient?: KafkaClient
}) => WinstonTransport
