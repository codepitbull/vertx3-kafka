package de.codeptibull.vertx.kafka.writer;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageConsumer;

import static org.apache.commons.lang3.Validate.notEmpty;

/**
 * A simple writer for persisting events in Kafka.
 */
public class KafkaWriterVerticle extends AbstractVerticle {

    public static final String ADDR_EVENTSTORE_WRITE = "eventstore.write";
    public static final String TOPIC = "topic";
    public static final String EVENT = "event";
    public static final String CONFIG_KAFKA_HOST = "kafka_host";
    private KafkaWriter writer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        writer = new KafkaWriter(notEmpty(config().getString(CONFIG_KAFKA_HOST), "kafka_host not set"));

        MessageConsumer<JsonObject> outgoing = vertx.eventBus().<JsonObject>localConsumer(ADDR_EVENTSTORE_WRITE);
        outgoing.handler(msg ->
                        writer.write(msg.body().getString(TOPIC), msg.body().getString(EVENT),
                            succ -> msg.reply("Topic:"+succ.topic()+" Partition"+succ.partition()+" Offset:"+succ.offset()),
                            fail -> msg.fail(1, "Failed writing with exception "+fail.getMessage())
                        )
        );

        outgoing.completionHandler(complete -> startFuture.complete());

    }

    @Override
    public void stop() throws Exception {
        writer.close();
    }
}
