package de.codeptibull.vertx.kafka.writer;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.commons.lang3.Validate.notEmpty;

/**
 * A simple writer for persisting events in Kafka.
 */
public class KafkaWriterVerticle extends AbstractVerticle {

    public static final String ADDR_EVENTSTORE_WRITE = "eventstore.write";
    public static final String TOPIC = "topic";
    public static final String EVENT = "event";
    private KafkaProducer<String, String> producer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", notEmpty(config().getString("bootstrap.server"), "bootstrap.server not set"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");
        props.put("request.required.acks", "1");
        props.put("batch.size", "0");

        producer = new KafkaProducer<>(props);

        MessageConsumer<JsonObject> outgoing = vertx.eventBus().<JsonObject>localConsumer(ADDR_EVENTSTORE_WRITE);
        outgoing.handler(msg ->
                        producer.send
                                (new ProducerRecord<>(
                                                msg.body().getString(TOPIC), msg.body().getString(EVENT)),
                                        (result, exception) -> {
                                            if(exception == null)
                                                msg.reply("Topic:"+result.topic()+" Partition"+result.partition()+" Offset:"+result.offset());
                                            else
                                                msg.fail(1, "Failed writing with exception "+exception.getMessage());
                                        })
        );

        outgoing.completionHandler(complete -> startFuture.complete());

    }

    @Override
    public void stop() throws Exception {
        producer.close();
    }
}
