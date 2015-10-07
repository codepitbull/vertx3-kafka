package de.codeptibull.vertx.kafka.writer;

import io.vertx.core.Handler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;
import java.util.Properties;

/**
 * Created by jmader on 19.09.15.
 */
public class KafkaWriter implements Closeable{
    private KafkaProducer<String, String> producer;

    public KafkaWriter(String hosts) {
        Properties props = new Properties();
        props.put("bootstrap.servers", hosts);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");
        props.put("request.required.acks", "1");
        props.put("batch.size", "0");

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.close();
    }

    public void write(String topic, String event, Handler<RecordMetadata> success, Handler<Throwable> failure) {
        producer.send
                (new ProducerRecord<>(topic, event),
                        (result, exception) -> {
                            if(exception == null)
                                success.handle(result);
                            else
                                failure.handle(exception);
                        });
    }
}
