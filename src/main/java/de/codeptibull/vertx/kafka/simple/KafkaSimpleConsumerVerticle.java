package de.codeptibull.vertx.kafka.simple;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;

import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang3.Validate.notEmpty;
import static org.apache.commons.lang3.Validate.notNull;

/**
 * Config-Parameters: <br/>
 * listenAddress -> address on the eventBus this verticle is going to listen to for requests <br/>
 * targetAddress -> address this verticle is going to send results from kafka to <br/>
 * partition -> the partition the verticle is going to read from <br/>
 * topic -> kafka-topic the verticle is going to subscribe to<br/>
 * port -> port of Kafka-hosts <br/>
 * brokers -> comma separated list of kafka-hosts <br/>
 */
public class KafkaSimpleConsumerVerticle extends AbstractVerticle {

    public static final String LISTEN_ADDRESS = "listenAddress";
    public static final String TARGET_ADDRESS = "targetAddress";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String PORT = "port";
    public static final String BROKERS = "brokers";
    public static final String OFFSET = "offset";
    private KafkaSimpleConsumer consumer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        validateConfig();

        String listenAddress = config().getString(LISTEN_ADDRESS);
        String targetAddress = config().getString(TARGET_ADDRESS);
        String topic = config().getString(TOPIC);
        Integer partition = config().getInteger(PARTITION);
        Integer port = config().getInteger(PORT);
        Integer offset = config().getInteger(OFFSET, -1);
        List<String> brokers = Arrays.asList(config().getString(BROKERS).split(","));

        Context context = vertx.getOrCreateContext();

        consumer = new KafkaSimpleConsumer(new SimpleConsumerProperties.Builder()
                .partition(partition)
                .port(port)
                .topic(topic)
                .addBrokers(brokers)
                .offset(offset)
                .build()
                , msg -> context.runOnContext(t -> vertx.eventBus().send(targetAddress, msg))
        );

        vertx.eventBus().<Integer>consumer(listenAddress, msg -> {
            consumer.request(msg.body());
            vertx.<ResultEnum>executeBlocking(event ->
                    consumer.fetch(), null);
        }).completionHandler(done -> startFuture.complete());

    }

    private void validateConfig() {
        notEmpty(config().getString(LISTEN_ADDRESS), "listenAddress not provided");
        notEmpty(config().getString(TARGET_ADDRESS), "targetAddress not provided");
        notEmpty(config().getString(TOPIC), "topic not provided");
        notEmpty(config().getString(BROKERS), "brokers not provided");
        notNull(config().getInteger(PARTITION), "partition not provided");
        notNull(config().getInteger(PORT), "port not provided");
    }

    @Override
    public void stop() throws Exception {
        consumer.close();
    }
}
