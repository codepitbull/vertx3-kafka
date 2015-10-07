package de.codeptibull.vertx.kafka.simple;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;

import static de.codeptibull.vertx.kafka.simple.ResultEnum.OK;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSimpleConsumerVerticle.class);

    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String PORT = "port";
    public static final String BROKERS = "brokers";
    public static final String OFFSET = "offset";
    public static final String ADDR_KAFKA_CONSUMER_BASE = "kafka.consumer.";
    public static final String CMD_RECEIVE_ONE = "receive";
    private KafkaSimpleConsumer consumer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Context context = vertx.getOrCreateContext();
        SimpleConsumerProperties props = new SimpleConsumerProperties.Builder()
                .partition(notNull(config().getInteger(PARTITION), "partition not provided"))
                .port(notNull(config().getInteger(PORT), "port not provided"))
                .topic(notEmpty(config().getString(TOPIC), "topic not provided"))
                .addBrokers(Arrays.asList(notEmpty(config().getString(BROKERS), "brokers not provided").split(",")))
                .offset(config().getInteger(OFFSET, -1))
                .build();

        consumer = new KafkaSimpleConsumer(props);

        vertx.eventBus().<String>consumer(ADDR_KAFKA_CONSUMER_BASE +props.getTopic(), msg -> {
            //TODO: add start/stop messages
            if(CMD_RECEIVE_ONE.equals(msg.body())) {
                vertx.<Pair<ResultEnum, byte[]>>executeBlocking(
                        event -> event.complete(consumer.fetch()),
                        result -> {
                            if (result.succeeded()) {
                                if (OK.equals(result.result().getLeft()))
                                    context.runOnContext(v -> msg.reply(result.result().getRight()));
                            } else {
                                LOGGER.error("Failed fetching next entry", result.cause());
                            }
                        }
                );
            }
        }).completionHandler(done -> startFuture.complete());
    }

    @Override
    public void stop() throws Exception {
        consumer.close();
    }
}
