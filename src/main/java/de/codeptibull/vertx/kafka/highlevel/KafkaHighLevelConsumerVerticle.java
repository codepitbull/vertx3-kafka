package de.codeptibull.vertx.kafka.highlevel;

import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Context;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.commons.lang3.Validate.notEmpty;

/**
 * Kafka Consumer built ontop the High Level Consumer API.<br/>
 * Required params in config:
 * - zookeeper.connect <br/>
 * - group.id <br/>
 * Option params in config and their default values:
 * address => kafka-(group.id) <br/>
 * zookeeper.session.timeout.ms => 400 <br/>
 * zookeeper.sync.time.ms => 200 <br/>
 * auto.commit.interval.ms => 1000 <br/>
 * auto.offset.reset => smallest </br>
 */
public class KafkaHighLevelConsumerVerticle extends AbstractVerticle {

    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String GROUP_ID = "group.id";
    public static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    public static final String ZOOKEEPER_SYNC_TIME_MS = "zookeeper.sync.time.ms";
    public static final String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
    public static final String TOPIC = "topic";
    public static final String ADDRESS = "address";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    private Thread consumerThread;

    @Override
    public void start() throws Exception {
        String topic = notEmpty(config().getString(TOPIC), "topic not set");
        String targetAddress = config().getString(ADDRESS, "kafka-" + topic);

        Context ctx = vertx.getOrCreateContext();
        Runnable consumerRunnable = () -> {
            ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                    createConsumerConfig(config()));
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            KafkaStream<byte[], byte[]> messageAndMetadatas = streams.get(0);

            messageAndMetadatas.forEach(msg -> {
                ctx.runOnContext(exe -> vertx.eventBus().send(targetAddress, msg.message()));
            });
            consumer.shutdown();
        };

        consumerThread = new Thread(consumerRunnable);
        consumerThread.start();
    }

    private ConsumerConfig createConsumerConfig(JsonObject config) {
        notEmpty(config().getString(ZOOKEEPER_CONNECT), "zookeper not set");
        notEmpty(config().getString(GROUP_ID), "consumerGroup not set");
        Properties props = new Properties();
        props.put(ZOOKEEPER_CONNECT, config.getString(ZOOKEEPER_CONNECT));
        props.put(GROUP_ID, config.getString(GROUP_ID));
        props.put(ZOOKEEPER_SESSION_TIMEOUT_MS, config.getInteger(ZOOKEEPER_SESSION_TIMEOUT_MS, 400).toString());
        props.put(ZOOKEEPER_SYNC_TIME_MS, config.getInteger(ZOOKEEPER_SYNC_TIME_MS, 200).toString());
        props.put(AUTO_COMMIT_INTERVAL_MS, config.getInteger(AUTO_COMMIT_INTERVAL_MS, 1000).toString());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(AUTO_OFFSET_RESET, config.getString(AUTO_OFFSET_RESET, "smallest"));
        return new ConsumerConfig(props);
    }

    @Override
    public void stop() throws Exception {
        consumerThread.interrupt();
        consumerThread.join();
    }
}
