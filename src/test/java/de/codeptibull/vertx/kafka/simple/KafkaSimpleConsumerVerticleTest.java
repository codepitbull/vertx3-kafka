package de.codeptibull.vertx.kafka.simple;

import de.codeptibull.vertx.kafka.util.EmbeddedKafkaCluster;
import de.codeptibull.vertx.kafka.util.EmbeddedZookeeper;
import de.codeptibull.vertx.kafka.util.KafkaProducerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static de.codeptibull.vertx.kafka.simple.KafkaSimpleConsumerVerticle.*;
import static java.util.stream.IntStream.range;

/**
 * Created by jmader on 28.02.15.
 */
public class KafkaSimpleConsumerVerticleTest extends VertxTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumerVerticleTest.class);

    private EmbeddedZookeeper embeddedZookeeper;
    private EmbeddedKafkaCluster embeddedKafkaCluster;

    public static final String TEST_TOPIC = "testTopic";

    @Before
    public void setUpTest() throws Exception{
        embeddedZookeeper = new EmbeddedZookeeper(-1);
        List<Integer> kafkaPorts = new ArrayList<Integer>();
        kafkaPorts.add(-1);
        embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
        embeddedZookeeper.startup();
        logger.info("### Embedded Zookeeper connection: " + embeddedZookeeper.getConnection());
        embeddedKafkaCluster.startup();
        logger.info("### Embedded Kafka cluster broker list: " + embeddedKafkaCluster.getBrokerList());

        vertx.deployVerticle(KafkaProducerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject().put("bootstrap.server", embeddedKafkaCluster.getBrokerList())));

        waitUntil(() -> vertx.deploymentIDs().size() == 1);

    }

    @After
    public void tearDown() {
        embeddedKafkaCluster.shutdown();
        embeddedZookeeper.shutdown();
    }

    @Test
    public void testProduceAndConsume() throws Exception{

        AtomicInteger counter = new AtomicInteger(0);

        vertx.eventBus().<byte[]>consumer("simple", msg -> {
            if(counter.incrementAndGet() == 40)
                testComplete();
        });

        range(0, 40).forEach(val -> {
            vertx.eventBus().send("outgoing", new JsonObject().put("topic", TEST_TOPIC).put("msg", "" + val));
        });

        Thread.sleep(1000);

        vertx.deployVerticle(KafkaSimpleConsumerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject()
                                .put(PARTITION, 0)
                                .put(PORT, embeddedKafkaCluster.getPorts().get(0))
                                .put(TOPIC, TEST_TOPIC)
                                .put(BROKERS, "127.0.0.1")
                                .put(LISTEN_ADDRESS, "receive")
                                .put(TARGET_ADDRESS, "simple")
                ), comp -> {
                    vertx.eventBus().send("receive", 10);
                    vertx.eventBus().send("receive", 10);
                });

        await();
    }
}
