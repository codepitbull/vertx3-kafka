package de.codeptibull.vertx.kafka.highlevel;

import de.codeptibull.vertx.kafka.util.EmbeddedKafkaCluster;
import de.codeptibull.vertx.kafka.util.EmbeddedZookeeper;
import de.codeptibull.vertx.kafka.util.KafkaProducerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static de.codeptibull.vertx.kafka.highlevel.KafkaHighLevelConsumerVerticle.*;
import static java.util.stream.IntStream.range;

/**
 * Created by jmader on 25.02.15.
 */
public class KafkaHighLevelConsumerVerticleTest extends VertxTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaHighLevelConsumerVerticleTest.class);

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

        vertx.deployVerticle(KafkaHighLevelConsumerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject()
                                .put(ZOOKEEPER_CONNECT, embeddedZookeeper.getConnection())
                                .put(TOPIC, TEST_TOPIC)
                                .put(GROUP_ID, "testGroup" + TEST_TOPIC)
                ));
        waitUntil(() -> vertx.deploymentIDs().size() == 2);

    }

    @After
    public void tearDown() {
        embeddedKafkaCluster.shutdown();
        embeddedZookeeper.shutdown();
    }

    @Test
    public void testProduceAndConsume() {
        MutableInt counter = new MutableInt(0);
        long startTime = System.currentTimeMillis();
        StringDeserializer stringDeserializer = new StringDeserializer();
        vertx.eventBus().<byte[]>consumer("kafka-" + TEST_TOPIC, msg -> {
            if ("499".equals(stringDeserializer.deserialize("", msg.body()))) {
                logger.info("# messages received: " + counter.getValue() + " average processing time / message: " + (System.currentTimeMillis() - startTime) / 500f);
                testComplete();
            }
        }).completionHandler(complete ->
                        range(0, 500).forEach(val -> {
                            vertx.eventBus().send("outgoing", new JsonObject().put("topic", TEST_TOPIC).put("msg", "" + val));
                        })
        );

        await();
    }

}
