package de.codeptibull.vertx.kafka.simple;

import de.codeptibull.vertx.kafka.util.EmbeddedKafkaCluster;
import de.codeptibull.vertx.kafka.util.EmbeddedZookeeper;
import de.codeptibull.vertx.kafka.util.KafkaProducerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.IntStream.range;

/**
 * Created by jmader on 28.02.15.
 */
public class KafkaSimpleConsumerTest extends VertxTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumerTest.class);

    private EmbeddedZookeeper embeddedZookeeper;
    private EmbeddedKafkaCluster embeddedKafkaCluster;

    public static final String TOPIC = "testTopic";

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

        range(0, 11).forEach(val -> {
            vertx.eventBus().send("outgoing", new JsonObject().put("topic", TOPIC).put("msg", "" + val));
        });
        Thread.sleep(1000);
        KafkaSimpleConsumer consumer = new KafkaSimpleConsumer(new SimpleConsumerProperties.Builder()
                .partition(0)
                .port(embeddedKafkaCluster.getPorts().get(0))
                .topic(TOPIC)
                .addBroker("127.0.0.1")
                .build()
                , msg -> {
                    if(10 < counter.incrementAndGet()) testComplete();
                }
        );
        consumer.request(10);
        consumer.fetch();
        consumer.close();

        await();
    }

    @Test
    public void testProduceAndConsumeWithOffset() throws Exception{
        range(0, 11).forEach(val -> {
            vertx.eventBus().send("outgoing", new JsonObject().put("topic", TOPIC).put("msg", "" + val));
        });
        Thread.sleep(1000);
        KafkaSimpleConsumer consumer = new KafkaSimpleConsumer(new SimpleConsumerProperties.Builder()
                .partition(0)
                .port(embeddedKafkaCluster.getPorts().get(0))
                .topic(TOPIC)
                .addBroker("127.0.0.1")
                .offset(4)
                .build()
                , msg -> {
                    if(new StringDeserializer().deserialize(null, msg).equals("4")) testComplete();
                }
        );
        consumer.request(1);
        consumer.fetch();
        consumer.close();

        await();
    }
}
