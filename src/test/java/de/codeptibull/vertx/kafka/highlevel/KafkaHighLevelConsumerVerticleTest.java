package de.codeptibull.vertx.kafka.highlevel;

import de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.test.core.VertxTestBase;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static de.codeptibull.vertx.kafka.highlevel.KafkaHighLevelConsumerVerticle.*;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.ADDR_EVENTSTORE_WRITE;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.CONFIG_KAFKA_HOST;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.EVENT;
import static java.util.stream.IntStream.range;

/**
 * Created by jmader on 25.02.15.
 */
public class KafkaHighLevelConsumerVerticleTest extends VertxTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaHighLevelConsumerVerticleTest.class);

    private int brokerId = 0;

    private ZkClient zkClient;
    private EmbeddedZookeeper zkServer;
    private KafkaServer kafkaServer;

    public static final String TEST_TOPIC = "testTopic";

    @Before
    public void setUpTest() throws Exception{

        String zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        vertx.deployVerticle(KafkaWriterVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject().put(CONFIG_KAFKA_HOST, "127.0.0.1:" + port)));

        vertx.deployVerticle(KafkaHighLevelConsumerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject()
                                .put(ZOOKEEPER_CONNECT, zkServer.connectString())
                                .put(TOPIC, TEST_TOPIC)
                                .put(GROUP_ID, "testGroup" + TEST_TOPIC)
                ));
        waitUntil(() -> vertx.deploymentIDs().size() == 2);

    }

    @After
    public void tearDown() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
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
                            vertx.eventBus().send(ADDR_EVENTSTORE_WRITE, new JsonObject().put(TOPIC, TEST_TOPIC).put(EVENT, "" + val));
                        })
        );

        await();
    }

}
