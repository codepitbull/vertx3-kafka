package de.codeptibull.vertx.kafka.simple;

import de.codeptibull.vertx.kafka.util.KafkaProducerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    private int brokerId = 0;

    private ZkClient zkClient;
    private kafka.zk.EmbeddedZookeeper zkServer;
    private KafkaServer kafkaServer;
    private int port = 0;

    public static final String TEST_TOPIC = "testTopic";

    @Before
    public void setUpTest() throws Exception{

        String zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new kafka.zk.EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        AdminUtils.createTopic(zkClient, TEST_TOPIC, 1, 1, new Properties());
        List<KafkaServer> servers = new ArrayList<>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), TEST_TOPIC, 0, 5000);

        vertx.deployVerticle(KafkaProducerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject().put("bootstrap.server", "127.0.0.1:" + port)));

        waitUntil(() -> vertx.deployments().size() == 1);

    }

    @After
    public void tearDown() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    @Test
    public void testProduceAndConsume() throws Exception{

        AtomicInteger counter = new AtomicInteger(0);

        vertx.eventBus().<byte[]>consumer("simple", msg -> {
            if (counter.incrementAndGet() == 3000)
                testComplete();
        });

        range(0, 4000).forEach(val -> {
            vertx.eventBus().send("outgoing", new JsonObject().put("topic", TEST_TOPIC).put("msg", "" + val));
        });

        //TODO: why do have to do this?? I hit every wait-method available ...
        Thread.sleep(1000);

        vertx.deployVerticle(KafkaSimpleConsumerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject()
                                .put(PARTITION, 0)
                                .put(PORT, port)
                                .put(TOPIC, TEST_TOPIC)
                                .put(BROKERS, "127.0.0.1")
                                .put(LISTEN_ADDRESS, "receive")
                                .put(TARGET_ADDRESS, "simple")
                ), comp -> {
//                    vertx.eventBus().send("receive", 10);
//                    vertx.eventBus().send("receive", 10);
                    vertx.eventBus().send("receive", Integer.MAX_VALUE);
                });

        await();
    }
}
