package de.codeptibull.vertx.kafka.simple;

import de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static de.codeptibull.vertx.kafka.highlevel.KafkaHighLevelConsumerVerticle.TOPIC;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.ADDR_EVENTSTORE_WRITE;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.CONFIG_KAFKA_HOST;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.EVENT;
import static java.util.stream.IntStream.range;

/**
 * Created by jmader on 28.02.15.
 */
public class KafkaSimpleConsumerTest extends VertxTestBase {
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
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), TEST_TOPIC, 0, 500);

        TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, TEST_TOPIC, 0, 500, scala.Option.apply(null), scala.Option.apply(null));

        vertx.deployVerticle(KafkaWriterVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject().put(CONFIG_KAFKA_HOST, "127.0.0.1:" + port)));

        waitUntil(() -> vertx.deploymentIDs().size() == 1);

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

        range(0, 11).forEach(val -> {
            vertx.eventBus().send(ADDR_EVENTSTORE_WRITE, new JsonObject().put(TOPIC, TOPIC).put(EVENT, "" + val));
        });
        //TODO: why do I have to do this?? I hit every wait-method available ...
        Thread.sleep(1000);
        KafkaSimpleConsumer consumer = new KafkaSimpleConsumer(new SimpleConsumerProperties.Builder()
                .partition(0)
                .port(port)
                .topic(TOPIC)
                .addBroker("127.0.0.1")
                .stopOnEmptyToppic(true)
                .build()
                , msg -> {
                    if(10 < counter.incrementAndGet()) testComplete();
                }
        );
        consumer.fetch();
        consumer.close();

        await();
    }

    @Test
    public void testProduceAndConsumeWithOffset() throws Exception{
        range(0, 11).forEach(val -> {
            vertx.eventBus().send(ADDR_EVENTSTORE_WRITE, new JsonObject().put(TOPIC, TOPIC).put(EVENT, "" + val));
        });
        //TODO: why do I have to do this?? I hit every wait-method available ...
        Thread.sleep(1000);

        KafkaSimpleConsumer consumer = new KafkaSimpleConsumer(new SimpleConsumerProperties.Builder()
                .partition(0)
                .port(port)
                .topic(TOPIC)
                .addBroker("127.0.0.1")
                .offset(4)
                .stopOnEmptyToppic(true)
                .build()
                , msg -> {
                    if(new StringDeserializer().deserialize(null, msg).equals("4")) testComplete();
                }
        );
        consumer.fetch();
        consumer.close();

        await();
    }
}
