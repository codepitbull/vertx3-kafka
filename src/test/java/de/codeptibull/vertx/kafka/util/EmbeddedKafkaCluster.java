package de.codeptibull.vertx.kafka.util;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

/**
 * https://gist.github.com/vmarcinko/e4e58910bcb77dac16e9
 */
public class EmbeddedKafkaCluster {
    private final List<Integer> ports;
    private final String zkConnection;
    private final Properties baseProperties;
 
    private final String brokerList;
 
    private final List<KafkaServer> brokers;
    private final List<File> logDirs;
 
    public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties, List<Integer> ports) {
        this.zkConnection = zkConnection;
        this.ports = resolvePorts(ports);
        this.baseProperties = baseProperties;
 
        this.brokers = new ArrayList<>();
        this.logDirs = new ArrayList<>();
 
        this.brokerList = constructBrokerList(this.ports);
    }
 
    private List<Integer> resolvePorts(List<Integer> ports) {
        return ports.stream().map(port -> resolvePort(port)).collect(Collectors.toList());
    }
 
    private int resolvePort(int port) {
        if (port == -1) return TestUtils.getAvailablePort();
        return port;
    }
 
    private String constructBrokerList(List<Integer> ports) {
        StringBuilder sb = new StringBuilder();
        ports.forEach(port -> {
            if (sb.length() > 0) sb.append(",");
            sb.append("localhost:").append(port);
        });
        return sb.toString();
    }
 
    public void startup() {
        range(0, ports.size()).forEach(i -> {
            Integer port = ports.get(i);
            File logDir = TestUtils.constructTempDir("kafka-local");

            Properties properties = new Properties();
            properties.putAll(baseProperties);
            properties.setProperty("zookeeper.connect", zkConnection);
            properties.setProperty("broker.id", String.valueOf(i + 1));
            properties.setProperty("host.name", "localhost");
            properties.setProperty("port", Integer.toString(port));
            properties.setProperty("log.dir", logDir.getAbsolutePath());
            properties.setProperty("log.flush.interval.messages", String.valueOf(1));

            KafkaServer broker = startBroker(properties);

            brokers.add(broker);
            logDirs.add(logDir);
        });
    }
 
 
    private KafkaServer startBroker(Properties props) {
        KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime());
        server.startup();
        return server;
    }
 
    public Properties getProps() {
        Properties props = new Properties();
        props.putAll(baseProperties);
        props.put("metadata.broker.list", brokerList);
        props.put("zookeeper.connect", zkConnection);
        return props;
    }
 
    public String getBrokerList() {
        return brokerList;
    }
 
    public List<Integer> getPorts() {
        return ports;
    }
 
    public String getZkConnection() {
        return zkConnection;
    }
 
    public void shutdown() {
        brokers.forEach(broker -> broker.shutdown());
        logDirs.forEach(logDir -> TestUtils.deleteFile(logDir));
    }
 
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedKafkaCluster{");
        sb.append("brokerList='").append(brokerList).append('\'');
        sb.append('}');
        return sb.toString();
    }
}