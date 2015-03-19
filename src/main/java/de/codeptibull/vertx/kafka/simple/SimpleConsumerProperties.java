package de.codeptibull.vertx.kafka.simple;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notEmpty;

/**
 * Created by jmader on 19.02.15.
 */
public class SimpleConsumerProperties {
    private final String clientName;
    private final String topic;
    private final int partition;
    private final int port;
    private final int soTimeout;
    private final int bufferSize;
    private final int fetchSize;
    private final long offset;
    private final boolean stopOnEmptyToppic;

    private final List<String> seedBrokers;
    private final List<String> roSeedBrokers;

    private SimpleConsumerProperties(String clientName, String topic, int partition, int port, List<String> seedBrokers, int soTimeout, int bufferSize, int fetchSize, long offset, boolean stopOnEmptyToppic) {
        this.clientName = clientName;
        this.topic = topic;
        this.partition = partition;
        this.port = port;
        this.seedBrokers = seedBrokers;
        this.bufferSize = bufferSize;
        this.soTimeout = soTimeout;
        this.fetchSize = fetchSize;
        this.offset = offset;
        roSeedBrokers = Collections.unmodifiableList(seedBrokers);
        this.stopOnEmptyToppic = stopOnEmptyToppic;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public String getClientName() {
        return clientName;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public int getPort() {
        return port;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public List<String> getSeedBrokers() {
        return roSeedBrokers;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public long getOffset() {
        return offset;
    }

    public boolean isStopOnEmptyToppic() {
        return stopOnEmptyToppic;
    }

    public SimpleConsumerProperties clearSeedBrokers() {
        seedBrokers.clear();
        return this;
    }

    public SimpleConsumerProperties addSeedBroker(String broker) {
        seedBrokers.add(broker);
        return this;
    }


    public static class Builder {
        private String topic = null;
        private int partition = -1;
        private int port = -1;
        private int soTimeout = 100000;
        private int bufferSize = 64 * 1024;
        private long offset = -1;
        private boolean stopOnEmptyToppic = false;
        int fetchSize = 10000;
        private List<String> seedBrokers = new ArrayList<>();

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder soTimeout(int soTimeout) {
            this.soTimeout = soTimeout;
            return this;
        }

        public Builder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder addBroker(String broker) {
            seedBrokers.add(broker);
            return this;
        }

        public Builder addBrokers(Collection<String> brokers) {
            seedBrokers.addAll(brokers);
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder stopOnEmptyToppic(boolean stopOnEmptyToppic) {
            this.stopOnEmptyToppic = stopOnEmptyToppic;
            return this;
        }

        public SimpleConsumerProperties build() {
            notEmpty("topic must not be empty", topic);
            isTrue(!seedBrokers.isEmpty(), "at least one seed broker must be provided");
            isTrue(partition > -1, "partition must be set");
            isTrue(port > -1, "partition must be set");
            isTrue(fetchSize > 0, "fetchSize has to be >0");
            isTrue(soTimeout > 0, "soTimeout has to be >0");
            isTrue(bufferSize > 0, "bufferSize has to be >0");

            return new SimpleConsumerProperties("Client_" + topic + "_" + partition, topic, partition, port, seedBrokers, soTimeout, bufferSize, fetchSize, offset, stopOnEmptyToppic);
        }
    }
}
