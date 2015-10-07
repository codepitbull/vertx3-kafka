package de.codeptibull.vertx.kafka.simple;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.*;

import static de.codeptibull.vertx.kafka.simple.ResultEnum.*;

/**
 * Refactored version from the Kafka documentation.
 */
public class KafkaSimpleConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSimpleConsumer.class);

    private SimpleConsumerProperties cd;
    private Deque<MessageAndOffset> pending = new ArrayDeque<>();
    private SimpleConsumer consumer;
    private String leadBroker = null;
    private MutableLong currentOffset = new MutableLong(0);

    public KafkaSimpleConsumer(SimpleConsumerProperties cd) {
        this.cd = cd;
        Optional<PartitionMetadata> metadata = findLeader(cd);
        if (!metadata.isPresent()) {
            throw new RuntimeException("Can't find metadata for Topic and Partition. Exiting");
        }
        if (metadata.get().leader() == null || metadata.get().leader().host() == null) {
            throw new RuntimeException("Can't find Leader for Topic and Partition. Exiting");
        }
        leadBroker = metadata.get().leader().host();

        consumer = new SimpleConsumer(leadBroker, cd.getPort(), cd.getSoTimeout(), cd.getBufferSize(), cd.getClientName());
        if(cd.getOffset() == -1)
            currentOffset.setValue(getLastOffset(consumer, cd, kafka.api.OffsetRequest.EarliestTime()));
        else
            currentOffset.setValue(cd.getOffset());
    }

    public void close() {
        if (consumer != null) consumer.close();
    }

    public Pair<ResultEnum,byte[]> fetch() {
        if(!pending.isEmpty()) {
            return Pair.of(OK, getPendingMessage(pending));
            //TODO: and here the commit-magic happens
        }

        FetchRequest req = new FetchRequestBuilder()
                .clientId(consumer.clientId())
                .addFetch(cd.getTopic(), cd.getPartition(), currentOffset.getValue(), cd.getFetchSize())
                .build();
        FetchResponse fetchResponse = consumer.fetch(req);

        if (fetchResponse.hasError()) {
            LOG.error("Error while fetching " + fetchResponse.errorCode(cd.getTopic(), cd.getPartition()));
            return Pair.of(ERROR, new byte[]{});
        }

        if(!processRepsonseAndReturnIfSomethingWasRead(currentOffset, cd, fetchResponse, pending) && cd.isStopOnEmptyTopic())
            return Pair.of(TOPIC_EMPTY, new byte[]{});
        else {
            return Pair.of(OK, getPendingMessage(pending));
        }
    }

    private static byte[] getPendingMessage(Deque<MessageAndOffset> pending) {
        MessageAndOffset messageAndOffset = pending.poll();
        if(messageAndOffset != null) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            return bytes;
        }
        return new byte[]{};
    }

    private static boolean processRepsonseAndReturnIfSomethingWasRead(MutableLong lastReadOffset, SimpleConsumerProperties cd, FetchResponse fetchResponse, Deque<MessageAndOffset> pending) {
        boolean read = false;

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(cd.getTopic(), cd.getPartition())) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < lastReadOffset.getValue()) {
                LOG.warn("Found an old offset: " + currentOffset + " Expecting: " + lastReadOffset.getValue());
                continue;
            }

            pending.add(messageAndOffset);
            lastReadOffset.setValue(messageAndOffset.nextOffset());
            read = true;
        }

        return read;
    }

    private static long getLastOffset(SimpleConsumer consumer, SimpleConsumerProperties cd, long whichTime) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(new TopicAndPartition(cd.getTopic(), cd.getPartition())
                , new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetResponse response = consumer.getOffsetsBefore(new OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId()));

        if (response.hasError()) {
            LOG.warn("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(cd.getTopic(), cd.getPartition()));
            return 0;
        }
        long[] offsets = response.offsets(cd.getTopic(), cd.getPartition());
        return offsets[0];
    }

    private static Optional<PartitionMetadata> findLeader(SimpleConsumerProperties cd) {
        for (String seed : cd.getSeedBrokers()) {
            SimpleConsumer consumer = new SimpleConsumer(seed, cd.getPort(), cd.getSoTimeout(), cd.getBufferSize(), "leaderLookup");
            Optional<PartitionMetadata> first = consumer.send(new TopicMetadataRequest(Collections.singletonList(cd.getTopic())))
                    .topicsMetadata().stream()
                    .map(TopicMetadata::partitionsMetadata)
                    .flatMap(metadatas -> metadatas.stream())
                    .filter(part -> part.partitionId() == cd.getPartition())
                    .findFirst();
            if (first.isPresent()) {
                PartitionMetadata returnMetaData = first.get();
                cd.clearSeedBrokers();
                returnMetaData.replicas().forEach(replica -> cd.addSeedBroker(replica.host()));
                return Optional.of(returnMetaData);
            }
        }
        return Optional.empty();
    }


}