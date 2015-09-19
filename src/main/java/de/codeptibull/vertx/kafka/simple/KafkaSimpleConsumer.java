package de.codeptibull.vertx.kafka.simple;

import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.commons.lang3.mutable.MutableLong;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Refactored version from the Kafka documentation.
 */
public class KafkaSimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumer.class);

    private SimpleConsumerProperties cd;
    private Deque<MessageAndOffset> pending = new ArrayDeque<>();
    private SimpleConsumer consumer;
    private Handler<byte[]> msgHandler;
    private String leadBroker = null;
    private MutableLong currentOffset = new MutableLong(0);

    public KafkaSimpleConsumer(SimpleConsumerProperties cd, Handler<byte[]> msgHandler) {
        this.msgHandler = msgHandler;
        this.cd = cd;
        Optional<PartitionMetadata> metadata = findLeader(cd);
        if (!metadata.isPresent()) {
            throw new RuntimeException("Can't find metadata for Topic and Partition. Exiting");
        }
        if (metadata.get().leader() == null) {
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

    public ResultEnum fetch() {
        int numErrors = 0;
        while (true) {
            while(!pending.isEmpty()) {
                MessageAndOffset messageAndOffset = pending.poll();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                msgHandler.handle(bytes);

                //TODO: and here the commit-magic happens

            }

            if (leadBroker == null) {
                Optional<String> newLeader = findNewLeader(leadBroker, cd);
                if(newLeader.isPresent()) leadBroker = newLeader.get();
                else return ResultEnum.MISSING_LEADER;
            }

            if (consumer == null) consumer = new SimpleConsumer(leadBroker, cd.getPort(), cd.getSoTimeout(), cd.getBufferSize(), cd.getClientName());

            FetchRequest req = new FetchRequestBuilder()
                    .clientId(consumer.clientId())
                    .addFetch(cd.getTopic(), cd.getPartition(), currentOffset.getValue(), cd.getFetchSize())
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(cd.getTopic(), cd.getPartition());
                logger.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;

                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    currentOffset.setValue(getLastOffset(consumer, cd, kafka.api.OffsetRequest.LatestTime()));
                    continue;
                }
                consumer.close();
                consumer = null;

            }
            numErrors = 0;

            if(cd.isStopOnEmptyTopic() && !processRepsonseAndReturnIfTopicIsEmpty(currentOffset, cd, fetchResponse, pending)) return ResultEnum.TOPIC_EMPTY;
            else
                processRepsonseAndReturnIfTopicIsEmpty(currentOffset, cd, fetchResponse, pending);

        }
        return ResultEnum.ERROR;
    }

    private static boolean processRepsonseAndReturnIfTopicIsEmpty(MutableLong lastReadOffset, SimpleConsumerProperties cd, FetchResponse fetchResponse, Deque<MessageAndOffset> pending) {
        boolean read = false;

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(cd.getTopic(), cd.getPartition())) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < lastReadOffset.getValue()) {
                logger.warn("Found an old offset: " + currentOffset + " Expecting: " + lastReadOffset.getValue());
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
            logger.warn("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(cd.getTopic(), cd.getPartition()));
            return 0;
        }
        long[] offsets = response.offsets(cd.getTopic(), cd.getPartition());
        return offsets[0];
    }

    private static Optional<String> findNewLeader(String oldLeader, SimpleConsumerProperties cd) {
        Optional<PartitionMetadata> metadata = findLeader(cd);
            if (!metadata.isPresent() || metadata.get().leader() == null || oldLeader.equalsIgnoreCase(metadata.get().leader().host())) return Optional.empty();
        return Optional.of(metadata.get().leader().host());
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