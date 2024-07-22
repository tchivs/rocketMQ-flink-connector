package org.apache.rocketmq.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.flink.legacy.common.selector.MessageQueueSelector;
import org.apache.rocketmq.flink.sink.writer.RocketMQSinkWriter;
import org.apache.rocketmq.flink.sink.writer.RocketMQWriterBucketState;
import org.apache.rocketmq.flink.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.connector.sink2.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class RocketMQSink<IN> implements Sink<IN>,
        SupportsWriterState<IN, RocketMQWriterBucketState>,
        SupportsWriterState.WithCompatibleState,
        SupportsConcurrentExecutionAttempts {
    private final Configuration configuration;
    private final RocketMQSerializationSchema<IN> serializationSchema;
    private final MessageQueueSelector messageQueueSelector;
    private final String messageQueueSelectorArg;

    public RocketMQSink(Configuration configuration,
                        RocketMQSerializationSchema<IN> serializationSchema,
                        MessageQueueSelector messageQueueSelector,
                        String messageQueueSelectorArg
    ) {
        this.configuration = configuration;
        this.serializationSchema = serializationSchema;
        this.messageQueueSelector = messageQueueSelector;
        this.messageQueueSelectorArg = messageQueueSelectorArg;
    }

    /**
     * Create a {@link RocketMQSinkBuilder} to construct a new {@link RocketMQSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link RocketMQSinkBuilder}
     */
    public static <IN> RocketMQSinkBuilder<IN> builder() {
        return new RocketMQSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext initContext) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }


    @Override
    public StatefulSinkWriter<IN, RocketMQWriterBucketState> restoreWriter(WriterInitContext writerInitContext, Collection<RocketMQWriterBucketState> collection) throws IOException {
        RocketMQSinkWriter<IN> writer = null;
        writer = new RocketMQSinkWriter<>(this.configuration, serializationSchema, writerInitContext, messageQueueSelector, messageQueueSelectorArg);
        writer.initializeState(collection);
        return writer;
    }

    @Override
    public SimpleVersionedSerializer<RocketMQWriterBucketState> getWriterStateSerializer() {
        return new SimpleVersionedSerializer<RocketMQWriterBucketState>() {
            private static final int VERSION = 1;
            protected final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public int getVersion() {
                return VERSION;
            }

            @Override
            public byte[] serialize(RocketMQWriterBucketState rocketMQWriterBucketState) throws IOException {
                return objectMapper.writeValueAsBytes(rocketMQWriterBucketState);
            }

            @Override
            public RocketMQWriterBucketState deserialize(int i, byte[] bytes) throws IOException {
                return objectMapper.readValue(bytes, RocketMQWriterBucketState.class);
            }
        };
    }

    @Override
    public Collection<String> getCompatibleWriterStateNames() {
        return Collections.emptyList();
    }
}
