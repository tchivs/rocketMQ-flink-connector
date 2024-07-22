package org.apache.rocketmq.flink.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.flink.common.RocketMQOptions;
import org.apache.rocketmq.flink.legacy.common.selector.MessageQueueSelector;
import org.apache.rocketmq.flink.sink.writer.context.RocketMQSinkContext;
import org.apache.rocketmq.flink.sink.writer.context.RocketMQSinkContextImpl;
import org.apache.rocketmq.flink.sink.writer.serializer.RocketMQSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RocketMQSinkWriter<IN>
        implements SinkWriter<IN>, StatefulSinkWriter<IN, RocketMQWriterBucketState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSinkWriter.class);
    private final Configuration configuration;
    private final transient DefaultMQProducer producer;


    // private RocketMQSinkContext rocketmqSinkContext;
    private final RocketMQSerializationSchema<IN> serializationSchema;
    private final WriterInitContext writerInitContext;
    private final MessageQueueSelector messageQueueSelector;
    private final Map<String, Message> messageMap = new ConcurrentHashMap<>();
    private final SinkWriterMetricGroup metricGroup;
    private String messageQueueSelectorArg;
    private Meter sinkInTps;
    private Meter outTps;
    private Meter outBps;

    public RocketMQSinkWriter(
            Configuration configuration,
            RocketMQSerializationSchema<IN> serializationSchema,
            WriterInitContext writerInitContext,
            MessageQueueSelector messageQueueSelector, String messageQueueSelectorArg
    ) {
        this.configuration = configuration;
        this.serializationSchema = serializationSchema;
        this.writerInitContext = writerInitContext;
        this.messageQueueSelector = messageQueueSelector;
        this.messageQueueSelectorArg = messageQueueSelectorArg;
        producer = this.initProducer();
        this.metricGroup = writerInitContext.metricGroup();
        //注册一个度量指标
        this.metricGroup.getIOMetricGroup().getNumBytesOutCounter().inc();

    }

    private DefaultMQProducer initProducer() {
        DefaultMQProducer p = null;
        try {
            String groupId = configuration.get(RocketMQOptions.PRODUCER_GROUP);
            String endPoints = configuration.get(RocketMQOptions.NAME_SERVER_ADDRESS);
            Preconditions.checkNotNull(endPoints, "endPoints is empty in rocketmq.client-conf");
            String accessKey = configuration.get(RocketMQOptions.OPTIONAL_ACCESS_KEY);
            String secretKey = configuration.get(RocketMQOptions.OPTIONAL_SECRET_KEY);
            if (!StringUtils.isNullOrWhitespaceOnly(accessKey)
                    && !StringUtils.isNullOrWhitespaceOnly(secretKey)) {
                AclClientRPCHook aclClientRpcHook =
                        new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
                p = new TransactionMQProducer(groupId, aclClientRpcHook);
            } else {
                p = new TransactionMQProducer(groupId);
            }
            p.setNamesrvAddr(endPoints);
            p.setVipChannelEnabled(false);
            p.setInstanceName(
                    String.join(
                            "#",
                            ManagementFactory.getRuntimeMXBean().getName(),
                            groupId,
                            UUID.randomUUID().toString()));
            int corePoolSize = configuration.get(RocketMQOptions.EXECUTOR_NUM);
//            p.setExecutorService(
//                    new ThreadPoolExecutor(
//                            corePoolSize,
//                            corePoolSize,
//                            100,
//                            TimeUnit.SECONDS,
//                            new ArrayBlockingQueue<>(2000),
//                            r -> {
//                                Thread thread = new Thread(r);
//                                thread.setName(groupId);
//                                return thread;
//                            }));

            p.start();
        } catch (MQClientException e) {
            LOG.error("RocketMQ producer in flink sink writer init failed", e);
            throw new RuntimeException(e);
        }
        return p;
    }


    @Override
    public void write(IN element, Context context) throws IOException {
        try {
            Message message =
                    serializationSchema.serialize(
                            element, this.writerInitContext, System.currentTimeMillis());
            write(message);

        } catch (Exception e) {
            LOG.error("Send message error", e);
            throw new IOException(e);
        }
    }

    final SendCallback sendCallback = new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
            metricGroup.getNumRecordsSendCounter().inc();

        }

        @Override
        public void onException(Throwable throwable) {
            metricGroup.getNumRecordsSendErrorsCounter().inc();
        }
    };

    void write(Message message) {
        try {
            if (messageQueueSelector == null) {
                producer.send(message, sendCallback);
            } else {
                Object arg =
                        StringUtils.isNullOrWhitespaceOnly(messageQueueSelectorArg)
                                ? null
                                : message.getProperty(messageQueueSelectorArg);
                producer.send(message, this.messageQueueSelector, arg, sendCallback);
            }
            metricGroup.getNumBytesSendCounter().inc(message.getBody().length);
        } catch (Exception e) {
            LOG.error("Send message error", e);
            throw new RuntimeException();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // rocketmq client send message to server immediately, no need flush here
    }


    @Override
    public void close() throws Exception {
        producer.shutdown();
        LOG.info("Shutdown RocketMQ Producer!!!");
    }

    @Override
    public List<RocketMQWriterBucketState> snapshotState(long checkpointId) throws IOException {
        List<RocketMQWriterBucketState> state = new ArrayList<>();
        // 将当前待处理消息保存到状态中
        List<Message> pendingMessages = new ArrayList<>(messageMap.values());
        // 创建新的状态对象，假设 currentOffset 是我们需要记录的偏移量
        RocketMQWriterBucketState bucketState = new RocketMQWriterBucketState(0, checkpointId, pendingMessages);
        state.add(bucketState);
        LOG.info("Snapshot state with checkpoint id: {}", checkpointId);
        return state;
    }

    public void initializeState(Collection<RocketMQWriterBucketState> states) {
        if (states.isEmpty()) {
            return;
        }
        for (RocketMQWriterBucketState state : states) {
            state.getMessages().forEach(this::write);
            LOG.info("Restoring state from checkpoint id: {}, offset: {}", state.getLastCheckpointId(), state.getCurrentOffset());
        }
        LOG.info("Initialized state with states: {}", states.size());
    }
}
