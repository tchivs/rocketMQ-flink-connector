package org.apache.rocketmq.flink.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.rocketmq.flink.common.*;
import org.apache.rocketmq.flink.legacy.common.selector.MessageQueueSelector;
import org.apache.rocketmq.flink.sink.writer.serializer.RocketMQSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RocketMQSinkBuilder<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSinkBuilder.class);

    public static final RocketMQConfigValidator SINK_CONFIG_VALIDATOR =
            RocketMQConfigValidator.builder().build();
    private final RocketMQConfigBuilder configBuilder;
    private RocketMQSerializationSchema<IN> serializer;
    private MessageQueueSelector messageQueueSelector;
    private String messageQueueSelectorArg;

    public RocketMQSinkBuilder() {
        this.configBuilder = new RocketMQConfigBuilder();
    }

    /**
     * Configure the access point with which the SDK should communicate.
     *
     * @param endpoints address of service.
     * @return the client configuration builder instance.
     */
    public RocketMQSinkBuilder<IN> setEndpoints(String endpoints) {
        return this.setConfig(RocketMQOptions.NAME_SERVER_ADDRESS, endpoints);
    }

    public RocketMQSinkBuilder<IN> setMessageQueueSelector(MessageQueueSelector messageQueueSelector, String messageQueueSelectorArg) {
        this.messageQueueSelector = messageQueueSelector;
        this.messageQueueSelectorArg = messageQueueSelectorArg;
        return this;
    }

    /**
     * Sets the consumer group id of the RocketMQSource.
     *
     * @param groupId the group id of the RocketMQSource.
     * @return this RocketMQSourceBuilder.
     */
    public RocketMQSinkBuilder<IN> setGroupId(String groupId) {
        this.configBuilder.set(RocketMQOptions.PRODUCER_GROUP, groupId);
        return this;
    }

    /**
     * Set an arbitrary property for the RocketMQ source. The valid keys can be found in {@link
     * RocketMQOptions}.
     *
     * <p>Make sure the option could be set only once or with same value.
     *
     * @param key   the key of the property.
     * @param value the value of the property.
     * @return this RocketMQSourceBuilder.
     */
    public <T> RocketMQSinkBuilder<IN> setConfig(ConfigOption<T> key, T value) {
        configBuilder.set(key, value);
        return this;
    }

    /**
     * Set arbitrary properties for the RocketMQSink and RocketMQ Consumer. The valid keys can be
     * found in {@link RocketMQOptions} and {@link RocketMQOptions}.
     *
     * @param config the config to set for the RocketMQSink.
     * @return this RocketMQSinkBuilder.
     */
    public RocketMQSinkBuilder<IN> setConfig(Configuration config) {
        configBuilder.set(config);
        return this;
    }

    /**
     * Set arbitrary properties for the RocketMQSink and RocketMQ Consumer. The valid keys can be
     * found in {@link RocketMQOptions} and {@link RocketMQOptions}.
     *
     * @param properties the config properties to set for the RocketMQSink.
     * @return this RocketMQSinkBuilder.
     */
    public RocketMQSinkBuilder<IN> setProperties(Properties properties) {
        configBuilder.set(properties);
        return this;
    }

    /**
     * Sets the {@link RocketMQSerializationSchema} that transforms incoming records to {@link
     * org.apache.rocketmq.common.message.MessageExt}s.
     *
     * @param serializer serialize message
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setSerializer(RocketMQSerializationSchema<IN> serializer) {
        this.serializer = checkNotNull(serializer, "serializer is null");
        ClosureCleaner.clean(this.serializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    /**
     * Build the {@link RocketMQSink}.
     *
     * @return a RocketMQSource with the settings made for this builder.
     */
    public RocketMQSink<IN> build() {
        return new RocketMQSink<>(configBuilder.build(SINK_CONFIG_VALIDATOR),
                serializer, messageQueueSelector, messageQueueSelectorArg);
    }
}
