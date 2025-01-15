package org.apache.rocketmq.flink.sink.writer.serializer;


import  org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import  org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.flink.sink.writer.context.RocketMQSinkContext;

import java.io.Serializable;

/**
 * The serialization schema for how to serialize records into RocketMQ. A serialization schema which
 * defines how to convert a value of type {@code T} to {@link MessageExt}.
 *
 * @param <T> the type of values being serialized
 */
@PublicEvolving
public interface RocketMQSerializationSchema<T> extends Serializable {
    final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object, WriterInitContext, Long)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context     Contextual information that can be used during initialization.
     * @param sinkContext runtime information i.e. partitions, subtaskId
     */
    default void open(
            SerializationSchema.InitializationContext context, RocketMQSinkContext sinkContext)
            throws Exception {
    }

    /**
     * Serializes given element and returns it as a {@link MessageExt}.
     *
     * @param element   element to be serialized
     * @param context   context to possibly determine target partition
     * @param timestamp timestamp
     * @return RocketMQ {@link MessageExt}
     */
    default Message serialize(T element, WriterInitContext context, Long timestamp) {
        try {
            String topic = getTopic();
            byte[] bytes = getBytes(element);
            String key = getMessageKey(element);
            String tag = getMessageTag(element);
            if (key != null) {
                return new Message(topic, tag, key, bytes);
            }
            return new Message(topic, tag, bytes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize element: " + element, e);
        }
    }

    default byte[] getBytes(T element) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(element);
    }

    String getTopic();

    default String getMessageKey(T element) {
        return null;
    }

    String getMessageTag(T element);
}
