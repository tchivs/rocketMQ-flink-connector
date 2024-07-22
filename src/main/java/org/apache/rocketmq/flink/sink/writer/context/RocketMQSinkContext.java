package org.apache.rocketmq.flink.sink.writer.context;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.MailboxExecutor;

/**
 * This context provides information on the rocketmq record target location. An implementation that
 * would contain all the required context.
 */
@PublicEvolving
public interface RocketMQSinkContext {

    /**
     * Get the number of the subtask that RocketMQSink is running on. The numbering starts from 0
     * and goes up to parallelism-1. (parallelism as returned by {@link
     * #getNumberOfParallelInstances()}
     *
     * @return number of subtask
     */
    int getParallelInstanceId();

    /** @return number of parallel RocketMQSink tasks. */
    int getNumberOfParallelInstances();

    /**
     * RocketMQ can check the schema and upgrade the schema automatically. If you enable this
     * option, we wouldn't serialize the record into bytes, we send and serialize it in the client.
     */
    @Experimental
    boolean isEnableSchemaEvolution();

    /** Returns the current process time in flink. */
    long processTime();

    MailboxExecutor getMailboxExecutor();
}
