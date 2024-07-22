package org.apache.rocketmq.flink.sink.writer.context;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;

@PublicEvolving
public class RocketMQSinkContextImpl implements RocketMQSinkContext {

    private final int numberOfParallelSubtasks;
    private final int parallelInstanceId;
    private final ProcessingTimeService processingTimeService;
    private final MailboxExecutor mailboxExecutor;
    private final boolean enableSchemaEvolution;

    public RocketMQSinkContextImpl(WriterInitContext initContext) {
        this.parallelInstanceId = initContext.getSubtaskId();
        this.numberOfParallelSubtasks = initContext.getNumberOfParallelSubtasks();
        this.processingTimeService = initContext.getProcessingTimeService();
        this.mailboxExecutor = initContext.getMailboxExecutor();
        this.enableSchemaEvolution = false;
    }
    @Deprecated
    public RocketMQSinkContextImpl(InitContext initContext) {
        this.parallelInstanceId = initContext.getSubtaskId();
        this.numberOfParallelSubtasks = initContext.getNumberOfParallelSubtasks();
        this.processingTimeService = initContext.getProcessingTimeService();
        this.mailboxExecutor = initContext.getMailboxExecutor();
        this.enableSchemaEvolution = false;
    }

    @Override
    public int getParallelInstanceId() {
        return parallelInstanceId;
    }

    @Override
    public int getNumberOfParallelInstances() {
        return numberOfParallelSubtasks;
    }

    @Override
    public boolean isEnableSchemaEvolution() {
        return enableSchemaEvolution;
    }

    @Override
    public long processTime() {
        return processingTimeService.getCurrentProcessingTime();
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }
}

