package org.apache.rocketmq.flink.sink.writer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public class RocketMQWriterBucketState implements Serializable {

    private static final long serialVersionUID = 1L;

    public long getCurrentOffset() {
        return currentOffset;
    }

    public List<Message> getPendingMessages() {
        return pendingMessages;
    }

    public long getLastCheckpointId() {
        return lastCheckpointId;
    }

    // 当前的写入偏移量
    private long currentOffset;

    // 未提交的消息列表
    private List<Message> pendingMessages;

    // 上次检查点的元数据
    private long lastCheckpointId;
    public RocketMQWriterBucketState() {


    }
    public RocketMQWriterBucketState(long currentOffset, long lastCheckpointId) {
        this.currentOffset = currentOffset;

        this.lastCheckpointId = lastCheckpointId;
    }
    public RocketMQWriterBucketState(long currentOffset, long lastCheckpointId, List<Message> sendCommittable) {
        this(currentOffset,lastCheckpointId);
        this.pendingMessages = sendCommittable;
    }

    public RocketMQWriterBucketState(long currentOffset, long lastCheckpointId, Message... sendCommittable) {
        this(currentOffset,lastCheckpointId);
        this.pendingMessages = Arrays.asList(sendCommittable);
    }


    @Override
    public String toString() {
        return "RocketMQWriterBucketState{" +
                "currentOffset=" + currentOffset +
                ", pendingMessages=" + pendingMessages +
                ", lastCheckpointId=" + lastCheckpointId +
                '}';
    }

    public List<Message> getMessages() {
        return pendingMessages;
    }
}
