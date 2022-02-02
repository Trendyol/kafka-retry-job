using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace KafkaRetry.Job.Services.Interfaces
{
    public interface ILogService
    {
        void LogApplicationStarted();
        void LogMatchingErrorTopics(List<string> errorTopics);
        void LogEndOfPartition(TopicPartition assignedPartition);
        void LogError(Exception exception);
        void LogProducingMessage(ConsumeResult<string, string> result, string errorTopic, string retryTopic);
        void LogAssignedPartitions(string partitions);
        void LogConsumerIsNotAssigned();
        void LogConsumerSubscribingTopic(string topic);
        void LogLastCommittedOffset(TopicPartitionOffset tpo);
        void LogNewMessageArrived(DateTime utcNow);
        void LogApplicationIsClosing();
    }
}