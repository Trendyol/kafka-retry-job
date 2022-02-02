using System;
using System.Collections.Generic;
using Confluent.Kafka;
using KafkaRetry.Job.Services.Interfaces;
using Microsoft.Extensions.Logging;

namespace KafkaRetry.Job.Services.Implementations
{
    public class LogService : ILogService
    {
        private readonly ILogger<LogService> _logger;

        public LogService(ILogger<LogService> logger)
        {
            _logger = logger;
        }

        public void LogApplicationStarted()
        {
            _logger.LogInformation("KafkaRetryJob started");
        }

        public void LogMatchingErrorTopics(List<string> errorTopics)
        {
            var errorTopicsString = string.Join(", ", errorTopics);
            _logger.LogInformation($"Matching error topics: {errorTopicsString}");
        }

        public void LogEndOfPartition(TopicPartition assignedPartition)
        {
            _logger.LogInformation(
                $"End of partition: {assignedPartition.Partition}, topic: {assignedPartition.Topic}.");
        }

        public void LogError(Exception exception)
        {
            _logger.LogError(exception, $"Exception thrown while moving messages. Message: {exception.Message}");
        }

        public void LogProducingMessage(ConsumeResult<string, string> result,
            string errorTopic,
            string retryTopic)
        {
            _logger.LogInformation(
                $"Producing message with key: {result.Message.Key}, " +
                $"partition: {result.Partition}, " +
                $"offset: {result.Offset}, " +
                $"from topic: {errorTopic}, " +
                $"to topic: {retryTopic}.");
        }

        public void LogAssignedPartitions(string partitions)
        {
            _logger.LogInformation($"Consumer assigned partitions: {partitions}");
        }

        public void LogConsumerIsNotAssigned()
        {
            _logger.LogInformation("Consumer is not assigned to any partition");
        }

        public void LogConsumerSubscribingTopic(string topic)
        {
            _logger.LogInformation($"Consumer is subscribing to topic: {topic}");
        }

        public void LogLastCommittedOffset(TopicPartitionOffset tpo)
        {
            _logger.LogInformation($"Last committed offset for consumer: {tpo.Offset}");
        }

        public void LogNewMessageArrived(DateTime utcNow)
        {
            _logger.LogInformation($"New message arrived to topic partition after: {utcNow}, " +
                                   $"breaking consume loop for this partition.");
        }

        public void LogApplicationIsClosing()
        {
            _logger.LogInformation("Application is ending, closing consumer and producer");
        }
    }
}