using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaRetry.Job.Services.Interfaces;

namespace KafkaRetry.Job.Services.Implementations
{
    public class KafkaRetryJobService : IKafkaRetryJobService
    {
        private readonly IKafkaService _kafkaService;
        private readonly ConfigurationService _configuration;
        private readonly ILogService _logService;

        public KafkaRetryJobService(IKafkaService kafkaService,
            ConfigurationService configuration,
            ILogService logService)
        {
            _kafkaService = kafkaService;
            _configuration = configuration;
            _logService = logService;
        }

        public async Task MoveMessages()
        {
            _logService.LogApplicationStarted();

            using var subscribedConsumer =
                _kafkaService.BuildKafkaConsumer();
            using var assignedConsumer =
                _kafkaService.BuildKafkaConsumer(Guid.NewGuid().ToString());
            var errorTopics =
                GetErrorTopicsFromCluster();

            _logService.LogMatchingErrorTopics(errorTopics);

            using var producer = _kafkaService.BuildKafkaProducer();

            var utcNow = DateTime.UtcNow;

            try
            {
                foreach (var errorTopic in errorTopics)
                {
                    _logService.LogConsumerSubscribingTopic(errorTopic);
                    subscribedConsumer.Subscribe(errorTopic);
                    await WaitForPartitionAssignments(subscribedConsumer, TimeSpan.FromSeconds(30));

                    var assignedPartitions = subscribedConsumer.Assignment;

                    LogAssignedPartitions(assignedPartitions);
                    var topicPartitionOffsets = subscribedConsumer.Committed(assignedPartitions,
                        TimeSpan.FromSeconds(5));

                    foreach (var assignedPartition in assignedPartitions)
                    {
                        var offset = topicPartitionOffsets
                            .First(tpo => tpo.Partition == assignedPartition.Partition);
                        _logService.LogLastCommittedOffset(offset);
                        assignedConsumer.Assign(offset);

                        while (true)
                        {
                            var result = assignedConsumer.Consume(TimeSpan.FromSeconds(3));

                            if (result is null)
                            {
                                _logService.LogEndOfPartition(assignedPartition);
                                break;
                            }

                            var resultDate = result.Message.Timestamp.UtcDateTime;

                            if (utcNow < resultDate)
                            {
                                _logService.LogNewMessageArrived(utcNow);
                                break;
                            }

                            var retryTopic =
                                errorTopic.ReplaceAtEnd(_configuration.ErrorSuffix, _configuration.RetrySuffix);

                            _logService.LogProducingMessage(result, errorTopic, retryTopic);

                            await producer.ProduceAsync(retryTopic, result.Message);

                            subscribedConsumer.Commit(result);
                        }

                        assignedConsumer.Unassign();
                    }

                    subscribedConsumer.Unsubscribe();
                }
            }
            catch (Exception e)
            {
                _logService.LogError(e);
                throw;
            }

            _logService.LogApplicationIsClosing();
        }

        private static async Task WaitForPartitionAssignments(IConsumer<string, string> subscribedConsumer,
            TimeSpan timeout)
        {
            try
            {
                var cts = new CancellationTokenSource(timeout);

                while (!cts.IsCancellationRequested)
                {
                    var assignments = subscribedConsumer.Assignment;

                    if (assignments.Count > 0)
                    {
                        return;
                    }

                    await Task.Delay(10, cts.Token);
                }
            }
            catch (TaskCanceledException)
            {
            }
        }

        private void LogAssignedPartitions(List<TopicPartition> assignedPartitions)
        {
            if (assignedPartitions.Count > 0)
            {
                var partitions = string.Join(", ", assignedPartitions.Select(ap => ap.Partition));
                _logService.LogAssignedPartitions(partitions);
            }
            else
            {
                _logService.LogConsumerIsNotAssigned();
            }
        }

        private List<string> GetErrorTopicsFromCluster()
        {
            var topicRegex = _configuration.TopicRegex;
            var topics = GetClusterTopics();
            var errorTopicRegex = new Regex(topicRegex);

            var errorTopics = topics
                .Where(t => errorTopicRegex.IsMatch(t))
                .Where(t => t.EndsWith(_configuration.ErrorSuffix))
                .ToList();

            return errorTopics;
        }

        private List<string> GetClusterTopics()
        {
            using var adminClient = _kafkaService.BuildAdminClient();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

            var clusterTopics = metadata.Topics.Select(t => t.Topic).ToList();
            return clusterTopics;
        }
    }
}