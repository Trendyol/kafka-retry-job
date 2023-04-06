using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
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

            using var assignedConsumer = _kafkaService.BuildKafkaConsumer();
            var errorTopics = GetErrorTopicsFromCluster();

            _logService.LogMatchingErrorTopics(errorTopics);

            using var producer = _kafkaService.BuildKafkaProducer();

            var utcNow = DateTime.UtcNow;

            try
            {
                foreach (var errorTopic in errorTopics)
                {
                    _logService.LogConsumerSubscribingTopic(errorTopic);

                    var topicPartitions = GetTopicMetadata().First(x => x.Topic == errorTopic).Partitions;

                    for (var partition = 0; partition < topicPartitions.Count; partition++)
                    {
                        var topicPartition = new TopicPartition(errorTopic, partition);
                        assignedConsumer.Assign(topicPartition);

                        while (true)
                        {
                            var result = assignedConsumer.Consume(TimeSpan.FromSeconds(3));

                            if (result is null)
                            {
                                _logService.LogEndOfPartition(topicPartition);
                                break;
                            }

                            var resultDate = result.Message.Timestamp.UtcDateTime;

                            if (utcNow < resultDate)
                            {
                                _logService.LogNewMessageArrived(utcNow);
                                break;
                            }

                            result.Message.Timestamp = new Timestamp(DateTime.UtcNow);

                            var retryTopic =
                                errorTopic.ReplaceAtEnd(_configuration.ErrorSuffix, _configuration.RetrySuffix);

                            _logService.LogProducingMessage(result, errorTopic, retryTopic);

                            await producer.ProduceAsync(retryTopic, result.Message);
                            
                            assignedConsumer.StoreOffset(result);
                            assignedConsumer.Commit();
                        }
                    }

                    assignedConsumer.Unassign();
                }
            }
            catch (Exception e)
            {
                _logService.LogError(e);
                assignedConsumer.Unassign();
                throw;
            }

            _logService.LogApplicationIsClosing();
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

        private List<TopicMetadata> GetTopicMetadata()
        {
            using var adminClient = _kafkaService.BuildAdminClient();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

            return metadata.Topics;
        }
    }
}