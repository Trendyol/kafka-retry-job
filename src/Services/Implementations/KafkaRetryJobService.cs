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
            var adminClient = _kafkaService.BuildAdminClient();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(120));
            adminClient.Dispose();
            var errorTopics = GetErrorTopicsFromCluster(metadata);

            _logService.LogMatchingErrorTopics(errorTopics);

            using var producer = _kafkaService.BuildKafkaProducer();

            var utcNow = DateTime.UtcNow;

            try
            {
                foreach (var errorTopic in errorTopics)
                {
                    _logService.LogConsumerSubscribingTopic(errorTopic);

                    var topicPartitions = metadata.Topics.First(x => x.Topic == errorTopic).Partitions;

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

                            var retryTopic = GetRetryTopicName(result, errorTopic);

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
        
        private string GetRetryTopicName(ConsumeResult<string,string> result , string errorTopic )
        {
            return !string.IsNullOrEmpty(_configuration.RetryTopicNameInHeader) && 
                   result.Message.Headers.TryGetLastBytes(_configuration.RetryTopicNameInHeader, out var retryTopicInHeader) ?
                System.Text.Encoding.UTF8.GetString(retryTopicInHeader) :
                errorTopic.ReplaceAtEnd(_configuration.ErrorSuffix, _configuration.RetrySuffix);
        }

        private List<string> GetErrorTopicsFromCluster(Metadata metadata)
        {
            var topicRegex = _configuration.TopicRegex;
            var clusterTopics = metadata.Topics.Select(t => t.Topic).ToList();
            var errorTopicRegex = new Regex(topicRegex);

            var errorTopics = clusterTopics
                .Where(t => errorTopicRegex.IsMatch(t))
                .Where(t => t.EndsWith(_configuration.ErrorSuffix))
                .ToList();

            return errorTopics;
        }
    }
}