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
            
            var errorTopicPartitionsWithLag = GetErrorTopicInfosFromCluster(assignedConsumer, metadata);
            var errorTopics = errorTopicPartitionsWithLag.Select(p => p.Item1.Topic).Distinct().ToList();
            
            _logService.LogMatchingErrorTopics(errorTopics);

            using var producer = _kafkaService.BuildKafkaProducer();

            var utcNow = DateTime.UtcNow;

            try
            {
                var messageConsumeLimit = _configuration.MessageConsumeLimit;
                if (messageConsumeLimit <= 0)
                {
                    _logService.LogMessageConsumeLimitIsZero();
                }
                
                foreach (var (topicPartition, lag) in errorTopicPartitionsWithLag)
                {
                    if (messageConsumeLimit <= 0)
                    {
                        break;
                    }
                    if (lag <= 0)
                    {
                        continue;
                    }
                    
                    _logService.LogStartOfSubscribingTopicPartition(topicPartition);
                    
                    var errorTopic = topicPartition.Topic;
                    var currentLag = lag;
                    
                    assignedConsumer.Assign(topicPartition);

                    while (currentLag > 0 && messageConsumeLimit > 0)
                    {
                        var result = assignedConsumer.Consume(TimeSpan.FromSeconds(3));

                        if (result is null)
                        {
                            break;
                        }

                        currentLag -= 1;
                        messageConsumeLimit -= 1;

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
                    
                    _logService.LogEndOfSubscribingTopicPartition(topicPartition);
                }

                assignedConsumer.Unassign();
                
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
        
        private List<(TopicPartition, long)> GetErrorTopicInfosFromCluster(IConsumer<string, string> assignedConsumer, Metadata metadata)
        {
            _logService.LogFetchingErrorTopicInfoStarted();
            
            var topicRegex = _configuration.TopicRegex;
            var errorTopicRegex = new Regex(topicRegex);

            var topicPartitionMetadata = metadata.Topics
                .Where(t => errorTopicRegex.IsMatch(t.Topic))
                .SelectMany(topic =>
                    topic.Partitions.Select(partition => 
                        new TopicPartition(topic.Topic, partition.PartitionId)))
                .ToArray();

            var topicsWithFoundOffsets = assignedConsumer.Committed(topicPartitionMetadata, TimeSpan.FromSeconds(10));

            var topicPartitionInfos = topicsWithFoundOffsets.Select<TopicPartitionOffset, (TopicPartition, long)>(tpo => {
                var watermark = assignedConsumer.QueryWatermarkOffsets(tpo.TopicPartition, TimeSpan.FromSeconds(5));
                var lag = tpo.Offset >= 0 ? watermark.High - tpo.Offset : 0; 
                return (tpo.TopicPartition, lag);
            }).ToList();

            _logService.LogFetchingErrorTopicInfoFinished();
            
            return topicPartitionInfos;
        }
    }
}