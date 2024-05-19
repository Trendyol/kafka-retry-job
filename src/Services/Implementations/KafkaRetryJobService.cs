using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
            
            var adminClient = _kafkaService.GetKafkaAdminClient();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(120));
            _kafkaService.ReleaseKafkaAdminClient(ref adminClient);

            var consumer = _kafkaService.GetKafkaConsumer();
            var errorTopicsWithLag = GetErrorTopicInfosFromCluster(consumer, metadata);
            var errorTopics = errorTopicsWithLag.Keys.ToList();
            
            _logService.LogMatchingErrorTopics(errorTopics);
            
            var consumerCommitStrategy= _kafkaService.GetConsumerCommitStrategy();
            
            var messageConsumeLimit = _configuration.MessageConsumeLimitPerTopic;
            if (messageConsumeLimit <= 0)
            {
                _logService.LogMessageConsumeLimitIsZero();
                return;
            }
            
            var maxDegreeOfParallelism = _configuration.MaxLevelParallelism;
            var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();
            
            foreach (var (_, topicPartitionsWithLag) in errorTopicsWithLag)
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                    {
                        await MoveMessagesForTopic(topicPartitionsWithLag, consumerCommitStrategy);
                        semaphore.Release();
                    }));
            }
            
            await Task.WhenAll(tasks.ToArray());
            
            _logService.LogApplicationIsClosing();
        }

        private async Task MoveMessagesForTopic(
            List<(TopicPartition, long)> topicPartitionsWithLag, 
            Action<IConsumer<string,string>, ConsumeResult<string,string>> consumerCommitStrategy
        )
        {
            var consumer = _kafkaService.GetKafkaConsumer();
            var producer = _kafkaService.GetKafkaProducer();
            
            var messageConsumeLimitPerTopic = _configuration.MessageConsumeLimitPerTopic;
            
            foreach (var (topicPartition, lag) in topicPartitionsWithLag)
            {
                if (lag <= 0)
                {
                    continue;
                }
                var errorTopic = topicPartition.Topic;

                try
                {
                    _logService.LogStartOfSubscribingTopicPartition(topicPartition);

                    var currentLag = lag;

                    consumer.Assign(topicPartition);
                    
                    while (currentLag > 0 && messageConsumeLimitPerTopic > 0)
                    {
                        var result = consumer.Consume(TimeSpan.FromSeconds(10));

                        if (result is null)
                        {
                            continue;
                        }

                        currentLag -= 1;
                        messageConsumeLimitPerTopic -= 1;

                        result.Message.Timestamp = new Timestamp(DateTime.UtcNow);

                        var retryTopic = GetRetryTopicName(result, errorTopic);
                        
                        _logService.LogProducingMessage(result, errorTopic, retryTopic);
                        
                        await producer.ProduceAsync(retryTopic, result.Message);

                        consumerCommitStrategy.Invoke(consumer, result);
                    }
                    
                    _logService.LogEndOfSubscribingTopicPartition(topicPartition);
                }
                catch (Exception e)
                {
                    _logService.LogError(e);
                }
                finally
                {
                    consumer.Unassign();
                    _kafkaService.ReleaseKafkaConsumer(ref consumer);
                    _kafkaService.ReleaseKafkaProducer(ref producer);
                }
            }
        }

        private string GetRetryTopicName(ConsumeResult<string,string> result , string errorTopic )
        {
            return !string.IsNullOrEmpty(_configuration.RetryTopicNameInHeader) && 
                result.Message.Headers.TryGetLastBytes(_configuration.RetryTopicNameInHeader, out var retryTopicInHeader) ?
                    System.Text.Encoding.UTF8.GetString(retryTopicInHeader) :
                    errorTopic.ReplaceAtEnd(_configuration.ErrorSuffix, _configuration.RetrySuffix);
        }
        
        private IDictionary<string, List<(TopicPartition, long)>> GetErrorTopicInfosFromCluster(IConsumer<string, string> assignedConsumer, Metadata metadata)
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
                var lag = tpo.Offset >= 0 ? watermark.High - tpo.Offset : watermark.High - watermark.Low; 
                return (tpo.TopicPartition, lag);
            })
                .GroupBy(t => t.Item1.Topic)
                .ToImmutableDictionary(
                    t => t.Key,
                    t => t.ToList()
                );

            _logService.LogFetchingErrorTopicInfoFinished();
            
            return topicPartitionInfos;
        }
    }
}