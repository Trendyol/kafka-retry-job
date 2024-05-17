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
            
            var adminClient = _kafkaService.BuildAdminClient();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(120));
            adminClient.Dispose();
            
            var assignedConsumerPool = new ThreadLocal<IConsumer<string, string>>(() => _kafkaService.BuildKafkaConsumer());
            var producerPool = new ThreadLocal<IProducer<string, string>>(() => _kafkaService.BuildKafkaProducer());
            
            var errorTopicsWithLag = GetErrorTopicInfosFromCluster(assignedConsumerPool.Value, metadata);
            var errorTopics = errorTopicsWithLag.Keys.ToList();
            
            _logService.LogMatchingErrorTopics(errorTopics);
            
            var consumerCommitStrategy= _kafkaService.GetConsumerCommitStrategy();
            
            var messageConsumeLimit = _configuration.MessageConsumeLimitPerTopicPartition;
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
                        await MoveMessagesForTopic(topicPartitionsWithLag, assignedConsumerPool, producerPool,
                            consumerCommitStrategy);
                        semaphore.Release();
                    }));
            }

            Task.WaitAll(tasks.ToArray());
            assignedConsumerPool.Value.Dispose();
            
            _logService.LogApplicationIsClosing();
        }

        private async Task MoveMessagesForTopic(
            List<(TopicPartition, long)> topicPartitionsWithLag, 
            ThreadLocal<IConsumer<string, string>> assignedConsumerPool, 
            ThreadLocal<IProducer<string, string>> producerPool,
            Action<IConsumer<string,string>, ConsumeResult<string,string>> consumerCommitStrategy
        ) {
            var assignedConsumer = assignedConsumerPool.Value;
            var producer = producerPool.Value;
            
            foreach (var (topicPartition, lag) in topicPartitionsWithLag)
            {
                var errorTopic = topicPartition.Topic;
                if (lag <= 0)
                {
                    continue;
                }
                
                try
                {
                    var messageConsumeLimitForTopicPartition = _configuration.MessageConsumeLimitPerTopicPartition;
                    _logService.LogStartOfSubscribingTopicPartition(topicPartition);
                    
                    var currentLag = lag;
                    
                    assignedConsumer.Assign(topicPartition);

                    while (currentLag > 0 && messageConsumeLimitForTopicPartition > 0)
                    {
                        var result = assignedConsumer.Consume(TimeSpan.FromSeconds(3));

                        if (result is null)
                        {
                            break;
                        }

                        currentLag -= 1;
                        messageConsumeLimitForTopicPartition -= 1;
                    
                        result.Message.Timestamp = new Timestamp(DateTime.UtcNow);

                        var retryTopic = GetRetryTopicName(result, errorTopic);

                        _logService.LogProducingMessage(result, errorTopic, retryTopic);

                        await producer.ProduceAsync(retryTopic, result.Message);

                        consumerCommitStrategy.Invoke(assignedConsumer, result);
                    }
                
                    assignedConsumer.Unassign();
                    
                    _logService.LogEndOfSubscribingTopicPartition(topicPartition);
                }
                catch (Exception e)
                {
                    _logService.LogError(e);
                    assignedConsumer.Unassign();
                    throw;
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