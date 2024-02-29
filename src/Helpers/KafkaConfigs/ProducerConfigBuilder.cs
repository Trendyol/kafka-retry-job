using Confluent.Kafka;

namespace KafkaRetry.Job.Helpers.KafkaConfigs;

public static class ProducerConfigBuilder
{
    public static ProducerConfig WithEnableIdempotence(this ProducerConfig config, bool? idempotence)
    {
        config.EnableIdempotence = idempotence;
        return config;
    }
    
    public static ProducerConfig WithBatchSize(this ProducerConfig config, int? batchSize)
    {
        config.BatchSize = batchSize;
        return config;
    }
    
    public static ProducerConfig WithLingerMs(this ProducerConfig config, double? lingerMs)
    {
        config.LingerMs = lingerMs;
        return config;
    }
    
    public static ProducerConfig WithMessageTimeoutMs(this ProducerConfig config, int? messageTimeoutMs)
    {
        config.MessageTimeoutMs = messageTimeoutMs;
        return config;
    }
    
    public static ProducerConfig WithRequestTimeoutMs(this ProducerConfig config, int? requestTimeoutMs)
    {
        config.RequestTimeoutMs = requestTimeoutMs;
        return config;
    }
}