using Confluent.Kafka;

namespace KafkaRetry.Job.Helpers.KafkaConfigs;

public static class ProducerConfigBuilder
{
    public static ProducerConfig WithEnableIdempotence(this ProducerConfig config, bool? idempotence)
    {
        if (idempotence is not null)
        {
            config.EnableIdempotence = idempotence;
        }
        return config;
    }
    
    public static ProducerConfig WithBatchSize(this ProducerConfig config, int? batchSize)
    {
        if (batchSize is not null)
        {
            config.BatchSize = batchSize;
        }
        return config;
    }
    
    public static ProducerConfig WithLingerMs(this ProducerConfig config, double? lingerMs)
    {
        if (lingerMs is not null)
        {
            config.LingerMs = lingerMs;
        }
        return config;
    }
    
    public static ProducerConfig WithMessageTimeoutMs(this ProducerConfig config, int? messageTimeoutMs)
    {
        if (messageTimeoutMs is not null)
        {
            config.MessageTimeoutMs = messageTimeoutMs;
        }
        return config;
    }
    
    public static ProducerConfig WithRequestTimeoutMs(this ProducerConfig config, int? requestTimeoutMs)
    {
        config.RequestTimeoutMs = requestTimeoutMs;
        return config;
    }
}