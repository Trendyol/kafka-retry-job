using Confluent.Kafka;

namespace KafkaRetry.Job.Helpers.KafkaConfigs;

public static class ConsumerConfigBuilder
{
    public static ConsumerConfig WithAutoOffsetReset(this ConsumerConfig config, AutoOffsetReset? autoOffsetReset)
    {
        if (autoOffsetReset is not null)
        {
            config.AutoOffsetReset = autoOffsetReset;
        }
        return config;
    }
    
    public static ConsumerConfig WithGroupId(this ConsumerConfig config, string groupId)
    {
        if (!string.IsNullOrWhiteSpace(groupId))
        {
            config.GroupId = groupId;
        }
        return config;
    }
    
    public static ConsumerConfig WithEnableAutoCommit(this ConsumerConfig config, bool? autoCommit)
    {
        if (autoCommit is not null)
        {
            config.EnableAutoCommit = autoCommit;
        }
        return config;
    }
    
    public static ConsumerConfig WithEnableAutoOffsetStore(this ConsumerConfig config, bool? autoOffsetStore)
    {
        if (autoOffsetStore is not null)
        {
            config.EnableAutoOffsetStore = autoOffsetStore;
        }
        return config;
    }
}