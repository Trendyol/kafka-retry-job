using Confluent.Kafka;

namespace KafkaRetry.Job.Helpers.KafkaConfigs;

public static class ConsumerConfigBuilder
{
    public static ConsumerConfig WithAutoOffsetReset(this ConsumerConfig config, AutoOffsetReset? autoOffsetReset)
    {
        config.AutoOffsetReset = autoOffsetReset;
        return config;
    }
    
    public static ConsumerConfig WithGroupId(this ConsumerConfig config, string groupId)
    {
        config.GroupId = groupId;
        return config;
    }
    
    public static ConsumerConfig WithEnableAutoCommit(this ConsumerConfig config, bool? autoCommit)
    {
        config.EnableAutoCommit = autoCommit;
        return config;
    }
    
    public static ConsumerConfig WithEnableAutoOffsetStore(this ConsumerConfig config, bool? autoOffsetStore)
    {
        config.EnableAutoOffsetStore = autoOffsetStore;
        return config;
    }
}