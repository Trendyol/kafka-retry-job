using System;
using Confluent.Kafka;
using KafkaRetry.Job.Exceptions;
using KafkaRetry.Job.Helpers;
using Microsoft.Extensions.Configuration;

namespace KafkaRetry.Job.Services.Implementations;

public class ConfigurationService
{
    private readonly IConfiguration _configuration;

    public ConfigurationService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public string BootstrapServers => GetValueOrThrowInvalidConfigException("BootstrapServers");
    public string TopicRegex => GetValueOrThrowInvalidConfigException("TopicRegex");
    public string ErrorSuffix => GetValueOrThrowInvalidConfigException("ErrorSuffix");
    public string RetrySuffix => GetValueOrThrowInvalidConfigException("RetrySuffix");
    public string RetryTopicNameInHeader => GetValue<string>("RetryTopicNameInHeader");

    public long MessageConsumeLimitPerTopicPartition =>
        GetValue<long?>("MessageConsumeLimitPerTopicPartition") ?? Int64.MaxValue;

    public bool EnableAutoCommit => GetValue<bool?>("EnableAutoCommit") ?? false;
    public bool EnableAutoOffsetStore => GetValue<bool?>("EnableAutoOffsetStore") ?? false;
    public string GroupId => GetValueOrThrowInvalidConfigException("GroupId");
    public string SaslUsername => GetValue<string>("SaslUsername");
    public string SaslPassword => GetValue<string>("SaslPassword");
    public string SslCaLocation => GetValue<string>("SslCaLocation");
    public SaslMechanism? SaslMechanism => GetValue<SaslMechanism?>("SaslMechanism");
    public string SslKeystorePassword => GetValue<string>("SslKeystorePassword");
    public SecurityProtocol? SecurityProtocol => GetValue<SecurityProtocol?>("SecurityProtocol");

    public bool EnableIdempotence => GetValue<bool?>("ProducerEnableIdempotence") ??
                                     Constants.ProducerConfigDefaults.EnableIdempotence;

    public Acks? Acks => GetValue<Acks?>("ProducerAcks");

    public int BatchSize =>
        GetValue<int?>("ProducerBatchSize") ?? Constants.ProducerConfigDefaults.BatchSize;

    public string ClientId =>
        GetValue<string>("ProducerClientId") ?? Constants.ProducerConfigDefaults.ClientId;

    public double LingerMs =>
        GetValue<double?>("ProducerLingerMs") ?? Constants.ProducerConfigDefaults.LingerMs;

    public int MessageTimeoutMs => GetValue<int?>("ProducerMessageTimeoutMs") ??
                                   Constants.ProducerConfigDefaults.MessageTimeoutMs;

    public int RequestTimeoutMs => GetValue<int?>("ProducerRequestTimeoutMs") ??
                                   Constants.ProducerConfigDefaults.RequestTimeoutMs;

    public int MessageMaxBytes => GetValue<int?>("ProducerMessageMaxBytes") ??
                                  Constants.ProducerConfigDefaults.MessageMaxBytes;

    private string GetValueOrThrowInvalidConfigException(string configName)
    {
        var configValue = _configuration.GetValue<string>(configName);
        if (string.IsNullOrEmpty(configValue))
        {
            throw new InvalidConfigException($"{configName} {ErrorMessages.ConfigCanNotBeNullOrEmpty}");
        }

        return configValue;
    }

    private T GetValue<T>(string configName)
    {
        return _configuration.GetValue<T>(configName);
    }
}