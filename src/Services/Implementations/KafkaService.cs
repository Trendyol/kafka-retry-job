using System;
using Confluent.Kafka;
using KafkaRetry.Job.Helpers.KafkaConfigs;
using KafkaRetry.Job.Services.Interfaces;

namespace KafkaRetry.Job.Services.Implementations;

public class KafkaService : IKafkaService
{
    private readonly ConfigurationService _configuration;

    public KafkaService(ConfigurationService configuration)
    {
        _configuration = configuration;
    }

    public IConsumer<string, string> BuildKafkaConsumer()
    {
        var bootstrapServers = _configuration.BootstrapServers;
        var groupId = _configuration.GroupId;
        var consumerConfig = CreateConsumerConfig(bootstrapServers, groupId);
        var consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);

        return consumerBuilder.Build();
    }

    public IProducer<string, string> BuildKafkaProducer()
    {
        var bootstrapServers = _configuration.BootstrapServers;
        var producerConfig = CreateProducerConfig(bootstrapServers);
        var producerBuilder = new ProducerBuilder<string, string>(producerConfig);

        return producerBuilder.Build();
    }

    public IAdminClient BuildAdminClient()
    {
        var bootstrapServers = _configuration.BootstrapServers;
        var adminClientConfig = CreateAdminClientConfig(bootstrapServers);
        var adminClientBuilder = new AdminClientBuilder(adminClientConfig);

        return adminClientBuilder.Build();
    }

    private ClientConfig CreateClientConfig(string bootstrapServers)
    {
        ClientConfig clientConfig = new ClientConfig()
            .WithBootstrapServers(bootstrapServers)
            .WithClientId(_configuration.ClientId)
            .WithMessageMaxBytes(_configuration.MessageMaxBytes);
        
        if (_configuration.SaslMechanism is not null)
        {
            clientConfig = clientConfig
                .WithSaslUsername(_configuration.SaslUsername)
                .WithSaslPassword(_configuration.SaslPassword)
                .WithSslCaLocation(_configuration.SslCaLocation)
                .WithSaslMechanism(_configuration.SaslMechanism)
                .WithSecurityProtocol(_configuration.SecurityProtocol)
                .WithSslKeystorePassword(_configuration.SslKeystorePassword);
        }
        
        if (_configuration.Acks is not null)
        {
            clientConfig = clientConfig
                .WithAcks(_configuration.Acks);
        }

        return clientConfig;
    }
    
    private AdminClientConfig CreateAdminClientConfig(string bootstrapServers)
    {
        ClientConfig clientConfig = CreateClientConfig(bootstrapServers);
        return new AdminClientConfig(clientConfig);
    }

    private ProducerConfig CreateProducerConfig(string bootstrapServers)
    {
        ClientConfig clientConfig = CreateClientConfig(bootstrapServers);
        ProducerConfig producerConfig = new ProducerConfig(clientConfig);

        producerConfig = producerConfig
            .WithEnableIdempotence(_configuration.EnableIdempotence)
            .WithBatchSize(_configuration.BatchSize)
            .WithLingerMs(_configuration.LingerMs)
            .WithMessageTimeoutMs(_configuration.MessageTimeoutMs)
            .WithRequestTimeoutMs(_configuration.RequestTimeoutMs);
        
        return producerConfig;
    }

    private ConsumerConfig CreateConsumerConfig(string bootstrapServers, string groupId)
    {
        ClientConfig clientConfig = CreateClientConfig(bootstrapServers);
        ConsumerConfig consumerConfig = new ConsumerConfig(clientConfig);
        
        consumerConfig = consumerConfig
            .WithGroupId(groupId)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithEnableAutoCommit(_configuration.EnableAutoCommit)
            .WithEnableAutoOffsetStore(_configuration.EnableAutoOffsetStore);
        
        return consumerConfig;
    }
        
    public Action<IConsumer<string, string>, ConsumeResult<string, string>> GetConsumerCommitStrategy()
    {
        return _configuration.EnableAutoCommit ?
            (assignedConsumer, result) =>
            {
                assignedConsumer.StoreOffset(result);
            } :
            (assignedConsumer, result) =>
            {
                assignedConsumer.StoreOffset(result);
                assignedConsumer.Commit();
            };
    }
}