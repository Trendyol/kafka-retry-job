using System;
using System.Collections.Concurrent;
using Confluent.Kafka;
using KafkaRetry.Job.Helpers.KafkaConfigs;
using KafkaRetry.Job.Services.Interfaces;

namespace KafkaRetry.Job.Services.Implementations;

public class KafkaService : IKafkaService
{
    private readonly ConfigurationService _configuration;
    private readonly ConcurrentBag<IConsumer<string, string>> _consumers = new();
    private readonly ConcurrentBag<IProducer<string, string>> _producers = new();
    private readonly ConcurrentBag<IAdminClient> _adminClients = new();

    public KafkaService(ConfigurationService configuration)
    {
        _configuration = configuration;
    }

    ~KafkaService()
    {
        while (_consumers.TryTake(out var consumer))
        {
            consumer.Dispose();
        }
        
        while (_producers.TryTake(out var producer))
        {
            producer.Dispose();
        }
        
        while (_adminClients.TryTake(out var adminClient))
        {
            adminClient.Dispose();
        }
    }
    
    private IConsumer<string, string> BuildKafkaConsumer()
    {
        var bootstrapServers = _configuration.BootstrapServers;
        var groupId = _configuration.GroupId;
        var consumerConfig = CreateConsumerConfig(bootstrapServers, groupId);
        var consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);

        return consumerBuilder.Build();
    }

    private  IProducer<string, string> BuildKafkaProducer()
    {
        var bootstrapServers = _configuration.BootstrapServers;
        var producerConfig = CreateProducerConfig(bootstrapServers);
        var producerBuilder = new ProducerBuilder<string, string>(producerConfig);

        return producerBuilder.Build();
    }
    
    private IAdminClient BuildAdminClient()
    {
        var bootstrapServers = _configuration.BootstrapServers;
        var adminClientConfig = CreateAdminClientConfig(bootstrapServers);
        var adminClientBuilder = new AdminClientBuilder(adminClientConfig);

        return adminClientBuilder.Build();
    }
    
    public IConsumer<string, string> GetKafkaConsumer()
    {
        return _consumers.TryTake(out var consumer) ? consumer : BuildKafkaConsumer();
    }
    
    public IProducer<string, string> GetKafkaProducer()
    {
        return _producers.TryTake(out var producer) ? producer : BuildKafkaProducer();
    }

    public IAdminClient GetKafkaAdminClient()
    {
        return _adminClients.TryTake(out var adminClient) ? adminClient : BuildAdminClient();
    }

    public void ReleaseKafkaConsumer(ref IConsumer<string, string> consumer)
    {
        _consumers.Add(consumer);
        consumer = null;
    }
    
    public void ReleaseKafkaProducer(ref IProducer<string, string> producer)
    {
        _producers.Add(producer);
        producer = null;
    }

    public void ReleaseKafkaAdminClient(ref IAdminClient adminClient)
    {
        _adminClients.Add(adminClient);
        adminClient = null;
    }

    private ClientConfig CreateClientConfig(string bootstrapServers)
    {
        ClientConfig clientConfig = new ClientConfig()
            .WithBootstrapServers(bootstrapServers)
            .WithClientId(_configuration.ClientId)
            .WithMessageMaxBytes(_configuration.MessageMaxBytes)
            .WithSaslUsername(_configuration.SaslUsername)
            .WithSaslPassword(_configuration.SaslPassword)
            .WithSslCaLocation(_configuration.SslCaLocation)
            .WithSaslMechanism(_configuration.SaslMechanism)
            .WithSecurityProtocol(_configuration.SecurityProtocol)
            .WithSslKeystorePassword(_configuration.SslKeystorePassword)
            .WithAcks(_configuration.Acks);
        
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
        return _configuration.EnableAutoCommit is true ?
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