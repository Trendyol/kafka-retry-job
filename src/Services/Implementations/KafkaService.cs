using Confluent.Kafka;
using KafkaRetry.Job.Services.Interfaces;

namespace KafkaRetry.Job.Services.Implementations
{
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

        private AdminClientConfig CreateAdminClientConfig(string bootstrapServers)
        {
            return new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SaslUsername = _configuration.SaslUsername ?? string.Empty,
                SaslPassword = _configuration.SaslPassword ?? string.Empty,
                SslCaLocation = _configuration.SslCaLocation ?? string.Empty,
                SaslMechanism = _configuration.SaslMechanism,
                SecurityProtocol = _configuration.SecurityProtocol,
                SslKeystorePassword = _configuration.SslKeystorePassword ?? string.Empty,
            };
        }

        private ProducerConfig CreateProducerConfig(string bootstrapServers)
        {
            return new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SaslUsername = _configuration.SaslUsername ?? string.Empty,
                SaslPassword = _configuration.SaslPassword ?? string.Empty,
                SslCaLocation = _configuration.SslCaLocation ?? string.Empty,
                SaslMechanism = _configuration.SaslMechanism,
                SecurityProtocol = _configuration.SecurityProtocol,
                SslKeystorePassword = _configuration.SslKeystorePassword ?? string.Empty,
                EnableIdempotence = true
            };
        }

        private ConsumerConfig CreateConsumerConfig(string bootstrapServers, string groupId)
        {
            return new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = groupId,
                EnableAutoCommit = false,
                SaslUsername = _configuration.SaslUsername ?? string.Empty,
                SaslPassword = _configuration.SaslPassword ?? string.Empty,
                SslCaLocation = _configuration.SslCaLocation ?? string.Empty,
                SaslMechanism = _configuration.SaslMechanism,
                SecurityProtocol = _configuration.SecurityProtocol,
                SslKeystorePassword = _configuration.SslKeystorePassword ?? string.Empty,
                EnableAutoOffsetStore = false
            };
        }
    }
}