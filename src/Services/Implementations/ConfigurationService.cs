using Confluent.Kafka;
using KafkaRetry.Job.Exceptions;
using KafkaRetry.Job.Helpers;
using Microsoft.Extensions.Configuration;

namespace KafkaRetry.Job.Services.Implementations
{
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
        public string GroupId => GetValueOrThrowInvalidConfigException("GroupId");
        public string SaslUsername => GetValue<string>("SaslUsername");
        public string SaslPassword => GetValue<string>("SaslPassword");
        public string SslCaLocation => GetValue<string>("SslCaLocation");
        public SaslMechanism? SaslMechanism => GetValue<SaslMechanism?>("SaslMechanism");
        public string SslKeystorePassword => GetValue<string>("SslKeystorePassword");
        public SecurityProtocol? SecurityProtocol => GetValue<SecurityProtocol?>("SecurityProtocol");

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
}