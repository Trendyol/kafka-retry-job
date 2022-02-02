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
        public string GroupId => GetValueOrThrowInvalidConfigException("GroupId"); 

        private string GetValueOrThrowInvalidConfigException(string configName)
        {
            var configValue = _configuration.GetValue<string>(configName);
            if (string.IsNullOrEmpty(configValue))
            {
                throw new InvalidConfigException($"{configName} {ErrorMessages.ConfigCanNotBeNullOrEmpty}");
            }

            return configValue;
        }
    }
}