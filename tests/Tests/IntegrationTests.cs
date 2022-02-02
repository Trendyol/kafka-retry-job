using System;
using System.Threading.Tasks;
using KafkaRetry.Job.Services.Implementations;
using KafkaRetry.Job.Tests.Containers;
using KafkaRetry.Job.Tests.Utils;
using NUnit.Framework;

namespace KafkaRetry.Job.Tests.Tests
{
    public class IntegrationTests
    {
        [Test]
        public async Task ShouldMoveMessagesFromErrorToRetryTopic_SinglePartition()
        {
            // Arrange
            SetApplicationConfig("error", "retry", "my-test-topic", ".*-test-.*error");

            var errorTopic = "my-test-topic.error";
            var retryTopic = "my-test-topic.retry";

            await TestUtils.CreateTopic(errorTopic);
            await TestUtils.CreateTopic(retryTopic);

            await TestUtils.ProduceMessage(errorTopic, "messageKey1", "messageValue1");
            await TestUtils.ProduceMessage(errorTopic, "messageKey2", "messageValue2");

            // Act
            await Program.Main(new string[] { });

            // Verify
            var timeout = TimeSpan.FromMinutes(1);

            TestUtils.AssertTopicContainsMessage(retryTopic,
                m =>
                    m.Key == "messageKey1" &&
                    m.Value == "messageValue1",
                timeout);
            TestUtils.AssertTopicContainsMessage(retryTopic,
                m =>
                    m.Key == "messageKey2" &&
                    m.Value == "messageValue2",
                timeout);
        }

        [Test]
        public async Task ShouldMoveMessagesFromErrorToRetryTopic_MultiplePartitions()
        {
            // Arrange
            SetApplicationConfig("error", "retry", "my-test-topic", ".*-test-.*error");

            var errorTopic = "my-test-topic.error";
            var retryTopic = "my-test-topic.retry";

            await TestUtils.CreateTopic(errorTopic, 2);
            await TestUtils.CreateTopic(retryTopic);

            await TestUtils.ProduceMessage(errorTopic, "messageKey1", "messageValue1");
            await TestUtils.ProduceMessage(errorTopic, "messageKey2", "messageValue2");
            await TestUtils.ProduceMessage(errorTopic, "messageKey3", "messageValue3");
            await TestUtils.ProduceMessage(errorTopic, "messageKey4", "messageValue4");

            // Act
            await Program.Main(new string[] { });

            // Verify
            var timeout = TimeSpan.FromMinutes(1);

            TestUtils.AssertTopicContainsMessage(retryTopic,
                m =>
                    m.Key == "messageKey1" &&
                    m.Value == "messageValue1",
                timeout);
            TestUtils.AssertTopicContainsMessage(retryTopic,
                m =>
                    m.Key == "messageKey2" &&
                    m.Value == "messageValue2",
                timeout);
            TestUtils.AssertTopicContainsMessage(retryTopic,
                m =>
                    m.Key == "messageKey3" &&
                    m.Value == "messageValue3",
                timeout);
            TestUtils.AssertTopicContainsMessage(retryTopic,
                m =>
                    m.Key == "messageKey4" &&
                    m.Value == "messageValue4",
                timeout);
        }

        private static void SetApplicationConfig(string errorSuffix,
            string retrySuffix,
            string groupId,
            string topicRegex)
        {
            Environment.SetEnvironmentVariable(nameof(ConfigurationService.BootstrapServers),
                ContainerConstants.KafkaServer);
            Environment.SetEnvironmentVariable(nameof(ConfigurationService.ErrorSuffix),
                errorSuffix);
            Environment.SetEnvironmentVariable(nameof(ConfigurationService.RetrySuffix),
                retrySuffix);
            Environment.SetEnvironmentVariable(nameof(ConfigurationService.GroupId),
                groupId);
            Environment.SetEnvironmentVariable(nameof(ConfigurationService.TopicRegex),
                topicRegex);
        }
    }
}