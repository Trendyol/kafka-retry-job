using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaRetry.Job.Tests.Containers;
using NUnit.Framework;

namespace KafkaRetry.Job.Tests.Utils
{
    public static class TestUtils
    {
        public static async Task CreateTopic(string name, int partition = 0)
        {
            await GlobalSetupFixture.KafkaContainer.CreateTopic(name, partition);
        }

        public static async Task ProduceMessage(string topic, string key, string value)
        {
            await ProduceMessage(topic, new Message<string, string>
            {
                Key = key,
                Value = value
            });
        }

        public static async Task ProduceMessage(string topic, Message<string, string> message)
        {
            using var producer = new ProducerBuilder<string, string>(
                new ProducerConfig
                {
                    BootstrapServers = ContainerConstants.KafkaServer
                }).Build();

            await producer.ProduceAsync(topic, message);
        }

        public static void AssertTopicContainsMessage(string topic,
            Func<Message<string, string>, bool> predicate,
            TimeSpan timeout)
        {
            var cts = new CancellationTokenSource(timeout);
            using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = ContainerConstants.KafkaServer,
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            }).Build();
            consumer.Subscribe(topic);

            while (!cts.IsCancellationRequested)
            {
                var result = consumer.Consume(100);
                if (result == null)
                {
                    continue;
                }

                if (predicate(result.Message))
                {
                    return;
                }
            }

            Assert.Fail($"AssertTopicContainsMessage timeout, topic: {topic}, timeout: {timeout}");
        }
    }
}