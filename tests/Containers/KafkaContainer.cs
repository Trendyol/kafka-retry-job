using System;
using System.IO;
using System.Threading.Tasks;
using DotNet.Testcontainers.Containers.Builders;
using DotNet.Testcontainers.Containers.Modules;
using DotNet.Testcontainers.Containers.OutputConsumers;
using DotNet.Testcontainers.Containers.WaitStrategies;

namespace KafkaRetry.Job.Tests.Containers
{
    public class KafkaContainer
    {
        private const int Port = 9092;
        private readonly TestcontainersContainer _container;

        private readonly Stream _outStream = new MemoryStream();
        private readonly Stream _errorStream = new MemoryStream();

        public KafkaContainer(string zookeeperAddress)
        {
            var dockerHost = Environment.GetEnvironmentVariable("DOCKER_HOST");
            if (string.IsNullOrEmpty(dockerHost))
            {
                dockerHost = "unix:/var/run/docker.sock";
            }

            _container = new TestcontainersBuilder<TestcontainersContainer>()
                .WithDockerEndpoint(dockerHost)
                .WithImage("confluentinc/cp-kafka:6.0.1")
                .WithExposedPort(Port)
                .WithPortBinding(Port, Port)
                .WithEnvironment("KAFKA_ADVERTISED_LISTENERS",
                    "PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:9092")
                .WithEnvironment("KAFKA_ZOOKEEPER_CONNECT", zookeeperAddress)
                .WithEnvironment("KAFKA_BROKER_ID", "1")
                .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT")
                .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .WithName("kafka-testcontainer")
                .WithOutputConsumer(Consume.RedirectStdoutAndStderrToStream(_outStream, _errorStream))
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(Port))
                .Build();
        }

        public async Task CreateTopic(string name, int partitionCount = 1)
        {
            if (_container == null)
            {
                throw new NullReferenceException();
            }

            var createTopicCommand = "./usr/bin/kafka-topics " +
                                     $"--create --topic {name} " +
                                     $"--bootstrap-server localhost:9092 " +
                                     $"--partitions {partitionCount} " +
                                     $"--replication-factor 1";
            await _container.ExecAsync(new[] {"/bin/sh", "-c", createTopicCommand});
        }

        public async Task StartAsync()
        {
            await _container.StartAsync();
        }

        public async Task StopAndDisposeAsync()
        {
            await _container.StopAsync();
            await _container.DisposeAsync();
        }
    }
}