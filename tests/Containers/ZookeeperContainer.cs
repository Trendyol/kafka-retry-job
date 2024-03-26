using System;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;

namespace KafkaRetry.Job.Tests.Containers
{
    public class ZookeeperContainer
    {
        private const int Port = 2181;
        private readonly IContainer _container;

        public string Address => $"{_container.IpAddress}:{Port}";

        public ZookeeperContainer()
        {
            var dockerHost = Environment.GetEnvironmentVariable("DOCKER_HOST");
            if (string.IsNullOrEmpty(dockerHost))
            {
                dockerHost = "unix:/var/run/docker.sock";
            }

            _container = new ContainerBuilder()
                .WithDockerEndpoint(dockerHost)
                .WithImage(new DockerImage("zookeeper"))
                .WithExposedPort(Port)
                .WithPortBinding(Port, Port)
                .WithPortBinding(2888, 2888)
                .WithPortBinding(3888, 3888)
                .WithName("zookeeper-testcontainer")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(Port))
                .Build();
        }

        public async Task StopAndDisposeAsync()
        {
            await _container.StopAsync();
            await _container.DisposeAsync();
        }

        public async Task StartAsync()
        {
            await _container.StartAsync();
        }
    }
}