using System.Threading.Tasks;
using KafkaRetry.Job.Tests.Containers;
using NUnit.Framework;

namespace KafkaRetry.Job.Tests
{
    [SetUpFixture]
    public class GlobalSetupFixture
    {
        public static KafkaContainer KafkaContainer;
        public static ZookeeperContainer ZookeeperContainer;

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            ZookeeperContainer = new ZookeeperContainer();
            await ZookeeperContainer.StartAsync();

            KafkaContainer = new KafkaContainer(ZookeeperContainer.Address);
            await KafkaContainer.StartAsync();
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            var stopAndDisposeAsync = KafkaContainer.StopAndDisposeAsync();
            var stopAndDisposeZookeeperTask = ZookeeperContainer.StopAndDisposeAsync();
            await Task.WhenAll(stopAndDisposeAsync, stopAndDisposeZookeeperTask);
        }
    }
}