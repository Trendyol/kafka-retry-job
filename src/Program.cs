using System.Threading.Tasks;
using KafkaRetry.Job.Services.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaRetry.Job
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var services = new ServiceCollection();
            var configuration = SetUpConfiguration(args, services);

            var startup = new Startup(configuration);
            startup.ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            await serviceProvider.GetRequiredService<IKafkaRetryJobService>().MoveMessages();
        }

        private static IConfiguration SetUpConfiguration(string[] args, ServiceCollection serviceCollection)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true)
                .AddCommandLine(args)
                .AddEnvironmentVariables()
                .Build();
            serviceCollection.AddSingleton<IConfiguration>(configuration);
            return configuration;
        }
    }
}