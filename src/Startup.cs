using KafkaRetry.Job.Extensions;
using KafkaRetry.Job.Services.Implementations;
using KafkaRetry.Job.Services.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaRetry.Job
{
    public class Startup
    {
        private readonly IConfiguration _configuration;

        public Startup(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            // Builders
            services.AddSingleton<IKafkaService, KafkaService>();

            // Services
            services.AddSingleton<IKafkaRetryJobService, KafkaRetryJobService>();
            services.AddSingleton<ConfigurationService>();

            // Logging
            services.AddSerilog(_configuration);
            services.AddScoped<ILogService, LogService>();
        }
    }
}