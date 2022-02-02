using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;

namespace KafkaRetry.Job.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static void AddSerilog(this IServiceCollection services, IConfiguration configuration)
        {
            var section = configuration.GetSection("Serilog");

            Logger logger;

            if (section.Exists())
            {
                logger = new LoggerConfiguration()
                    .ReadFrom.Configuration(configuration)
                    .CreateLogger();
            }
            else
            {
                logger = new LoggerConfiguration()
                    .MinimumLevel.Information()
                    .WriteTo.Console()
                    .CreateLogger();
            }

            Log.Logger = logger;

            services.AddLogging(cfg =>
            {
                cfg.ClearProviders()
                    .AddSerilog(Log.Logger, true);
            });
        }
    }
}