using Confluent.Kafka;

namespace KafkaRetry.Job.Services.Interfaces
{
    public interface IKafkaService
    {
        IConsumer<string, string> BuildKafkaConsumer();
        IConsumer<string, string> BuildKafkaConsumer(string groupId);
        IProducer<string, string> BuildKafkaProducer();
        IAdminClient BuildAdminClient();
    }
}