using System;
using Confluent.Kafka;

namespace KafkaRetry.Job.Services.Interfaces
{
    public interface IKafkaService
    {
        public IConsumer<string, string> GetKafkaConsumer();
        public IProducer<string, string> GetKafkaProducer();
        public IAdminClient GetKafkaAdminClient();
        public void ReleaseKafkaConsumer(ref IConsumer<string, string> consumer);
        public void ReleaseKafkaProducer(ref IProducer<string, string> producer);
        public void ReleaseKafkaAdminClient(ref IAdminClient adminClient);
        public Action<IConsumer<string, string>, ConsumeResult<string, string>> GetConsumerCommitStrategy();
    }
}