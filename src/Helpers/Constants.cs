namespace KafkaRetry.Job.Helpers;

public static class Constants
{
    public static class ProducerConfigDefaults
    {
        public const bool EnableIdempotence = true;
        public const int BatchSize = 1000000;
        public const string ClientId = "rdkafka";
        public const double LingerMs = 5;
        public const int MessageTimeoutMs = 300000;
        public const int RequestTimeoutMs = 30000;
        public const int MessageMaxBytes = 1000000;
    }
}