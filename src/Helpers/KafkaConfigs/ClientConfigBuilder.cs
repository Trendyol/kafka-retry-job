using Confluent.Kafka;

namespace KafkaRetry.Job.Helpers.KafkaConfigs;

public static class ClientConfigBuilder
{
    public static ClientConfig WithBootstrapServers(this ClientConfig config, string bootstrapServers)
    {
        config.BootstrapServers = bootstrapServers;
        return config;
    }
    
    public  static ClientConfig WithSaslUsername(this ClientConfig config, string username)
    {
        config.SaslUsername = username;
        return config;
    }
    
    public  static ClientConfig WithSaslPassword(this ClientConfig config, string password)
    {
        config.SaslPassword = password;
        return config;
    }
    
    public  static ClientConfig WithSslCaLocation(this ClientConfig config, string sslCaLocation)
    {
        config.SslCaLocation = sslCaLocation;
        return config;
    }
    
    public  static ClientConfig WithSaslMechanism(this ClientConfig config, SaslMechanism? saslMechanism)
    {
        config.SaslMechanism = saslMechanism;
        return config;
    }
    
    public  static ClientConfig WithSecurityProtocol(this ClientConfig config, SecurityProtocol? securityProtocol)
    {
        config.SecurityProtocol = securityProtocol;
        return config;
    }

    public  static ClientConfig WithSslKeystorePassword(this ClientConfig config, string sslKeystorePassword)
    {
        config.SslKeystorePassword = sslKeystorePassword;
        return config;
    }
    
    public static ClientConfig WithClientId(this ClientConfig config, string clientId)
    {
        config.ClientId = clientId;
        return config;
    }
    
    public static ClientConfig WithMessageMaxBytes(this ClientConfig config, int? messageMaxBytes)
    {
        config.MessageMaxBytes = messageMaxBytes;
        return config;
    }
    
    public static ClientConfig WithAcks(this ClientConfig config, Acks? acks)
    {
        config.Acks = acks;
        return config;
    }
}