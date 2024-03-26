using Confluent.Kafka;

namespace KafkaRetry.Job.Helpers.KafkaConfigs;

public static class ClientConfigBuilder
{
    public static ClientConfig WithBootstrapServers(this ClientConfig config, string bootstrapServers)
    {
        if (!string.IsNullOrWhiteSpace(bootstrapServers))
        {
            config.BootstrapServers = bootstrapServers;   
        }
        return config;
    }
    
    public  static ClientConfig WithSaslUsername(this ClientConfig config, string username)
    {
        if (!string.IsNullOrWhiteSpace(username))
        {
            config.SaslUsername = username;   
        }
        return config;
    }
    
    public  static ClientConfig WithSaslPassword(this ClientConfig config, string password)
    {
        if (!string.IsNullOrWhiteSpace(password))
        {
            config.SaslPassword = password;   
        }
        return config;
    }
    
    public  static ClientConfig WithSslCaLocation(this ClientConfig config, string sslCaLocation)
    {
        if (!string.IsNullOrWhiteSpace(sslCaLocation))
        {
            config.SslCaLocation = sslCaLocation;   
        }
        return config;
    }
    
    public  static ClientConfig WithSaslMechanism(this ClientConfig config, SaslMechanism? saslMechanism)
    {
        if (saslMechanism is not null)
        {
            config.SaslMechanism = saslMechanism;   
        }
        return config;
    }
    
    public  static ClientConfig WithSecurityProtocol(this ClientConfig config, SecurityProtocol? securityProtocol)
    {
        if (securityProtocol is not null)
        {
            config.SecurityProtocol = securityProtocol;   
        }
        return config;
    }

    public  static ClientConfig WithSslKeystorePassword(this ClientConfig config, string sslKeystorePassword)
    {
        if (!string.IsNullOrWhiteSpace(sslKeystorePassword))
        {
            config.SslKeystorePassword = sslKeystorePassword;   
        }
        return config;
    }
    
    public static ClientConfig WithClientId(this ClientConfig config, string clientId)
    {
        if (!string.IsNullOrWhiteSpace(clientId))
        {
            config.ClientId = clientId;   
        }
        return config;
    }
    
    public static ClientConfig WithMessageMaxBytes(this ClientConfig config, int? messageMaxBytes)
    {
        if (messageMaxBytes is not null)
        {
            config.MessageMaxBytes = messageMaxBytes;   
        }
        return config;
    }
    
    public static ClientConfig WithAcks(this ClientConfig config, Acks? acks)
    {
        if (acks is not null)
        {
            config.Acks = acks;   
        }
        return config;
    }
}