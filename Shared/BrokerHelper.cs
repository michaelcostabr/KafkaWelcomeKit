using System;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

public class BrokerHelper
{
    public ProducerConfig ProducerConfig = null;
    public ConsumerConfig ConsumerConfig = null;
    public SchemaRegistryConfig SchemaRegistryConfig = null;

    public BrokerHelper(string BootstrapServers,
                        string SecurityProtocol,
                        string SaslMechanism,
                        string SaslUsername,
                        string SaslPassword,
                        string SslCaLocation,
                        string SchemaRegistryUrl,
                        int SchemaRegistryRequestTimeoutMs,
                        int SchemaRegistryMaxCachedSchemas,
                        string SchemaRegistryBasicAuthUserInfo)
    {

        SecurityProtocol securityProtocolEnum;
        SaslMechanism saslMechanismEnum;

        Enum.TryParse(SecurityProtocol, out securityProtocolEnum);
        Enum.TryParse(SaslMechanism, out saslMechanismEnum);

        var clientConfig = new ClientConfig
        {
            BootstrapServers = BootstrapServers
        };

        if (!string.IsNullOrEmpty(SaslUsername))
        {
            clientConfig.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            clientConfig.SecurityProtocol = securityProtocolEnum;
            clientConfig.SaslMechanism = saslMechanismEnum;
            clientConfig.SaslUsername = SaslUsername;
            clientConfig.SaslPassword = SaslPassword;
        }

        clientConfig.SslCaLocation = SslCaLocation;

        ProducerConfig = new ProducerConfig(clientConfig);
        ConsumerConfig = new ConsumerConfig(clientConfig);

        SchemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = SchemaRegistryUrl,
            RequestTimeoutMs = SchemaRegistryRequestTimeoutMs,
            MaxCachedSchemas = SchemaRegistryMaxCachedSchemas

        };

        if (!string.IsNullOrEmpty(SchemaRegistryBasicAuthUserInfo))
        {
            SchemaRegistryConfig.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
            SchemaRegistryConfig.BasicAuthUserInfo = SchemaRegistryBasicAuthUserInfo;
        }
    }

}