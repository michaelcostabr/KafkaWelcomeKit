using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace KafkaWelcomeKit.Consumir
{
    public abstract class ConsumidorAbstrato
    {
        private BrokerHelper _brokerHelper = null;
        protected readonly string ConsumerGroup;

        protected ConsumidorAbstrato(BrokerHelper brokerHelper, string consumerGroup)
        {
            _brokerHelper = brokerHelper;
            ConsumerGroup = consumerGroup;
        }

        public ConsumerConfig ConsumerConfig
        {
            get => _brokerHelper.ConsumerConfig;
            set => _brokerHelper.ConsumerConfig = value;
        }

        public SchemaRegistryConfig SchemaRegistryConfig
        {
            get => _brokerHelper.SchemaRegistryConfig;
        }

        public virtual void Unsubscribe(string ConsumerGroup)
        {
            _brokerHelper.ConsumerConfig.GroupId = ConsumerGroup;
            using (var consumer = new ConsumerBuilder<string, string>(_brokerHelper.ConsumerConfig).Build())
            {
                consumer.Unsubscribe();
                consumer.Close();
            }
        }
    }
}
