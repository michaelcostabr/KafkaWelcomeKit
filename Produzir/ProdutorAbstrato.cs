using Confluent.Kafka;
using KafkaWelcomeKit.Infra;

namespace KafkaWelcomeKit.Produzir
{
    public abstract class ProdutorAbstrato
    {
        protected BrokerHelper _brokerHelper = null;
        protected IMessageWriter _messageWriter = null;

        protected ProdutorAbstrato(BrokerHelper brokerHelper, IMessageWriter messageWriter)
        {
            _brokerHelper = brokerHelper;
            _messageWriter = messageWriter;
        }

        public ProducerConfig ProducerConfig { 
            get => _brokerHelper.ProducerConfig; 
            set => _brokerHelper.ProducerConfig = value; 
        }
    }
}
