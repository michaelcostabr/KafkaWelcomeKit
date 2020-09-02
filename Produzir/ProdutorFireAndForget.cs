using Confluent.Kafka;
using KafkaWelcomeKit.Infra;

namespace KafkaWelcomeKit.Produzir
{
    public class ProdutorFireAndForget : ProdutorAbstrato
    {
        public ProdutorFireAndForget(BrokerHelper brokerHelper, IMessageWriter messageWriter) : base(brokerHelper, messageWriter) { }

        public void Produzir<TChave, TValor>(string topico, TChave chave, TValor valor)
        {

            using (var p = new ProducerBuilder<TChave, TValor>(_brokerHelper.ProducerConfig).Build())
            {
                try
                {
                    p.Produce(topico, new Message<TChave, TValor> { Key = chave, Value = valor});
                    _messageWriter.Write("Requisitou envio sem relatório de entrega.", MessageType.Output);
                    //Aparentemente há um bug no driver, pois se executar debugando, a mensagem é entregue, se passar executando normalmente, não vai.
                }
                catch (ProduceException<Null, string> e)
                {
                    _messageWriter.Write($"Falha na entrega: {e.Error.Reason}", MessageType.Output);
                }
            }
        }
    }
}
