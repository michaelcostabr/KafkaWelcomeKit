using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaWelcomeKit.Infra;

namespace KafkaWelcomeKit.Produzir
{
    class ProdutorAssincrono2 : ProdutorAbstrato
    {
        public ProdutorAssincrono2(BrokerHelper brokerHelper, IMessageWriter messageWriter) : base(brokerHelper, messageWriter) { }

        //Neste exemplo, como utilizamos awaiter, o processo é interrompido de forma a aguardar o resultado do broker. 
        // Similar a produzir de forma síncrona
        // Neste exemplo, a mensagem é postada com Key e Value
        public async Task Produzir<TChave, TValor>(string topico, TChave chave, TValor valor)
        {

            using (var p = new ProducerBuilder<TChave, TValor>(_brokerHelper.ProducerConfig).Build())
            {
                try
                {
                    var deliveryReport = await p.ProduceAsync(topico, new Message<TChave, TValor> { Key = chave, Value = valor});
                    _messageWriter.Write($"Entregou a mensagem com key '{deliveryReport.Message.Key}', value '{deliveryReport.Message.Value}' na partição '{deliveryReport.Partition}' e offset '{deliveryReport.Offset}'", MessageType.Output);
                }
                catch (ProduceException<string, string> e)
                {
                    _messageWriter.Write($"Falha na entrega: {e.Error.Reason}", MessageType.Output);
                }
            }
        }
    }
}
