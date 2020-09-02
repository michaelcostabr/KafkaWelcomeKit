using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaWelcomeKit.Infra;

namespace KafkaWelcomeKit.Produzir
{
    public class ProdutorSincrono : ProdutorAbstrato
    {
        public ProdutorSincrono(BrokerHelper brokerHelper, IMessageWriter messageWriter) : base(brokerHelper, messageWriter) { }

        // Neste exemplo, como utilizamos awaiter, o processo é interrompido de forma a aguardar o resultado do broker. 
        // Similar a produzir de forma síncrona
        // Neste exemplo, a mensagem é postada apenas informando Value
        public async Task Produzir<TChave, TValor>(string topico, TChave chave, TValor valor)
        {

            try
            {
                using (var p = new ProducerBuilder<TChave, TValor>(_brokerHelper.ProducerConfig).Build())
                {
                    try
                    {
                        var deliveryReport = await p.ProduceAsync(topico, new Message<TChave, TValor> { Key = chave, Value = valor });
                        _messageWriter.Write($"Entregou a mensagem com key '{deliveryReport.Message.Key}', value '{deliveryReport.Message.Value}' na partição '{deliveryReport.Partition}' e offset '{deliveryReport.Offset}'", MessageType.Output);
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        _messageWriter.Write($"Falha na entrega: {e.Error.Reason}", MessageType.Output);
                    }
                }
            }
            catch (Exception e)
            {
                _messageWriter.Write($"Erro: {e.Message}", MessageType.Output);
            }
        }
    }
}