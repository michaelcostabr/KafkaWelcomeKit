using System;
using Confluent.Kafka;
using KafkaWelcomeKit.Infra;

namespace KafkaWelcomeKit.Produzir
{
    public class ProdutorAssincrono : ProdutorAbstrato
    {
        public ProdutorAssincrono(BrokerHelper brokerHelper, IMessageWriter messageWriter) : base(brokerHelper, messageWriter) { }
        
        //Outro estilo de realizar o envio da mensagem, entretanto a execução da thread principal continua, 
        //enquanto a thread auxiliar aguarda o retorno da execução.
        public void Produzir<TChave, TValor>(string topico, TChave chave, TValor valor)
        {
            using (var p = new ProducerBuilder<TChave, TValor>(_brokerHelper.ProducerConfig).Build())
            {
                try
                {
                    p.Produce(topico, new Message<TChave, TValor> {Key = chave, Value = valor },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                _messageWriter.Write($"Falha na entrega: {deliveryReport.Error.Reason}", MessageType.Output);
                            }
                            else
                            {
                                _messageWriter.Write($"Entregou a mensagem com key '{deliveryReport.Message.Key}', value '{deliveryReport.Message.Value}' na partição '{deliveryReport.Partition}' e offset '{deliveryReport.Offset}'", MessageType.Output);
                            }
                        });
                    _messageWriter.Write("Requisitou envio. Aguardando relatório de entrega...", MessageType.Output);

                    //Neste exemplo não estou devolvendo um callback para o chamado, então utilizo o método poll para garantir que o broker finalizou a requisição.
                    _messageWriter.Write("Polling...", MessageType.Output);
                    p.Poll(TimeSpan.FromSeconds(60));
                    _messageWriter.Write("Polling ended.", MessageType.Output);
                }
                catch (ProduceException<Null, string> e)
                {
                    _messageWriter.Write($"Falha na entrega: {e.Error.Reason}", MessageType.Output);
                }
            }
        }
    }
}
