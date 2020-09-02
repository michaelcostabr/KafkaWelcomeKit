using Confluent.Kafka;
using KafkaWelcomeKit.Infra;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaWelcomeKit.Semanticas
{
    public class AtMostOnce
    {
        private BrokerHelper _brokerHelper = null;
        private IMessageWriter _messageWriter = null;
        private string _topico = null;
        private string _consumerGroup = null;

        public AtMostOnce(BrokerHelper brokerHelper, string topico, string consumerGroup, IMessageWriter messageWriter)
        {
            _brokerHelper = brokerHelper;
            _topico = topico;
            _consumerGroup = consumerGroup;
            _messageWriter = messageWriter;
        }
        public async Task IniciarProducao(CancellationTokenSource cts)
        {
            //para garantir a entrega de no máximo 1, é necessário configurar os seguintes parametros
            //acks=1, max.in.flight.requests.per.connection=5

            _brokerHelper.ProducerConfig.Acks = Acks.Leader; //garantir o recebimento por pelo menos a partição líder
            _brokerHelper.ProducerConfig.MaxInFlight = 5;

            using (var p = new ProducerBuilder<string, string>(_brokerHelper.ProducerConfig).Build())
            {

                while (!cts.IsCancellationRequested)
                {

                    try
                    {
                        var deliveryReport = await p.ProduceAsync(_topico, new Message<string, string> { Key = ChaveAleatoria(), Value = ValorAleatorio() });
                        _messageWriter.Write($"Produziu (key : message): {deliveryReport.Key} : {deliveryReport.Value} no offset {deliveryReport.Offset} da partição {deliveryReport.Partition}.", MessageType.Input);
                        await Task.Delay(2000);
                    }
                    catch (ProduceException<string, string> e)
                    {
                        _messageWriter.Write($"Falha na entrega: {e.Error.Reason}", MessageType.Input);
                    }

                }
            }
        }

        public async Task IniciarConsumo(CancellationTokenSource cts)
        {
            _brokerHelper.ConsumerConfig.GroupId = _consumerGroup;

            using (var consumer = new ConsumerBuilder<string, string>(_brokerHelper.ConsumerConfig).Build())
            {
                consumer.Subscribe(_topico);
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        //sempre commita a mensagem retirada, independente de haver erro no resto do processo
                        consumer.Commit(cr);
                        _messageWriter.Write($"Consumiu (key : message): {cr.Message.Key} : {cr.Message.Value} no offset {cr.Offset}", MessageType.Output);

                        if (SimularProcessamentoBemSucedido())
                        {
                            _messageWriter.Write($"Processou com sucesso e realizou commit do offset {cr.Offset}", MessageType.Output);
                        }
                        else
                        {
                            _messageWriter.Write($"Erro no processamento pós consumo. Realizou commit do offset {cr.Offset} mesmo assim.", MessageType.Output);
                        }
                        await Task.Delay(1000);
                    }
                    catch (ConsumeException e)
                    {
                        _messageWriter.Write($"Falha no consumo: {e.Error.Reason}", MessageType.Input);
                    }

                }
            }
        }

        private bool SimularProcessamentoBemSucedido()
        {
            //aleatoriamente retorna sucesso ou erro
            return (new Random()).Next(0, 5) == 1;
        }

        #region Métodos de Apoio
        private string ValorAleatorio()
        {
            string[] Chaves = new string[] { "Mensagem 1", "Mensagem 2", "Mensagem 3", "Mensagem 4", "Mensagem 5" };
            return $"{Chaves[(new Random()).Next(0, 5)]} {DateTime.Now}";
        }

        private static string ChaveAleatoria()
        {
            string[] Chaves = new string[] { "Chave1", "Chave2", "Chave3", "Chave4", "Chave5" };
            return Chaves[(new Random()).Next(0, 5)];
        }
        #endregion
    }
}
