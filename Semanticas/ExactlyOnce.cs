using Confluent.Kafka;
using KafkaWelcomeKit.Infra;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaWelcomeKit.Semanticas
{
    public class ExatclyOnce
    {
        private BrokerHelper _brokerHelper = null;
        private IMessageWriter _messageWriter = null;
        private string _topico = null;
        private string _consumerGroup = null;

        public ExatclyOnce(BrokerHelper brokerHelper, string topico, string consumerGroup, IMessageWriter messageWriter)
        {
            _brokerHelper = brokerHelper;
            _topico = topico;
            _consumerGroup = consumerGroup;
            _messageWriter = messageWriter;
        }
        public async Task IniciarProducao(CancellationTokenSource cts)
        {
            // para garantir a entrega de 1 e apenas 1 mensagem, e na ordem, limitamos os requests in-flight para o máximo de 5, 
            // retries > 0 e acks=all. Se algum destes parâmetros for configurado fora dessa regra, ocorrerá exceção no producer.
            // para ajustar todos de uma vez, basta habilitar o parametro EnableIdempotence = true

            _brokerHelper.ProducerConfig.EnableIdempotence = true;

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
            //para permitir que em caso de erro, não realizar commit para não perder a mensagem
            _brokerHelper.ConsumerConfig.EnableAutoCommit = false;
            _brokerHelper.ConsumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            _brokerHelper.ConsumerConfig.GroupId = _consumerGroup;

            using (var consumer = new ConsumerBuilder<string, string>(_brokerHelper.ConsumerConfig).Build())
            {
                consumer.Subscribe(_topico);

                var fakeDatabase = new FakeDatabase(_messageWriter);

                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        _messageWriter.Write($"Consumiu (key : message): {cr.Message.Key} : {cr.Message.Value} no offset {cr.Offset}", MessageType.Output);

                        fakeDatabase.BeginTransaction();

                        if (SimularProcessamentoBemSucedido())
                        {
                            _messageWriter.Write($"Processou com sucesso e realizou commit do offset {cr.Offset}", MessageType.Output);
                            fakeDatabase.Commit();
                            consumer.Commit(cr);
                        }
                        else
                        {
                            _messageWriter.Write($"Erro no processamento pós consumo. Não realizou commit do offset {cr.Offset}", MessageType.Output);
                            fakeDatabase.Rollback();
                            consumer.Subscribe(_topico); //forçar recomeçar a leitura a partir do último registro commited
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

//simular um banco de dados.
internal class FakeDatabase
{
    private IMessageWriter _messageWriter = null;
    public FakeDatabase(IMessageWriter messageWriter) { _messageWriter = messageWriter; }
    internal void BeginTransaction()
    {
        _messageWriter.Write("Iniciou transação no banco.", MessageType.Output);
    }

    internal void Commit()
    {
        _messageWriter.Write("Realizou commit no banco.", MessageType.Output);
    }

    internal void Rollback()
    {
        _messageWriter.Write("Realizou rollback no banco.", MessageType.Output);
    }
}
