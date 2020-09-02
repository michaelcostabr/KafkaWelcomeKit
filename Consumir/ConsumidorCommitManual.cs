using Confluent.Kafka;
using System;
using System.Threading;

namespace KafkaWelcomeKit.Consumir
{
    public class ConsumidorCommitManual : ConsumidorAbstrato
    {
        public ConsumidorCommitManual(BrokerHelper brokerHelper, string consumerGroup) : base(brokerHelper, consumerGroup) { }
        
        public void Consumir(string topico)
        {
            ConsumerConfig.GroupId = ConsumerGroup;
            ConsumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            ConsumerConfig.EnableAutoCommit = false;

            //Este token será usado para mapear o CTRL+C e cancelar o consumo do broker. Seu uso é opcional.
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, string>(ConsumerConfig).Build())
            {
                consumer.Subscribe(topico);
                try
                {
                    Console.WriteLine("Preparando para consumir... Novos registros chegarão imediatamente.\nPressione CTRL+C para encerrar");
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumido registro da partição {cr.Partition}, offset {cr.Offset}, chave '{cr.Message.Key}' e valor '{cr.Message.Value}'");
                        //desconmentando a linha abaixo, ocorre o commit do registro.
                        //consumer.Commit(cr); 
                    }

                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C foi pressionado.
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}
