using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaWelcomeKit.Consumir
{
    public class ConsumidorAPartirDeOffset : ConsumidorAbstrato
    {
        public ConsumidorAPartirDeOffset(BrokerHelper brokerHelper, string consumerGroup) : base(brokerHelper, consumerGroup) { }

        public async Task Consumir(string topico, Offset offsetInicio)
        {
            ConsumerConfig.GroupId = ConsumerGroup;
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

                var assignment = consumer.Assignment;

                while (assignment.Count == 0)
                {
                    Thread.Sleep(50);
                    assignment = consumer.Assignment;
                }

                var topicPartition = new TopicPartition(assignment[0].Topic, assignment[0].Partition);
                consumer.Assign(topicPartition);

                await this.SeekAsync(consumer, assignment[0].Topic, assignment[0].Partition, offsetInicio);

                try
                {
                    Console.WriteLine($"Preparando para consumir a partir do offset {offsetInicio}... Novos registros chegarão imediatamente.\nPressione CTRL+C para encerrar");
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumido registro via offset {cr.Offset} da partição {cr.Partition} com chave '{cr.Message.Key}' e valor '{cr.Message.Value}'");
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