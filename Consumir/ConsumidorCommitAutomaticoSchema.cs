using com.localiza.arquitetura;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;

namespace KafkaWelcomeKit.Consumir
{
    public class ConsumidorCommitAutomaticoSchema : ConsumidorAbstrato
    {
        public ConsumidorCommitAutomaticoSchema(BrokerHelper brokerHelper, string consumerGroup) : base(brokerHelper, consumerGroup) { }

        public void Consumir(string topico)
        {
            ConsumerConfig.GroupId = ConsumerGroup;

            //Este token será usado para mapear o CTRL+C e cancelar o consumo do broker. Seu uso é opcional.
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(this.SchemaRegistryConfig))
            using (var consumer =
            new ConsumerBuilder<string, welcome_kafka>(ConsumerConfig)
                .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                .SetValueDeserializer(new AvroDeserializer<welcome_kafka>(schemaRegistry).AsSyncOverAsync())
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build())
            {
                consumer.Subscribe(topico);
                try
                {
                    Console.WriteLine("Preparando para consumir... Novos registros chegarão imediatamente.\nPressione CTRL+C para encerrar");
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumido registro da partição {cr.Partition}, offset {cr.Offset}, com chave '{cr.Message.Key}' e valor '{cr.Message.Value.id} / {cr.Message.Value.mensagem}'");
                        consumer.Commit(cr);
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
