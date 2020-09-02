using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaWelcomeKit.Infra;

namespace KafkaWelcomeKit.Produzir
{
    class ProdutorAssincronoSchema : ProdutorAbstrato
    {
        public ProdutorAssincronoSchema(BrokerHelper brokerHelper, IMessageWriter messageWriter) : base(brokerHelper, messageWriter) { }

        // Neste exemplo, utilizamos o Confluent Schema Registry e Apache Avro para serializar as mensagens enviadas
        // Neste exemplo, como utilizamos awaiter, o processo é interrompido de forma a aguardar o resultado do broker. 
        // Similar a produzir de forma síncrona
        // Neste exemplo, a mensagem é postada com Key e Value
        public async Task Produzir<TChave, TValor>(string topico, TChave chave, TValor valor)
        { 

            using (var schemaRegistry = new CachedSchemaRegistryClient(_brokerHelper.SchemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<TChave, TValor>(_brokerHelper.ProducerConfig)
                    .SetKeySerializer(new AvroSerializer<TChave>(schemaRegistry))
                    .SetValueSerializer(new AvroSerializer<TValor>(schemaRegistry))
                    .Build())
            {
                try
                {
                    var deliveryReport = await producer.ProduceAsync(topico, new Message<TChave, TValor> { Key = chave, Value = valor });
                    _messageWriter.Write($"Entregou um objeto '{deliveryReport.Message.Value}' com a key '{deliveryReport.Message.Key}' em '{deliveryReport.TopicPartition} e offset '{deliveryReport.Offset}'", MessageType.Output);
                }
                catch (ProduceException<string, string> e)
                {
                    _messageWriter.Write($"Falha na entrega: {e.Error.Reason}", MessageType.Output);
                }
                catch (Exception ex)
                {
                    _messageWriter.Write($"Falha na entrega: {ex.Message}", MessageType.Output);
                }
            }
        }
    }
}
