using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaWelcomeKit.Consumir
{
    public class ConsumidorPorOffset : ConsumidorAbstrato
    {
        public ConsumidorPorOffset(BrokerHelper brokerHelper, string consumerGroup) : base(brokerHelper, consumerGroup) { }

        public async Task Consumir(string topico, Offset offset)
        {
            ConsumerConfig.GroupId = ConsumerGroup;
            ConsumerConfig.EnableAutoCommit = false;

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

                await this.SeekAsync(consumer, assignment[0].Topic, assignment[0].Partition, offset);

                var cr = consumer.Consume();
                Console.WriteLine($"Consumido registro via offset {cr.Offset} da partição {cr.Partition} com chave '{cr.Message.Key}' e valor '{cr.Message.Value}'");
            }
        }       

    }
}
