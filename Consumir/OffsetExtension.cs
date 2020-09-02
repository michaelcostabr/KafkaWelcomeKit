using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaWelcomeKit.Consumir
{
    public static class OffsetExtension
    {
        public static WatermarkOffsets BuscarOffsetsDisponiveis(this ConsumidorAbstrato ob, string topico, string consumerGroup)
        {
            ob.ConsumerConfig.GroupId = consumerGroup;

            using (var consumer = new ConsumerBuilder<string, string>(ob.ConsumerConfig).Build())
            {
                consumer.Subscribe(topico);

                var assignment = consumer.Assignment;

                while (assignment.Count == 0)
                {
                    Thread.Sleep(50); //ha um bug no driver para .net, as vezes é preciso aguardar a informação chegar
                    assignment = consumer.Assignment;
                }

                var topicPartition = new TopicPartition(assignment[0].Topic, assignment[0].Partition);

                var wmo = consumer.GetWatermarkOffsets(topicPartition);
                while (wmo.High.Value == -1001) //ha um bug no driver para .net, as vezes é preciso aguardar a informação chegar
                {
                    wmo = consumer.GetWatermarkOffsets(topicPartition);
                    Thread.Sleep(50);
                }

                return wmo;
            }
        }

        public static async Task<bool> SeekAsync(this ConsumidorAbstrato ob, IConsumer<string, string> consumer, string topic, Partition partition, long offset)
        {
            //ha um bug no driver para .net, as vezes é preciso aguardar a informação chegar, por isso criei o SeekAsync
            bool sucesso = TrySeek(consumer, topic, partition, offset);
            while (!sucesso)
            {
                await Task.Delay(1000);
                sucesso = TrySeek(consumer, topic, partition, offset);
            }
            return sucesso;
        }

        private static bool TrySeek(IConsumer<string, string> consumer, string topic, Partition partition, long offset)
        {
            try
            {
                consumer.Seek(new TopicPartitionOffset(topic, partition, new Offset(offset)));
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
