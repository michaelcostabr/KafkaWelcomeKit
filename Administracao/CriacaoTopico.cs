using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaWelcomeKit.Produzir;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaWelcomeKit.Administracao
{
    public class CriacaoTopico : ProdutorAbstrato
    {
        public CriacaoTopico(BrokerHelper brokerHelper) : base(brokerHelper, new Infra.NullMessageWriter()) { }

        public async Task CriarTopico(string nomeTopico, int numParticoes, short fatorReplicacao, ClientConfig clientConfig)
        {
            using (var adminClient = new AdminClientBuilder(this.ProducerConfig).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new List<TopicSpecification> {
                        new TopicSpecification { Name = nomeTopico, NumPartitions = numParticoes, ReplicationFactor = fatorReplicacao} });
                    Console.WriteLine($"Tópico {nomeTopico} criado.");
                }
                catch (CreateTopicsException e)
                {
                    if (e.Results[0].Error.Code != ErrorCode.TopicAlreadyExists)
                    {
                        Console.WriteLine($"Erro na criação do tópico {nomeTopico}: {e.Results[0].Error.Reason}");
                    }
                    else
                    {
                        Console.WriteLine("O tópico já existe.");
                    }
                }
                
                Console.WriteLine("Pressione qualquer tecla para continuar.");
                Console.ReadKey();
            }
        }
    }
}
