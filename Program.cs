using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading.Tasks;

namespace KafkaWelcomeKit
{
    partial class Program
    {
        public static IConfigurationRoot Configuration { get; set; }
        private static string _consumerGroup = null;
        private static string _topico = null;
        private static string _topicoSchema = null;
        private static BrokerHelper GerarBrokerConfig()
        {
            return new BrokerHelper(Configuration["BootstrapServers"],
                                           Configuration["SecurityProtocol"],
                                           Configuration["SaslMechanism"],
                                           Configuration["SaslUsername"],
                                           Configuration["SaslPassword"],
                                           Configuration["SslCaLocation"],
                                           Configuration["SchemaRegistryUrl"],
                                           int.Parse(Configuration["SchemaRegistryRequestTimeoutMs"]),
                                           int.Parse(Configuration["SchemaRegistryMaxCachedSchemas"]),
                                           Configuration["SchemaRegistryBasicAuthUserInfo"]);
        }

        static async Task Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json");

            Configuration = builder.Build();
            _consumerGroup = Configuration["NomeConsumerGroup"];
            _topico = Configuration["NomeTopico"];
            _topicoSchema = Configuration["SchemaRegistryNomeTopico"];


            Console.WriteLine("Hello Kafka! Este app tem como função exibir alguns casos de uso do Apache Kafka.\n");

            string cmd = string.Empty;

            while (cmd.ToUpper() != "SAIR")
            {
                switch (cmd.ToUpper())
                {
                    case "SAIR":
                        return;
                    case "AJUDA":
                        ImprimirAjuda();
                        break;
                    case "P1":
                        await ProduzirAguardando();
                        break;
                    case "P2":
                        await ProduzirAssincronamente();
                        break;
                    case "P3":
                        await ProduzirAssincronamenteComChave();
                        break;
                    case "P4":
                        await ProduzirComGarantiaDeEntregaEOrdem();
                        break;
                    case "P5":
                        ProducaoFireAndForget();
                        break;
                    case "P6":
                        await ProducaoSchema();
                        break;
                    case "C1":
                        ConsumirCommitManual();
                        break;
                    case "C2":
                        ConsumirCommitAutomatico();
                        break;
                    case "C3":
                        BuscarOffsetsDisponiveis();
                        break;
                    case "C4":
                        await ConsumirOffsetAsync();
                        break;
                    case "C5":
                        await ConsumirApartirDeOffsetEspecifico();
                        break;
                    case "C6":
                        ConsumirCommitAutomaticoSchema();
                        break;
                    case "C7":
                        Unsubscribe(_topico, _consumerGroup);
                        break;
                    case "D1":
                        await AtLeastOnce();
                        break;
                    case "D2":
                        await AtMostOnce();
                        break;
                    case "D3":
                        await ExactlyOnce();
                        break;
                    case "A1":
                        await CriacaoTopico();
                        break;
                    default:
                        break;
                }

                if (cmd.ToUpper() != "AJUDA") Console.Clear();
                Console.Write("Digite o comando desejado, 'AJUDA' para ver lista de comandos ou 'SAIR' para finalizar.\nComando: ");

                cmd = Console.ReadLine();
            }

            Console.WriteLine("Processo finalizado.");

        }        

        private static void ImprimirAjuda()
        {
            Console.WriteLine("\nComandos disponíveis:");
            Console.WriteLine("SAIR - finaliza a aplicação");
            Console.WriteLine("P1 - Produzir aguardando");
            Console.WriteLine("P2 - Produzir de forma assíncrona");
            Console.WriteLine("P3 - Produzir de forma assíncrona e utilizando key");
            Console.WriteLine("P4 - Produzir com garantia de entrega e ordem");
            Console.WriteLine("P5 - Produzir sem garantias (Fire and Forget)");
            Console.WriteLine("P6 - Produzir validando Schema");
            Console.WriteLine("\nC1 - Consumir realizando commit manual");
            Console.WriteLine("C2 - Consumir realizando commit automático");
            Console.WriteLine("C3 - Consultar offsets disponíveis");
            Console.WriteLine("C4 - Consumir um offset específico");
            Console.WriteLine("C5 - Consumir a partir de um offset específico");
            Console.WriteLine("C6 - Consumir utilizando schema");
            Console.WriteLine("C7 - Unsubcribe em tópico");
            Console.WriteLine("\nD1 - Semântica At least once");
            Console.WriteLine("D2 - Semântica At most once");
            Console.WriteLine("D3 - Semântica Exactly Once");
            Console.WriteLine("\nA1 - Criação de tópico");
            Console.WriteLine("\n");            
        }            
    }
}