using Confluent.Kafka;
using KafkaWelcomeKit.Consumir;
using KafkaWelcomeKit.Produzir;
using System;
using System.Threading.Tasks;

namespace KafkaWelcomeKit
{
    partial class Program
    {
        private static Infra.DualConsoleMessageWriter console = new Infra.DualConsoleMessageWriter(Console.WindowHeight);

        public static void BuscarOffsetsDisponiveis()
        {
            var consumer = new ConsumidorPorOffset(GerarBrokerConfig(), _consumerGroup);
            var watermark = consumer.BuscarOffsetsDisponiveis(_topico, _consumerGroup);
            if (watermark != null)
            {
                Console.WriteLine($"Os offsets disponíveis vão de #{watermark.Low} a #{watermark.High}).");
            }
            else
            {
                Console.WriteLine($"Não foi possível buscar os offsets disponíveis");
            }
            Console.WriteLine("Pressione qualquer tecla para continuar.");
            Console.ReadKey();
        }

        private static async Task ConsumirOffsetAsync()
        {
            var consumer = new ConsumidorPorOffset(GerarBrokerConfig(), _consumerGroup);

            var cmd = string.Empty;
            Console.WriteLine("Consumir qual offset?");
            cmd = Console.ReadLine();
            long offset;

            if (long.TryParse(cmd, out offset))
            {
                var watermark = consumer.BuscarOffsetsDisponiveis(_topico, _consumerGroup);
                if (watermark != null)
                {
                    if (offset > watermark.High || offset < watermark.Low)
                    {
                        Console.WriteLine($"Erro: offset fora do range disponível ({watermark.Low} a {watermark.High} ");
                        Console.ReadKey();
                        return;
                    }
                }
                await consumer.Consumir(_topico, offset);
            }
            else
            {
                Console.WriteLine("Erro: offset deve ser um número.");
            }

            Console.WriteLine("Pressione qualquer tecla para continuar.");
            Console.ReadKey();
        }

        private static async Task ConsumirApartirDeOffsetEspecifico()
        {
            var consumer = new ConsumidorAPartirDeOffset(GerarBrokerConfig(), _consumerGroup);

            var cmd = string.Empty;
            Console.WriteLine("Consumir a partir de qual offset?");
            cmd = Console.ReadLine();
            long offset;

            if (long.TryParse(cmd, out offset))
            {
                var watermark = consumer.BuscarOffsetsDisponiveis(_topico, _consumerGroup);
                if (watermark != null)
                {
                    if (offset > watermark.High || offset < watermark.Low)
                    {
                        Console.WriteLine($"Erro: offset fora do range disponível ({watermark.Low} a {watermark.High})");
                        Console.ReadKey();
                        return;
                    }
                }
                await consumer.Consumir(_topico, offset);
            }
            else
            {
                Console.WriteLine("Erro: offset deve ser um número.");
                Console.ReadKey();
            }
        }

        private static void Unsubscribe(string Topic, string ConsumerGroup)
        {
            var consumer = new ConsumidorCommitAutomatico(GerarBrokerConfig(), ConsumerGroup);
            consumer.Unsubscribe(ConsumerGroup);
            Console.WriteLine("Unsubscribed.");
            Console.WriteLine("Pressione qualquer tecla para continuar.");
            Console.ReadKey();
        }

        private static void ConsumirCommitAutomatico()
        {
            var consumer = new ConsumidorCommitAutomatico(GerarBrokerConfig(), _consumerGroup);
            consumer.Consumir(_topico);
        }

        private static void ConsumirCommitManual()
        {
            var consumer = new ConsumidorCommitManual(GerarBrokerConfig(), _consumerGroup);
            consumer.Consumir(_topico);
        }

        private static void ConsumirCommitAutomaticoSchema()
        {
            var consumer = new ConsumidorCommitAutomaticoSchema(GerarBrokerConfig(), _consumerGroup);
            consumer.Consumir(_topicoSchema);
        }

        private static async Task ProduzirAguardando()
        {
            var producer = new ProdutorSincrono(GerarBrokerConfig(), console);
            console = new Infra.DualConsoleMessageWriter(Console.WindowHeight);

            while (true)
            {
                var cmd = string.Empty;
                console.Write("Digite a mensagem a ser enviada ('SAIR' para finalizar): ", Infra.MessageType.Input);
                cmd = Console.ReadLine();
                if (cmd.ToUpper() == "SAIR") return;

                await producer.Produzir<Null, string>(_topico, null, cmd);
            }
        }

        private async static Task ProduzirAssincronamente()
        {
            console = new Infra.DualConsoleMessageWriter(Console.WindowHeight);
            var producer = new ProdutorAssincrono(GerarBrokerConfig(), console);

            while (true)
            {
                console.Write("Digite a mensagem a ser enviada ('SAIR' para finalizar): ", Infra.MessageType.Input);
                string cmd = Console.ReadLine();
                if (cmd.ToUpper() == "SAIR") return;

                await Task.Run(() =>
                {
                    producer.Produzir<Null, string>(_topico, null, cmd);
                });
            }
        }

        private async static Task ProduzirAssincronamenteComChave()
        {
            console = new Infra.DualConsoleMessageWriter(Console.WindowHeight);
            var producer = new ProdutorAssincrono2(GerarBrokerConfig(), console);

            while (true)
            {
                console.Write("Digite a mensagem a ser enviada ('SAIR' para finalizar): ", Infra.MessageType.Input);
                string cmd = Console.ReadLine();
                if (cmd.ToUpper() == "SAIR") return;

                console.Write("Digite a chave (key) para a mensagem: ", Infra.MessageType.Input);
                string key = Console.ReadLine();

                await producer.Produzir(_topico, key, cmd);
            }
        }


        private static void ProducaoFireAndForget()
        {
            var brokerFireAndForget = GerarBrokerConfig();
            console = new Infra.DualConsoleMessageWriter(Console.WindowHeight);

            while (true)
            {
                console.Write("Digite a mensagem a ser enviada ('SAIR' para finalizar): ", Infra.MessageType.Input);
                string cmd = Console.ReadLine();
                if (cmd.ToUpper() == "SAIR") return;

                Task.Run(() =>
                {
                    console.Write("Vou chamar.", Infra.MessageType.Output);

                    brokerFireAndForget.ProducerConfig.Acks = Acks.None;
                    brokerFireAndForget.ProducerConfig.EnableDeliveryReports = false;
                    var producer = new ProdutorFireAndForget(brokerFireAndForget, console); // new Infra.NullMessageWriter());
                    producer.Produzir<Null, string>(_topico, null, cmd);

                    console.Write("Chamei.", Infra.MessageType.Output);

                }).Wait();
            }
        }

        private async static Task ProducaoSchema()
        {
            console = new Infra.DualConsoleMessageWriter(Console.WindowHeight);
            var producer = new ProdutorAssincronoSchema(GerarBrokerConfig(), console);

            while (true)
            {
                console.Write("Digite a mensagem a ser enviada ('SAIR' para finalizar): ", Infra.MessageType.Input);
                string cmd = Console.ReadLine();
                if (cmd.ToUpper() == "SAIR") return;

                var record = new com.localiza.arquitetura.welcome_kafka
                {
                    id = cmd.Length, //key apenas para fins de demonstração da tipagem no schema
                    mensagem = cmd
                };

                await producer.Produzir(_topicoSchema, "Michael", record);
            }
        }

        private static async Task ProduzirComGarantiaDeEntregaEOrdem()
        {
            console = new Infra.DualConsoleMessageWriter(Console.WindowHeight);

            var brokerComGarantiaDeEntregaEOrdem = GerarBrokerConfig();

            brokerComGarantiaDeEntregaEOrdem.ProducerConfig.Acks = Acks.All;
            brokerComGarantiaDeEntregaEOrdem.ProducerConfig.MaxInFlight = 1;
            brokerComGarantiaDeEntregaEOrdem.ProducerConfig.MessageSendMaxRetries = 10000000;
            var producerComGarantiaDeEntregaEOrdem = new ProdutorAssincrono2(brokerComGarantiaDeEntregaEOrdem, console);

            while (true)
            {
                console.Write("Digite a mensagem a ser enviada ('SAIR' para finalizar): ", Infra.MessageType.Input);
                string cmd = Console.ReadLine();
                if (cmd.ToUpper() == "SAIR") return;

                await producerComGarantiaDeEntregaEOrdem.Produzir<Null, string>(_topico, null, cmd);
            }
        }

        private static async Task AtLeastOnce()
        {
            Console.WriteLine("At least once semantic.\nEste exemplo simula consumo com sucesso ou erro e o comportamento de releitura do mesmo offset, garantindo pelo menos um registro entregue e processado no destino.\nPressione CTRL+C para finalizar.");
            var a = new Semanticas.AtLeastOnce(GerarBrokerConfig(), _topico, _consumerGroup, new Infra.ColorMessageWriter());

            var cts = new System.Threading.CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                var task1 = a.IniciarProducao(cts);
                var task2 = a.IniciarConsumo(cts);

                await Task.WhenAll(task1, task2);
            }
            catch (OperationCanceledException) { }

            Console.WriteLine("Processamento finalizado. Pressione qualquer tecla para continuar.");
            Console.ReadKey();
        }

        private static async Task AtMostOnce()
        {
            Console.WriteLine("At most once semantic.\nEste exemplo simula consumo com sucesso ou erro no processamento, não lendo novamente para evitar duplicação no destino.\nPressione CTRL+C para finalizar.");
            var a = new Semanticas.AtMostOnce(GerarBrokerConfig(), _topico, _consumerGroup, new Infra.ColorMessageWriter());

            var cts = new System.Threading.CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                var task1 = a.IniciarProducao(cts);
                var task2 = a.IniciarConsumo(cts);

                await Task.WhenAll(task1, task2);
            }
            catch (OperationCanceledException) { }

            Console.WriteLine("Processamento finalizado. Pressione qualquer tecla para continuar.");
            Console.ReadKey();
        }

        private static async Task ExactlyOnce()
        {
            Console.WriteLine("Exactly Once semantic.\nEste exemplo simula consumo com sucesso ou erro no processamento, com tratativas para que a mensagem seja processada apenas 1 vez tanto no producer quanto consumer.\nPressione CTRL+C para finalizar.");
            var a = new Semanticas.ExatclyOnce(GerarBrokerConfig(), _topico, _consumerGroup, new Infra.ColorMessageWriter());

            var cts = new System.Threading.CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                var task1 = a.IniciarProducao(cts);
                var task2 = a.IniciarConsumo(cts);

                await Task.WhenAll(task1, task2);
            }
            catch (OperationCanceledException) { }

            Console.WriteLine("Processamento finalizado. Pressione qualquer tecla para continuar.");
            Console.ReadKey();
        }

        private static async Task CriacaoTopico()
        {
            var criadorTopico = new Administracao.CriacaoTopico(GerarBrokerConfig());

            Console.WriteLine("Digite o nome do tópico a ser criado:");
            string nomeTopico = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(nomeTopico)) {
                Console.WriteLine("Informe um nome válido.\nPressione qualquer tecla para continuar.");
                Console.ReadKey();
                return;
            }
            
            Console.WriteLine("Informe a quantidade de partições:");            
            int numParticoes;
            if (!int.TryParse(Console.ReadLine(), out numParticoes)) {
                Console.WriteLine("Informe um número válido.\nPressione qualquer tecla para continuar.");
                Console.ReadKey();
                return;
            }

            Console.WriteLine("Informe o fator de replicação:");
            short fatorReplicacao;
            if (!short.TryParse(Console.ReadLine(), out fatorReplicacao))
            {
                Console.WriteLine("Informe um número válido.\nPressione qualquer tecla para continuar.");
                Console.ReadKey();
                return;
            }

            await criadorTopico.CriarTopico(nomeTopico, numParticoes, fatorReplicacao, criadorTopico.ProducerConfig);
        }
    }
}
