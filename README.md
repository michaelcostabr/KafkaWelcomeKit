# Introdução
Este projeto tem como objetivo demonstrar o uso do Apache Kafka através da biblioteca da Confluent para .Net Core.
Foram implementados diversos cenários de uso cobrindo várias nuances de uma aplicação que trabalhe com streaming/mensageria.
Existem dois arquivos de configuração, appsettings.json, configurado para apontar para nosso ambiente Kafka Corporativo para Pocs, e também o arquivo appsettings.cloud.json, configurado para rodar uma instancia local do Kafka.

Na pasta "docs", podem ser apreciados alguns artigos que escrevi durante meus estudos da plataforma.

Veja abaixo os cenários iniciais:

## Produtor
* P1 - Produzir aguardando:
  Produzir uma mensagem de forma síncrona, ou seja, aguardando a conclusão da chamada
* P2 - Produzir de forma assíncrona:
  Produzir uma mensagem de forma assincrona
* P3 - Produzir de forma assíncrona e utilizando key:
  Produzir uma mensagem de forma assincrona, usando await
* P4 - Produzir com garantia de entrega e ordem:
  Produção com configurações que garantam a entrega e ordem
* P5 - Produzir sem garantias (Fire and Forget):
  Produção sem garantia de entrega. Obs: Até o fechamendo da primeira versão, aparentemente há um bug no driver, que somente entrega se estiver deburando passo-a-passo.
* P6 - Produzir validando Schema:
  Produzir de forma assíncrona, entretanto utilizando validação do Schema Registry durante a serialização da mensagem 
## Consumidor
* C1 - Consumir realizando commit manual:
  Consumir mensagens de um tópico sem realizar commit do offset
* C2 - Consumir realizando commit automático:
  Consumir mensagens de um tópico
* C3 - Consultar offsets disponíveis:
  Exibir quais os offsets ainda estão disponíveis, ou seja, não foram expurgados (por tempo ou por armazenamento)
* C4 - Consumir um offset específico:
  Consumir mensagem de um offset específico do tópico, lembrando que o range disponível é variável. A medida que o record é expurgado, o offset mínimo cresce.
* C5 - Consumir a partir de um offset específico:
  Consumir todas mensagens a partir do offset informado
* C6 - Consumir utilizando schema:
  Consumir mensagens de um tópico, serializando-a conforme declaração do Schema Registry
* C7 - Unsubcribe em tópico:
  Realizar unsubscribe do tópico, abandonando o consumer group.
## Semânticas de entrega de mensagem
* D1 - Semântica At least once:
  Exemplo onde um consumidor e um produtor trabalham em paralelo, para simular um cenário onde a semântica "Pelo menos uma" é atendida, ou seja, 
  o produtor tem a garantia de que a mensagem foi enviada e o consumidor somente realiza o commit da mesma após ter processado com sucesso suas atividades após a leitura. 
* D2 - Semântica At most once:
  Exemplo onde um consumidor e um produtor trabalham em paralelo, para simular um cenário onde a semântica "No máximo uma" é atendida, ou seja,
  o produtor para evitar duplicação de mensagem no broker, espera apenas 1 ack, e o consumidor já realiza o commit assim que retira do tópico. 
* D3 - Semântica Exactly Once:
  Exemplo onde um consumidor e um produtor trabalham em paralelo, para simular um cenário onde a semântica "Exatamente uma" é atendida, ou seja, 
  o produtor tem a garantia de que a mensagem foi enviada, deduplicada e o consumidor somente realiza o commit da mesma após ter processado com sucesso suas atividades após a leitura.
  Além disso, há no cliente uma tratativa para evitar duplicação de dados gerados após o recebimento da mensagem. Neste exemplo, é simulada uma transação no banco, que é desfeita caso haja algum erro, e o commit do offset não é realizado.

## Administração
* A1 - Criação de tópico:
  Criar um tópico novo, definindo o nome, a quantidade de partições e o fator de replicação.

# Começando
Na execução do programa, será apresentado um menu com as opções disponíveis. Selecionando uma opção, o programa iniciará a interação do caso de uso escolhido.
Para sair, a qualquer momento pressione CTRL+C, ou quando estiver em um menu, digite 'SAIR'

O programa foi desenvolvido com algumas variáveis (appsettings.json), para definir o nome do tópico e consumer group padrão. Se for alterar o tópico, certifique-se de criar o mesmo antes de testar o programa.

# Build and Test
dotnet build
dotnet run

Para gerar os POCO's baseados nos schemas, é necessário instalar o avrogen, através do seguinte comando:

```
dotnet tool install --global Confluent.Apache.Avro.AvroGen
```

Feito isso, baixar o schema do tópico em questão (arquivo .asvc) e rodar o comando abaixo:

```
avrogen -s ./schema.avsc .
```
Será gerada uma classe para ser importada no projeto.


# Contribute
Este projeto foi criado como um Kit de Boas Vindas para a implantação do Apache Kafka, e foi concebido orientado aos Guidelines inicias descritos na wiki do movimento ágil.
A medida que mais times utilizem o produto, novos desafios serão descobertos e você está convidado a evoluir este projeto, seja encontrando/corrigindo bugs ou implementando novos casos de uso.

Dúvidas e sugestões podem ser enviadas para Michael Costa - michael.costa@localiza.com
