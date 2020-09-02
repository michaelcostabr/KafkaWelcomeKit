Para gerar os POCO's baseados nos schemas, é necessário instalar o avrogen, através do seguinte comando:

```
dotnet tool install --global Confluent.Apache.Avro.AvroGen
```

Feito isso, baixar o schema do tópico em questão (arquivo .asvc) e rodar o comando abaixo:

```
avrogen -s ./schema.asvc .
```
Será gerada uma classe para ser importada no projeto.