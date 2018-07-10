# Asgard Log Ingestor

Esse é o ingestor de logs de (quase) todas as apps que rodam no Asgard. "Quase" pois algumas apps precisam de um
processamento especial nos logs, por exemplo, Proxy reverso.

Esse código serve para apps que não precisam de nenhum tratamento nos logs, ou seja, basta indexar em algum lugar.


## Projetos incluídos nesse repositório

 - asgard-log-ingestor: Indexador de logs de aplicações que rodam no Asgard;
 - asgard-stats-indexer: Indexador de dados de uso de CPU/RAM de cada instância de cada App.


## Como rodar cada um dos projetos

Para rodar o log ingestor
```
$ python -m logsingestor
```

Para rodar o indexador de estatísticas de CPU/RAM

```
$ python -m statsindexer
```


## Env vars

Essas são as envs que o cóidigo precisa para rodar:

### Para o ingestor de logs de aplicação

* LOGS_RABBITMQ_HOST: Host onde vamos nos conectar para pegar mensagens. Não inclui a porta, apenas o IP;
* LOGS_RABBITMQ_USER: Usuário que se conectrá com a fila;
* LOGS_RABBITMQ_PWD: Senha que será usada;
* LOGS_QUEUE_NAMES: Lista separada por vírgula de todas as filas que serão consumidas.
* LOGS_RABBITMQ_VHOST: Vhost da fila que será usado na conexão com a fila. Se não passado, será usado vhost `/`;
* LOGS_RABBITMQ_PREFETCH: Prefetch usado para pegar mensagens da fila. Esse número de msgs é usado para **cada** fila que está sendo consumida.
* LOGS_BULK_SIZE: Tamanho do lote de mensagens que os handlers vão receber. O framework `async-worker` acumula mensagens até chegar esse numero e depois disso ele entrega **todas** as mensagens para o handler.

### Para o indexador de estatísticas

* STATS_RABBITMQ_HOST
* STATS_RABBITMQ_USER
* STATS_RABBITMQ_PWD
* STATS_RABBITMQ_PREFETCH
* STATS_RABBITMQ_VHOST 
* STATS_QUEUE_NAMES
* STATS_BULK_SIZE 

### Envs comuns aos projetos

* ASGARD_LOGINGESTOR_LOGLEVEL: Log level que o código usará em seus logs: Valores possíveis são os levels do Python: "INFO", "ERROR", ...

## Trabalhos futuros

Esse repositório surgiu como sendo um projeto individual, mas começamos a ver que outros projetos usavam a mesma base de código, por exemplo, o bulk indexer do elasticsearch. Então começamos a juntar masi comandos nesse mesmo repositório. Separações que ainda temos que fazer:

* Criar um novo módulo onde as coisas comuns podem ficar;
* Mover o `conf.py` para a raiz, fora do módulo `logsingestor`;



