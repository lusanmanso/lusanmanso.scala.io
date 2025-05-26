- Requisito (Levantar un Broker de Kafka):
	- confluent local services kafka start
- Crear un topic
	- bin/kafka-topics --create --topic mi_tema --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
		--create: Indica que se debe crear un nuevo tema
		--topic: Especifica el nombre del tema
		--bootstrap-server: Especifica la lista de servidores de brokers Kafka
		--partitions: Número de particiones en el tema
		--replication-factor: Factor de replicación para las particiones
- Listar tópicos existentes
	- bin/kafka-topics --list --bootstrap-server localhost:9092
		--list: Muestra la lista de todos los temas existentes en el clúster
- Eliminar tópico
	- bin/kafka-topics --delete --topic mi_tema --bootstrap-server localhost:9092
		--delete: Elimina el tema especificado
- Definir número de particiones y factor de replicación
	- bin/kafka-topics --list --bootstrap-server localhost:9092
	- bin/kafka-topics --create --topic mi_tema --bootstrap-server localhost:9092 --partitions 2 --replication-factor 2
	- bin/kafka-topics --create --topic mi_tema --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
- Modificar la retención de mensajes
	- Requisito:
		- confluent local services kafka-rest start
	- confluent kafka topic update mi_tema --url http://localhost:8082 --config retention.ms=259200000
		--url: Kafka REST URL
		--config: Agrega una o modifica un parámetros de configuración, en este caso, retención de mensajes en milisegundos
- Detalles de un tema
	-	kafka-topics --describe --bootstrap-server localhost:9092 --topic mi_tema
- Producir mensajes
	- bin/kafka-console-producer --bootstrap-server localhost:9092 --topic mi_tema
	- bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --from-beginning
	- bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema
	- bin/kafka-console-producer --bootstrap-server localhost:9092 --topic mi_tema < /home/bigdata/microcuento.txt
- Consumir mensajes
	- bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --from-beginning
	- bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --from-beginning > /home/bigdata/salida.txt
		--from-beginning: offset=0 (histórico de mensajes)
- Consumir mensajes (Consumer Group)
	- bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --group mi_grupo
- Listar Consumer Groups
	- bin/kafka-consumer-groups --bootstrap-server localhost:9092 --list
- Describir Consumer Group
	- bin/kafka-consumer-groups --bootstrap-server localhost:9092 --group mi_grupo --describe
- Producir mensajes con claves
	- bin/kafka-console-producer --bootstrap-server localhost:9092 --topic mi_tema --property parse.key=true --property key.separator=":"
- Consumir mensajes con claves
	- bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --from-beginning --property print.key=true --property key.separator="-"

- Generador de datos de Confluence

confluent-hub install confluentinc/kafka-connect-datagen:latest
cat /home/bigdata/confluent-7.5.1/console-consumer-primitive-keys-values/primitives.json
bin/ksql-datagen schema=/home/bigdata/confluent-7.5.1/console-consumer-primitive-keys-values/primitives.json key-format=json value-format=json topic=mi_tema key=key_field iterations=10
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --property print.key=true --property key.separator="-"

- Leer desde mensajes a partir de un OFFSET

bin/kafka-consumer-groups --bootstrap-server localhost:9092 --group mi_grupo --describe
bin/ksql-datagen quickstart=orders iterations=10 topic=mi_tema key=orderid key-format=delimited
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --partition 0 --offset ???
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --partition 1 --offset ???
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mi_tema --group mi_grupo
