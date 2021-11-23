# Kafka MongoDB Transforms

Project to create useful Single Message Transforms (SMTs) to handle MongoDB payloads with Kafka Connect.

## SMTs

### ByteToUuidString

Debezium connector for MongoDB works well with `ObjectID` keys, casting them properly as Strings, but for `UUID` keys, as MongoDB stores them as `Binary data`, the default MongoDB JSON serializer cast its value to `byte[]`, which is returned as a `base64` string if no additional converter is added.

A raw hex dump from the UUID key can be retrieved by using Debezium's [MongoDB New Document State Extraction](https://debezium.io/documentation/reference/transformations/mongodb-event-flattening.html) SMT, but that raw hex dump still requires formatting to follow UUID layout, as seen in section [4.1.2 of the RFC 4122](https://www.ietf.org/rfc/rfc4122.txt).

## Building

Open a new console, go to your project folder, then run:

```shell
mvn install
```

This will generate two JAR packages at your Maven local cache folder, usuarlly:

```
$HOME/.m2/repository/systems/edgar/kafka/kafka-connect-mongodb-transforms/1.0.0-SNAPSHOT
```

Copy the file `kafka-connect-mongodb-transforms-1.0.0-SNAPSHOT-jar-with-dependencies.jar` to the `plugins` folder of your Kafka Connect server.

## Using

To use the ByteToUuidString SMT at your connector to transform the id field from byte to UUID string, use the following properties:

```json
{
  //...
  "transforms": "byteToUuid",
  "transforms.uuid.type": "systems.edgar.kafka.mongodb.transforms.ByteToUuidString$Value",
  "transforms.uuid.field.name": "id",
  //...
}
```
