kafka-acls --bootstrap-server localhost:9092 --command-config /etc/kafka/connect.properties --add --allow-principal User:public --operation READ --topic ratings
kafka-acls --bootstrap-server localhost:9092 --command-config /etc/kafka/connect.properties --add --allow-principal User:admin --operation WRITE --topic ratings
