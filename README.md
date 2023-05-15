Tutorials to run Go-Kafka-Client
docker compose up -d
go run consumer/worker.go
go run producer/producer.go

curl --location --request POST '0.0.0.0:3000/api/v1/comments' --header 'Content-Type: application/json' --data-raw '{ "text":"Bank microservice" }'