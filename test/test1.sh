#!/bin/bash

echo "Script test2 iniciado"
echo "Obsérvese la lógica de los CRDTs en las ventanas de los agentes"
sleep 5

echo "Insertando value white en site-b"
nats kv put config theme "white" -s nats://localhost:5222

sleep 3

echo "Insertando value white en site-a"
nats kv put config theme "white" -s nats://localhost:4222

sleep 4

echo "Insertando value black en site-b"
nats kv put config theme "black" -s nats://localhost:5222

sleep 5

echo "Insertando value blue en site-a"
nats kv put config theme "blue" -s nats://localhost:4222