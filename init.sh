docker compose up -d

sleep 30

nats --server localhost:4222 server ping
nats --server localhost:5222 server ping

nats kv add config --server localhost:4222
nats kv add config --server localhost:5222
