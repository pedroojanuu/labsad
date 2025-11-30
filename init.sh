docker compose up -d

sleep 10

nats --server localhost:3222 server ping    # nats-hub
nats --server localhost:4222 server ping    # nats-a
nats --server localhost:5222 server ping    # nats-b

nats kv add config --server localhost:4222
nats kv add config --server localhost:5222

nats --server localhost:3222 stream add KV_REPLICATION \
  --subjects rep.kv.ops \
  --storage file \
  --retention limits \
  --discard old \
  --replicas 1 \
  --defaults

# --storage file permite que los mensajes del stream persistan tras reiniciar el contendedor (ver volúmenes de nats-hub en docker-compose.yaml)
