#!/bin/bash

echo "Script test2 iniciado"
echo "Insertando value "white" en "site-b"
nats kv put config theme "white" -s nats://localhost:5222
