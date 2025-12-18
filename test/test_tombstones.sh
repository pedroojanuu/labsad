#!/bin/bash

echo "INICIANDO PRUEBA"

KEY="estado_tombstone"
SERVER_A="localhost:4222"
SERVER_B="localhost:5222"
SERVER_HUB="localhost:3222"

# 0. Limpieza previa
echo "Limpiando estado anterior"

nats kv del config $KEY --server $SERVER_A -f

echo ""
echo "Site-A: Crear dato inicial 'vivo'"
# Ahora verás el mensaje "Key/Value pair saved"
nats kv put config $KEY "vivo" --server $SERVER_A

echo "Esperando replicación"
sleep 1

echo ""
echo "Site-A: Borrar clave (Generando Tombstone)"
# Verás el mensaje "Key deleted"
nats kv del config $KEY --server $SERVER_A -f

echo "Esperando propagación"
sleep 1

echo ""
echo "Site-B: Comprobar persistencia"
# Leemos el valor y lo guardamos, pero TE LO ENSEÑO inmediatamente
VALOR_B=$(nats kv get config $KEY --server $SERVER_B 2>&1)

echo "LO QUE HA RESPONDIDO NATS EN SITE-B:"
echo "$VALOR_B"
sleep 1

# Comprobación automática
if [[ "$VALOR_B" == *"deleted\": true"* ]] || [[ "$VALOR_B" == *"deleted\":true"* ]]; then
    echo "El sistema tiene el Tombstone correcto."
else
    echo "No veo el tombstone."
    exit 1
fi

echo ""
echo "Lanzando Ataque Zombie (TS=1)"
# Verás el mensaje "Published X bytes to..."
nats pub rep.kv.ops "{\"op\":\"put\", \"bucket\":\"config\", \"key\":\"$KEY\", \"value\":\"ZOMBIE\", \"ts\": 1, \"node_id\":\"site-hacker\"}" --server $SERVER_HUB

echo "Esperando"
sleep 1

echo ""
echo "Verificando que el zombie ha fallado "
VALOR_POST_ATAQUE=$(nats kv get config $KEY --server $SERVER_B 2>&1)
echo "VALOR ACTUAL EN B:"
echo "$VALOR_POST_ATAQUE"

if [[ "$VALOR_POST_ATAQUE" == *"deleted\": true"* ]] || [[ "$VALOR_POST_ATAQUE" == *"deleted\":true"* ]]; then
     echo "El zombie fue ignorado correctamente."
else
     echo "El zombie ha ganado."
     exit 1
fi

echo ""
echo "Resucitando dato"
nats kv put config $KEY "resucitado" --server $SERVER_A

echo "Esperando sincronización..."
sleep 1

echo ""
echo "Site-B: Lectura Final"
VALOR_FINAL=$(nats kv get config $KEY --server $SERVER_B --raw 2>/dev/null)
echo "VALOR FINAL:"
echo "$VALOR_FINAL"

if [[ "$VALOR_FINAL" == *"resucitado"* ]]; then
    echo "completado."
else
    echo "ERROR."
fi
