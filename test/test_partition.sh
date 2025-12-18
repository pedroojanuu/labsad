#!/bin/bash

echo "Iniciando Prueba de Partición (Punto 7)"


# 1. Simular desconexión
echo "Deteniendo servicio nats-b..."
docker compose stop nats-b

# 2. Realizar cambio en el sitio A mientras B está fuera
echo "Site-A: Cambiando theme a 'dark'..."
nats kv put config theme dark --server localhost:4222

# 3. Intentar cambio en B
echo "Intentando cambio en Site-B (esto fallará por estar offline)..."
nats kv put config theme light --server localhost:5222 2>/dev/null || echo "Info: Site-B fuera de línea, cambio no registrado localmente."

# 4. Restaurar conexión
echo "Reiniciando nats-b..."
docker compose start nats-b

echo "Esperando sincronización..."
sleep 5

# 5. Verificar resultado
echo "Verificando convergencia..."
VALOR_A=$(nats kv get config theme --server localhost:4222 --raw)
VALOR_B=$(nats kv get config theme --server localhost:5222 --raw)

echo "Resultado en Site-A: $VALOR_A"
echo "Resultado en Site-B: $VALOR_B"

# La prueba es exitosa si los valores son idénticos (Convergencia)
if [ "$VALOR_A" == "$VALOR_B" ] && [ ! -z "$VALOR_A" ]; then
    echo "✔ ÉXITO: Los sitios han convergido al mismo estado."
else
    echo "✘ ERROR: Los sitios no coinciden."
fi
