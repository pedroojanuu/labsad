# SAD &mdash; Laboratorio: Sincronización de almacenes KV de NATS mediante CRDT

- Alberto Olcina Calabuig
- Julen García Muñoz
- Pedro Simão Januário Vieira

## Compilación y ejecución

**En el directorio raíz**, ejecutar ```./init.sh```. Esto arrancará 3 contenedores Docker (el servidor NATS de _site-a_, el de _site-b_ y el del _hub_ de replicación; según ```docker-compose.yaml```), comprobará la conexión a ellos y creará un _bucket_ ```config``` en cada uno de los servidores de los nodos y el _stream_ ```KV_REPLICATION``` para replicación en el _hub_, del que los dos primeros actuarán como _leaf nodes_.

**En la carpeta ```nats-kv-syncd```**, ejecutar ```go build```, que compilará el agente, creando el programa ```nats-kv-syncd```.

Para ejecutar ambos agentes (el de _site-a_ y el de _site-b_):

```sh
./nats-kv-syncd --nats-url nats://localhost:4222 --node-id site-a
./nats-kv-syncd --nats-url nats://localhost:5222 --node-id site-b
```

## Lógica CRDT

Lorem ipsum dolor sit amet.

## Almacenamiento de metadados

Lorem ipsum dolor sit amet.

## Pruebas

Lorem ipsum dolor sit amet.


---
Alberto Olcina, Julen García y Pedro Januário

diciembre 2025
