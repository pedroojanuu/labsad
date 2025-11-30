package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
)

// Estructura de la operación CRDT
type CRDTOp struct {
	Op     string `json:"op"` // "put" o "del"
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Value  []byte `json:"value"`
	Ts     int64  `json:"ts"`      // Timestamp unix
	NodeID string `json:"node_id"` // Identificador del nodo (site-a, site-b)
}

func main() {
	// Parámetros de línea de comandos
	natsURL := flag.String("nats-url", "nats://localhost:4222", "URL de NATS")
	nodeID := flag.String("node-id", "site-a", "ID único de este nodo")
	flag.Parse()

	// Parámetros estáticos
	bucketName := "config"     // Bucket del KV local
	repSubject := "rep.kv.ops" // Tópico de replicación

	// Conexión a NATS
	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatal(err)
	}

	// Conexión al JetStream local (para KV storage)
	jsLocal, _ := nc.JetStream()
	fmt.Printf("[%s] Conectado al JetStream local en %s\n", *nodeID, *natsURL)

	// Conexión al JetStream del hub, del que cada nodo es una hoja (para subject de replicación)
	jsHub, _ := nc.JetStream(nats.Domain("hub"))
	fmt.Printf("[%s] Conectado al JetStream del hub para replicación\n", *nodeID)

	// Obtener el KV store local
	kv, err := jsLocal.KeyValue(bucketName)
	if err != nil {
		log.Fatalf("[%s] Error accediendo al KV '%s': %v", *nodeID, bucketName, err)
	}
	fmt.Printf("[%s] Obtuvo KV local %s\n", *nodeID, bucketName)

	// =========================================================================
	// PARTE 1: RECIBIR OPERACIONES REMOTAS (SUSCRIPCIÓN)
	// =========================================================================
	_, err = jsHub.Subscribe(repSubject, func(m *nats.Msg) {
		var op CRDTOp
		if err := json.Unmarshal(m.Data, &op); err != nil {
			return
		}

		// Ignorar eco local (si el mensaje vino de mí mismo)
		if op.NodeID == *nodeID {
			m.Ack()
			return
		}

		fmt.Printf("[%s] Recibida OP remota (%s) de %s: %s (TS: %d)\n", *nodeID, op.Op, op.NodeID, op.Key, op.Ts)

		// Lógica LWW (Last-Writer-Wins) y Resolución de Conflictos
		// Obtenemos el valor actual local para comparar valores y timestamps
		entry, err := kv.Get(op.Key)

		doUpdate := false
		switch err {
			case nats.ErrKeyNotFound:
				if op.Op != "del" {
					// Si no existe localmente y la operación no es DELETE, aplicamos el remoto directamente - no hay conflicto
					doUpdate = true
				} else {
					log.Printf("[%s]	IGNORANDO (Operación DELETE sobre valor que ya no existe en KV local)", *nodeID)
				}
			case nil:
				// Si existe, verificamos si es el mismo valor para romper bucles de replicación
				// Si el valor binario es igual, no vale la pena verificar timestamps
				if bytes.Equal(entry.Value(), op.Value) {
					log.Printf("[%s]	IGNORANDO (Valor igual): %s", *nodeID, op.Key)
					// doUpdate se mantiene falso, así que el código caerá en el "else" final y hará Ack.
				} else {
					// Si el valor es diferente, aplicamos LWW:
					// Gana si: (ts_remoto > ts_local) OR (ts_remoto == ts_local AND node_id_remoto > node_id_local)
					localTs := entry.Created().Unix()

					if op.Ts > localTs || (op.Ts == localTs && op.NodeID > *nodeID) {
						doUpdate = true
					} else {
						log.Printf("[%s]	IGNORANDO (Gana local)", *nodeID)
					}
				}
			default:
				// Error inesperado leyendo el KV
				log.Printf("[%s]	Error leyendo KV local para key %s: %v. Reintentando...", *nodeID, op.Key, err)
				m.Nak() // Pide al servidor que reenvíe el mensaje
				return
		}

		if doUpdate {
			log.Printf("[%s]	APLICANDO (Gana remoto): %s = %s", *nodeID, op.Key, string(op.Value))

			switch op.Op {
				case "put":
					kv.Put(op.Key, op.Value)
				case "del":
					kv.Delete(op.Key)
			}
		}

		m.Ack() // Confirmar recepción de mensaje después de procesarla
	}, nats.Durable(*nodeID), nats.ManualAck())

	if err != nil {
		log.Fatalf("[%s] Erro al suscribirse al tópico de replicación: %v", *nodeID, err)
	}

	// =========================================================================
	// PARTE 2: VIGILAR CAMBIOS LOCALES (WATCH)
	// =========================================================================
	watcher, _ := kv.WatchAll()
	go func() {
		for update := range watcher.Updates() {
			if update == nil {
				continue
			}

			// Crear operación CRDT
			op := CRDTOp{
				Bucket: bucketName,
				Key:    update.Key(),
				Ts:     update.Created().Unix(), // Usamos el tiempo de creación del registro
				NodeID: *nodeID,
			}

			if update.Operation() == nats.KeyValuePut {
				op.Op = "put"
				op.Value = update.Value()
			} else {
				op.Op = "del"
			}

			// Serializar y publicar en rep.kv.ops
			data, _ := json.Marshal(op)
			nc.Publish(repSubject, data)

			// Solo imprimir logs para depuración visual
			fmt.Printf("[%s] Cambio Local detectado: %s. Publicado a %s\n", *nodeID, op.Key, repSubject)
		}
	}()

	// Mantener el programa corriendo
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
