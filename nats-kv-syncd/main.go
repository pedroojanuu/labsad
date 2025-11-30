package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	// "time"

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

	// Conexión a NATS y JetStream
	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatal(err)
	}
	js, _ := nc.JetStream()

	// Obtener el KV store local
	kv, err := js.KeyValue(bucketName)
	if err != nil {
		log.Fatalf("Error accediendo al KV '%s': %v", bucketName, err)
	}
	fmt.Printf("Agente iniciado en %s [%s] conectado a %s\n", *nodeID, bucketName, *natsURL)

	// =========================================================================
	// PARTE 1: RECIBIR OPERACIONES REMOTAS (SUSCRIPCIÓN)
	// Cada nodo se suscribe al repSubject. Dado que cada servidor NATS A/B
	// actúa como hoja del hub, el repSubject en realidad está en el hub, pero
	// desde el punto de vista de cada agente, basta con interactuar con su
	// servidor NATS
	// =========================================================================
	_, err = js.Subscribe(repSubject, func(m *nats.Msg) {
		var op CRDTOp
		if err := json.Unmarshal(m.Data, &op); err != nil {
			return
		}

		// Ignorar eco local (si el mensaje vino de mí mismo)
		if op.NodeID == *nodeID {
			return
		}

		fmt.Printf("[%s] Recibida OP remota de %s: %s (TS: %d)\n", *nodeID, op.NodeID, op.Key, op.Ts)

		// Lógica LWW (Last-Writer-Wins) y Resolución de Conflictos
		// Obtenemos el valor actual local para comparar timestamps
		entry, err := kv.Get(op.Key)

		doUpdate := false
		if err == nats.ErrKeyNotFound {
			// Si no existe localmente, aplicamos el remoto directamente -no hay conflicto
			doUpdate = true
		} else if err == nil {
			// Si existe -hay conflicto-, aplicamos la regla:
			// Gana si: (ts_remoto > ts_local) OR (ts_remoto == ts_local AND node_id_remoto > node_id_local)
			localTs := entry.Created().Unix() // Usamos Created como aproximación del TS lógico

			if op.Ts > localTs || (op.Ts == localTs && op.NodeID > *nodeID) {
				doUpdate = true
			}
		}

		if doUpdate {
			log.Printf("--> APLICANDO CAMBIO REMOTO (Gana remoto): %s = %s", op.Key, string(op.Value))
			
			switch op.Op {
				case "put":
					kv.Put(op.Key, op.Value)
				case "del":
					kv.Delete(op.Key)
			}
		} else {
			log.Printf("--- IGNORANDO CAMBIO REMOTO (Gana local o es antiguo)")
		}

		m.Ack() // Confirmar recepción de mensaje después de procesarla
	}, nats.Durable(*nodeID), nats.ManualAck())

	if err != nil {
		log.Fatalf("Erro al suscribirse al tópico de replicación: %v", err)
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
			fmt.Printf(">> Cambio Local detectado: %s. Publicado a %s\n", op.Key, repSubject)
		}
	}()

	// Mantener el programa corriendo
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
