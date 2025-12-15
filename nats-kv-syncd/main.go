package main

import (
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
	Value  string `json:"value"`
	Ts     int64  `json:"ts"`      // Timestamp unix
	NodeID string `json:"node_id"` // Identificador del nodo (site-a, site-b)
}

// Estructura de metadatos que se almacena en el KV local
type StoredCRDT struct {
	Value  string `json:"value"`   // El valor de configuración real
	Ts     int64  `json:"ts"`      // Contador Lógico persistente
	NodeID string `json:"node_id"` // ID del nodo que realizó el último cambio
}

func main() {
	// Parámetros de línea de comandos
	natsURL := flag.String("nats-url", "nats://localhost:4222", "URL de NATS")
	nodeID := flag.String("node-id", "site-a", "ID único de este nodo")
	flag.Parse()

	// Parámetros estáticos
	bucketName := "config"               // Bucket del KV local
	repSubject := "rep.kv.ops"           // Tópico de replicación
	const metaBucketName = "config_meta" // Bucket para metadatos
	const counterKey = "logical_clock"   // Key para el contador lógico

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
	kv, err := jsLocal.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: bucketName, // "config"
	})
	if err != nil {
		log.Fatalf("[%s] Error accediendo al KV '%s': %v", *nodeID, bucketName, err)
	}
	fmt.Printf("[%s] Obtuvo KV local %s\n", *nodeID, bucketName)

	// Obtener el KV store de metadatos local (solo para el contador del nodo)
	metaKv, err := jsLocal.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: metaBucketName, // "config_meta"
	})
	if err != nil {
		log.Fatalf("[%s] Error accediendo al KV meta '%s': %v", *nodeID, metaBucketName, err)
	}
	fmt.Printf("[%s] Obtuvo KV meta %s\n", *nodeID, metaBucketName)

	// -------------------------------------------------------------------------
	// CARGA DEL ESTADO PERSISTENTE DEL RELOJ LÓGICO
	// -------------------------------------------------------------------------
	var localCounter int64 = 0
	counterEntry, err := metaKv.Get(counterKey)
	if err == nil {
		// Si existe, cargar el contador persistente
		fmt.Sscanf(string(counterEntry.Value()), "%d", &localCounter)
		fmt.Printf("[%s] Reloj Lógico cargado desde KV: %d\n", *nodeID, localCounter)
	} else {
		fmt.Printf("[%s] Iniciando Reloj Lógico en: %d\n", *nodeID, localCounter)
	}

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
		localStored := StoredCRDT{}
		// Intentar obtener metadatos locales CRDT si la clave existe.
		if err == nil {
			if json.Unmarshal(entry.Value(), &localStored) != nil {
				// Si no se puede leer el JSON CRDT del valor local, se trata como no existente/corrupto.
				log.Printf("[%s] ERROR: No se pudo deserializar StoredCRDT para %s. Tratando como valor no existente.", *nodeID, op.Key)
				err = nats.ErrKeyNotFound // Se fuerza el flujo de 'key not found'
			}
		}
		doUpdate := false
		switch err {
		case nats.ErrKeyNotFound:
			if op.Op != "del" {
				// Si no existe localmente y la operación no es DELETE, se aplica el remoto directamente - no hay conflicto
				doUpdate = true
			} else {
				log.Printf("[%s]	IGNORANDO (Operación DELETE sobre valor que ya no existe en KV local)", *nodeID)
			}
		case nil:
			// Si existe, se verifica si es el mismo valor para romper bucles de replicación
			if localStored.Value == op.Value && localStored.Ts == op.Ts && localStored.NodeID == op.NodeID {
				log.Printf("[%s]	IGNORANDO (Mismo valor y metadatos - Eco/Repetición): %s", *nodeID, op.Key)
			} else {
				// Si el valor es diferente, se aplica LWW:
				// Gana si: (ts_remoto > ts_local) OR (ts_remoto == ts_local AND node_id_remoto > node_id_local)
				localTs := localStored.Ts
				localNodeID := localStored.NodeID

				if op.Ts > localTs || (op.Ts == localTs && op.NodeID > localNodeID) {
					doUpdate = true
				} else {
					log.Printf("[%s]	IGNORANDO (Gana local - Ts %d vs %d, Node %s vs %s)", *nodeID, localTs, op.Ts, localNodeID, op.NodeID)
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

			// Lógica de avance del reloj lógico: Si el remoto Ts es mayor que mi contador local
			if op.Ts > localCounter {
				localCounter = op.Ts
				counterValue := []byte(fmt.Sprintf("%d", localCounter))

				// Persistir el nuevo valor del contador lógico en el KV de metadatos
				if _, err := metaKv.Put(counterKey, counterValue); err != nil {
					log.Printf("[%s] ERROR al persistir el contador actualizado: %v", *nodeID, err)
				} else {
					log.Printf("[%s] Reloj lógico avanzado a %d (basado en remoto).", *nodeID, localCounter)
				}
			}

			switch op.Op {
			case "put":
				newStored := StoredCRDT{
					Value:  op.Value,
					Ts:     op.Ts,
					NodeID: op.NodeID,
				}
				data, _ := json.Marshal(newStored)

				if _, err := kv.Put(op.Key, data); err != nil {
					log.Printf("[%s] ERROR: Falló al guardar KV en Suscriptor (remoto): %v", *nodeID, err)
				}

			case "del":
				kv.Delete(op.Key)
			}
		}

		m.Ack() // Confirmar recepción de mensaje después de procesarla
	}, nats.Durable(*nodeID), nats.ManualAck())

	if err != nil {
		log.Fatalf("[%s] Error al suscribirse al tópico de replicación: %v", *nodeID, err)
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

			// DETECCIÓN DE ECO
			if update.Operation() == nats.KeyValuePut {
				var stored StoredCRDT
				if json.Unmarshal(update.Value(), &stored) == nil {
					// Si es un JSON CRDT válido, ignorar la replicación para evitar bucles.
					continue
				}
			}

			localCounter++ // Incrementar el contador lógico local
			counterValue := []byte(fmt.Sprintf("%d", localCounter))

			// Persistir el nuevo valor del contador lógico en el KV de metadatos
			if _, err := metaKv.Put(counterKey, counterValue); err != nil {
				log.Printf("[%s] Error guardando contador lógico en KV meta: %v", *nodeID, err)
				continue
			}

			// Crear operación CRDT
			op := CRDTOp{
				Bucket: bucketName,
				Key:    update.Key(),
				Ts:     localCounter,
				NodeID: *nodeID,
			}

			if update.Operation() == nats.KeyValuePut {
				op.Op = "put"
			} else {
				op.Op = "del"
			}

			if update.Operation() == nats.KeyValuePut {
				// El valor para la publicación es el valor puro detectado (convertido a string).
				op.Value = string(update.Value())

				// Se crea la nueva estructura StoredCRDT con los metadatos de esta operación.
				newStored := StoredCRDT{
					Value:  op.Value, // El valor puro (e.g., "dark")
					Ts:     op.Ts,
					NodeID: op.NodeID,
				}
				storedJSON, _ := json.Marshal(newStored)

				// Se escribe el JSON de StoredCRDT de vuelta al KV local (kv).
				// Esto garantiza que la próxima lectura de KV obtenga el Ts y NodeID correctos.
				if _, err := kv.Put(op.Key, storedJSON); err != nil {
					log.Printf("[%s] ERROR al parchear KV local con metadatos: %v", *nodeID, err)
					continue
				}
			} else {
				// Para 'DELETE' no hay parcheo local, simplemente se publica la operación de borrado.
			}

			// Serializar y publicar
			data, _ := json.Marshal(op)
			nc.Publish(repSubject, data)

			fmt.Printf("[%s] Cambio Local detectado: %s (TS: %d). Publicado a %s\n", *nodeID, op.Key, op.Ts, repSubject)
		}
	}()

	// Mantener el programa corriendo
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
