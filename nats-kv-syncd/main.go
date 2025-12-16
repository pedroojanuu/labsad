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
	Deleted bool   `json:"deleted,omitempty"` //Flag para Tombstone
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
		// Manejo de KeyNotFound
		doUpdate := false
		// Manejo de KeyNotFound
		if err == nats.ErrKeyNotFound {
			// Si no tengo el dato, SIEMPRE aplico lo que venga (sea Put o Del/Tombstone)
			doUpdate = true
		} else {
			// CRDT LWW: Resolución de conflictos
			// Gana si (Remoto.Ts > Local.Ts) O (Empate de tiempo y ID remoto es mayor)
			if op.Ts > localStored.Ts || (op.Ts == localStored.Ts && op.NodeID > localStored.NodeID) {
				doUpdate = true
			} else {
				// [LOG AÑADIDO]: Aquí verás si un ataque zombie es rechazado
				log.Printf("[%s] IGNORANDO (Gana local - Ts Local:%d vs Remoto:%d)", *nodeID, localStored.Ts, op.Ts)
			}
		}

		if doUpdate {
			// Avanzar reloj lógico si el remoto es más futuro
			if op.Ts > localCounter {
				localCounter = op.Ts
				metaKv.Put(counterKey, []byte(fmt.Sprintf("%d", localCounter)))
			}

			// Lógica unificada. Ya no usamos kv.Delete, siempre kv.Put
			newStored := StoredCRDT{
				
				Ts:      op.Ts,
				NodeID:  op.NodeID,
				
			}

			if op.Op == "del" {
				newStored.Deleted = true // Marcamos como Tombstone
				newStored.Value = ""     // Limpiamos valor para ahorrar espacio
				log.Printf("[%s] APLICANDO TOMBSTONE (Remoto): %s", *nodeID, op.Key)
			} else {
				newStored.Deleted = false
				newStored.Value = op.Value
				log.Printf("[%s] APLICANDO PUT (Remoto): %s", *nodeID, op.Key)
			}
			
			bytes, _ := json.Marshal(newStored)
			kv.Put(op.Key, bytes) // Guardamos (sea valor o lápida)
			log.Printf("[%s] APLICADO %s (Gana remoto)", *nodeID, op.Op)
		}
		m.Ack()
	}, nats.Durable(*nodeID), nats.ManualAck())

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
			// Si es un PUT, miramos si es un formato interno nuestro
			if update.Operation() == nats.KeyValuePut {
				var stored StoredCRDT
				if json.Unmarshal(update.Value(), &stored) == nil {
					// Si tiene flag Deleted true, es un tombstone que acabamos de escribir nosotros -> IGNORAR
					if stored.Deleted {
						continue
					}
					// Si tiene metadatos completos y valor, es un parche local -> IGNORAR
					if stored.Ts > 0 && stored.NodeID != "" {
						continue
					}
				}
			}

			localCounter++ // Incrementar el contador lógico local
			metaKv.Put(counterKey, []byte(fmt.Sprintf("%d", localCounter)))

			// Crear operación CRDT
			op := CRDTOp{
				Bucket: bucketName,
				Key:    update.Key(),
				Ts:     localCounter,
				NodeID: *nodeID,
			}

			//Lógica de intercepción de Borrados
			if update.Operation() == nats.KeyValueDelete || update.Operation() == nats.KeyValuePurge {
				// El usuario hizo 'nats kv del'. 
				// 1. Preparamos mensaje 'del' para la red
				op.Op = "del"
				
				// 2.Resucitamos el dato localmente como Tombstone inmediatamente.
				// Esto dispara el Watcher otra vez (como PUT), pero el filtro de arriba (stored.Deleted) lo frenará.
				tombstone, _ := json.Marshal(StoredCRDT{
					Ts: op.Ts, NodeID: op.NodeID, Deleted: true,
				})
				kv.Put(op.Key, tombstone) 
				
				log.Printf("[%s] Borrado físico detectado -> Convertido a Tombstone local", *nodeID)

			} else {
				// Es un PUT normal del usuario
				op.Op = "put"
				op.Value = string(update.Value())
				
				// Parcheamos el dato local con sus metadatos
				patch, _ := json.Marshal(StoredCRDT{
					Value: op.Value, Ts: op.Ts, NodeID: op.NodeID, Deleted: false,
				})
				kv.Put(op.Key, patch)
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
