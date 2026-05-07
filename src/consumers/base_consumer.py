"""
Clase base abstracta para consumidores de sensores IoT con Kafka.

DIFERENCIAS CLAVE vs RabbitMQ:
    - RabbitMQ (push): el broker empuja mensajes al consumidor via basic_consume.
    - Kafka (pull): el consumidor JALA mensajes activamente con poll().

    - RabbitMQ: ACK manual por mensaje (basic_ack).
    - Kafka: commit de offset. El consumidor le dice al broker "ya leí hasta aquí".

    - RabbitMQ: cada consumidor tiene su propia cola.
    - Kafka: los consumidores del mismo grupo comparten particiones del topic.

Principios aplicados:
    - SRP: solo gestiona el consumo de mensajes.
    - OCP: nuevos consumidores extienden sin modificar esta clase.
    - Template Method: run() define el algoritmo, subclases implementan process_reading().
"""
import logging
from abc import ABC, abstractmethod

from confluent_kafka import Consumer, KafkaError

from src.config import settings
from src.models.sensor_reading import SensorReading

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    """
    Consumidor base que implementa el patrón Template Method.

    Comparación con RabbitMQ:
        - En RabbitMQ: conexión → canal → declare queue → bind → basic_consume (push)
        - En Kafka: Consumer(config) → subscribe(topic) → poll() en loop (pull)

        El modelo pull de Kafka da más control al consumidor:
        puede decidir cuándo leer, cuántos mensajes procesar, y rebobinar si necesita.
    """

    def __init__(self, group_id: str, consumer_name: str):
        self._consumer_name = consumer_name
        self._group_id = group_id

        # Configuración del consumidor Kafka
        # En RabbitMQ esto era: ConnectionParameters + Channel + Queue
        # En Kafka es un diccionario con el group.id que identifica al grupo
        self._consumer_config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",  # Leer desde el inicio si no hay offset previo
            "enable.auto.commit": False,       # Commit manual (como ACK manual en RabbitMQ)
        }

    @abstractmethod
    def process_reading(self, reading: SensorReading) -> None:
        """Hook: cada consumidor implementa su lógica de procesamiento."""
        ...

    def run(self):
        """
        Template Method: define el ciclo de consumo.

        Diferencias vs RabbitMQ:
            1. No hay conexión/canal. Consumer maneja todo internamente.
            2. subscribe() reemplaza a queue_declare + queue_bind.
            3. poll() reemplaza a basic_consume (pull vs push).
            4. commit() reemplaza a basic_ack.
            5. close() es CRÍTICO: notifica al broker que el consumidor
               se va, para que reasigne sus particiones inmediatamente.
        """
        consumer = Consumer(self._consumer_config)
        consumer.subscribe([settings.TOPIC_NAME])

        logger.info(
            "Consumidor '%s' (grupo: %s) escuchando topic '%s'. Ctrl+C para detener.",
            self._consumer_name, self._group_id, settings.TOPIC_NAME,
        )

        try:
            while True:
                # poll() JALA un mensaje del broker
                # Timeout 1.0s: si no hay mensajes, espera hasta 1 segundo
                # En RabbitMQ, basic_consume era automático (push)
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    # No hay mensajes disponibles, continuar esperando
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Llegamos al final de la partición (normal)
                        logger.debug("Fin de partición %d", msg.partition())
                    else:
                        logger.error("Error de consumo: %s", msg.error())
                    continue

                # Deserializar el mensaje
                try:
                    reading = SensorReading.from_json(msg.value().decode("utf-8"))
                    self.process_reading(reading)

                    # Commit manual del offset (equivalente a basic_ack en RabbitMQ)
                    # Le dice al broker: "ya procesé hasta este mensaje"
                    consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error("Error procesando mensaje: %s", e)

        except KeyboardInterrupt:
            logger.info("Consumidor '%s' detenido por usuario.", self._consumer_name)
        finally:
            # close() es CRÍTICO en Kafka:
            # - Hace commit del último offset
            # - Notifica al broker para reasignar particiones (rebalance)
            # En RabbitMQ, esto era connection.close()
            consumer.close()
            logger.info("Consumidor '%s' cerrado correctamente.", self._consumer_name)
