"""
Clase base abstracta para productores de sensores IoT con Kafka.

DIFERENCIAS CLAVE vs RabbitMQ:
    - RabbitMQ: conexión síncrona con pika, publicación bloqueante con basic_publish.
    - Kafka: productor asíncrono con buffer interno. Los mensajes se acumulan
      en un buffer y se envían en lotes (batching) para mayor rendimiento.
      Se requiere llamar a flush() o poll() para confirmar la entrega.

Principios aplicados:
    - SRP: solo gestiona la producción de mensajes.
    - OCP: nuevos sensores extienden sin modificar esta clase.
    - Template Method: run() define el algoritmo, subclases implementan _generate_reading().
"""
import logging
import time
from abc import ABC, abstractmethod

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.config import settings
from src.models.sensor_reading import SensorReading

logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    """
    Productor base que implementa el patrón Template Method.

    Comparación con RabbitMQ:
        - En RabbitMQ, abríamos una conexión + canal, declarábamos el exchange,
          y publicábamos con basic_publish (síncrono).
        - En Kafka, creamos un Producer con configuración, y producimos al topic.
          No hay exchanges ni routing keys: el productor elige el topic directamente.
          La 'key' del mensaje determina a qué partición va (hash de la key).
    """

    def __init__(self, sensor_type: str):
        self._sensor_type = sensor_type
        sensor_config = settings.SENSORS[sensor_type]
        self._sensor_id = sensor_config["sensor_id"]
        self._location = sensor_config["location"]
        self._interval = sensor_config["interval_seconds"]
        self._unit = sensor_config["unit"]

        # Configuración del productor Kafka
        # En RabbitMQ esto era: ConnectionParameters + Channel
        # En Kafka es un diccionario de configuración
        self._producer_config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": self._sensor_id,
        }

    @abstractmethod
    def _generate_reading(self) -> SensorReading:
        """Hook: cada sensor implementa su lógica de generación."""
        ...

    def _delivery_callback(self, err, msg):
        """
        Callback de confirmación de entrega.

        En RabbitMQ no teníamos esto porque basic_publish era síncrono.
        En Kafka, la producción es ASÍNCRONA: el mensaje va a un buffer
        interno y se envía en lote. Este callback nos notifica si la
        entrega fue exitosa o falló.
        """
        if err:
            logger.error("Error entregando mensaje: %s", err)
        else:
            logger.info(
                "[%s] Enviado → topic=%s partition=%d offset=%d | valor=%.2f%s",
                self._sensor_id,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                SensorReading.from_json(msg.value().decode("utf-8")).value,
                self._unit,
            )

    def _ensure_topic(self):
        """
        Crea el topic si no existe.

        Equivalente a exchange_declare() en RabbitMQ.
        En RabbitMQ, el exchange se creaba automáticamente al declararlo.
        En Kafka, usamos AdminClient para verificar/crear el topic.
        Esto garantiza que cualquiera que clone el proyecto pueda
        ejecutarlo sin pasos manuales adicionales.
        """
        admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin.list_topics(timeout=5)

        if settings.TOPIC_NAME not in metadata.topics:
            topic = NewTopic(
                settings.TOPIC_NAME,
                num_partitions=settings.NUM_PARTITIONS,
                replication_factor=settings.REPLICATION_FACTOR,
            )
            futures = admin.create_topics([topic])
            for topic_name, future in futures.items():
                future.result()  # Espera a que se cree
                logger.info("Topic '%s' creado automáticamente.", topic_name)
        else:
            logger.info("Topic '%s' ya existe.", settings.TOPIC_NAME)

    def run(self):
        """
        Template Method: define el ciclo de vida del productor.

        Diferencias vs RabbitMQ:
            1. No hay 'conexión' ni 'canal'. El Producer de Kafka
               maneja conexiones internamente.
            2. _ensure_topic() reemplaza a exchange_declare().
            3. produce() es asíncrono: encola el mensaje en un buffer.
            4. poll() dispara callbacks de entregas completadas.
            5. flush() al salir asegura que todos los mensajes pendientes se envíen.
        """
        self._ensure_topic()
        producer = Producer(self._producer_config)
        logger.info("Productor '%s' iniciado.", self._sensor_id)

        try:
            while True:
                reading = self._generate_reading()

                # produce() es ASÍNCRONO: no envía inmediatamente
                # El mensaje se acumula en un buffer interno
                producer.produce(
                    topic=settings.TOPIC_NAME,
                    key=self._sensor_type,          # Determina la partición
                    value=reading.to_json(),         # Payload en JSON
                    callback=self._delivery_callback,
                )

                # poll() procesa callbacks de entregas anteriores
                # Parámetro: timeout en segundos (0 = no bloqueante)
                producer.poll(0)

                time.sleep(self._interval)

        except KeyboardInterrupt:
            logger.info("Productor '%s' detenido por usuario.", self._sensor_id)
        finally:
            # flush() bloquea hasta que TODOS los mensajes pendientes se envíen
            # Es como cerrar la conexión en RabbitMQ, pero asegurando entrega
            producer.flush(timeout=5)
            logger.info("Productor '%s' finalizado. Buffer vaciado.", self._sensor_id)
