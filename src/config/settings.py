"""
Configuración centralizada del sistema IoT con Kafka.

Todas las constantes de configuración se definen aquí para facilitar
cambios entre entornos (desarrollo, producción, etc.) sin modificar
el código de negocio.
"""
import os

# --- Kafka Connection ---
# Bootstrap server: punto de entrada al clúster Kafka.
# En RabbitMQ teníamos host + port + vhost. En Kafka, un solo string
# con formato "host:puerto" es suficiente para descubrir el clúster.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

# --- Topic ---
# En RabbitMQ teníamos un Exchange + múltiples Queues.
# En Kafka, todo va a un solo Topic. Los consumidores se diferencian
# por su Consumer Group, no por tener colas separadas.
TOPIC_NAME = "sensor-data"
NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1  # 1 porque usamos un solo broker

# --- Consumer Groups ---
# Cada grupo mantiene su propio offset (posición de lectura).
# Dashboard y Alertas leen los MISMOS mensajes de forma independiente.
DASHBOARD_GROUP_ID = "dashboard-group"
ALERTS_GROUP_ID = "alerts-group"

# --- Sensor Configuration ---
SENSORS = {
    "temperature": {
        "sensor_id": "temp-sensor-001",
        "location": "Laboratorio A",
        "interval_seconds": 2,
        "min_value": 15.0,
        "max_value": 45.0,
        "unit": "°C",
    },
    "humidity": {
        "sensor_id": "hum-sensor-001",
        "location": "Laboratorio A",
        "interval_seconds": 3,
        "min_value": 20.0,
        "max_value": 95.0,
        "unit": "%",
    },
}

# --- Alert Thresholds ---
ALERT_THRESHOLDS = {
    "temperature": {"max": 35.0, "label": "Temperatura crítica"},
    "humidity": {"max": 80.0, "label": "Humedad crítica"},
}
