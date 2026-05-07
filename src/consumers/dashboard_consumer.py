"""
Consumidor de Dashboard para visualización en tiempo real.

Lógica idéntica al de RabbitMQ: agrega lecturas y muestra resumen.
La diferencia está en CÓMO recibe los mensajes (pull vs push),
no en QUÉ hace con ellos.
"""
import logging
from collections import defaultdict

from src.consumers.base_consumer import BaseConsumer
from src.models.sensor_reading import SensorReading

logger = logging.getLogger(__name__)


class DashboardConsumer(BaseConsumer):
    """Consumidor que muestra un resumen en tiempo real de las lecturas."""

    def __init__(self):
        super().__init__(
            group_id="dashboard-group",
            consumer_name="Dashboard",
        )
        # Acumuladores por tipo de sensor
        self._readings: dict[str, list[float]] = defaultdict(list)

    def process_reading(self, reading: SensorReading) -> None:
        """Muestra un resumen formateado de la lectura recibida."""
        self._readings[reading.sensor_type].append(reading.value)
        values = self._readings[reading.sensor_type]
        avg = sum(values) / len(values)

        print(f"""
=======================================================
  DASHBOARD - IoT Sensor Monitor (Kafka)
=======================================================
  Sensor:    {reading.sensor_id}
  Tipo:      {reading.sensor_type}
  Ubicación: {reading.location}
  Timestamp: {reading.timestamp}
-------------------------------------------------------
  Lectura actual:  {reading.value} {reading.unit}
  Promedio:        {avg:.1f} {reading.unit}
  Total lecturas:  {len(values)}
=======================================================""")
