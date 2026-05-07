"""
Consumidor de Alertas para evaluación de umbrales críticos.

Lógica idéntica al de RabbitMQ: evalúa reglas de negocio por tipo de sensor.
"""
import logging

from src.consumers.base_consumer import BaseConsumer
from src.models.sensor_reading import SensorReading
from src.config import settings

logger = logging.getLogger(__name__)


class AlertConsumer(BaseConsumer):
    """Consumidor que evalúa umbrales y emite alertas."""

    def __init__(self):
        super().__init__(
            group_id="alerts-group",
            consumer_name="Alertas",
        )

    def process_reading(self, reading: SensorReading) -> None:
        """Evalúa la lectura contra los umbrales definidos en settings."""
        threshold = settings.ALERT_THRESHOLDS.get(reading.sensor_type)

        if threshold is None:
            return

        if reading.value > threshold["max"]:
            print(f"""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  ⚠️  ALERTA: {threshold['label']}!
  Sensor:  {reading.sensor_id}
  Valor:   {reading.value} {reading.unit} (umbral: {threshold['max']})
  Lugar:   {reading.location}
  Hora:    {reading.timestamp}
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!""")
