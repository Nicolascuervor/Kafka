"""
Sensor de humedad concreto.

Implementación idéntica en lógica al de RabbitMQ.
Solo cambia la clase base (BaseProducer de Kafka vs BaseProducer de pika).
"""
import random

from src.models.sensor_reading import SensorReading
from src.producers.base_producer import BaseProducer
from src.config import settings


class HumiditySensor(BaseProducer):
    """Sensor que genera lecturas aleatorias de humedad."""

    def __init__(self):
        super().__init__(sensor_type="humidity")
        config = settings.SENSORS["humidity"]
        self._min_val = config["min_value"]
        self._max_val = config["max_value"]

    def _generate_reading(self) -> SensorReading:
        """Genera una lectura aleatoria de humedad entre 20% y 95%."""
        value = random.uniform(self._min_val, self._max_val)
        return SensorReading.create(
            sensor_id=self._sensor_id,
            sensor_type=self._sensor_type,
            value=value,
            unit=self._unit,
            location=self._location,
        )
