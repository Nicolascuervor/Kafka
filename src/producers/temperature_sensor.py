"""
Sensor de temperatura concreto.

Implementación idéntica en lógica al de RabbitMQ.
Solo cambia la clase base (BaseProducer de Kafka vs BaseProducer de pika).
"""
import random

from src.models.sensor_reading import SensorReading
from src.producers.base_producer import BaseProducer
from src.config import settings


class TemperatureSensor(BaseProducer):
    """Sensor que genera lecturas aleatorias de temperatura."""

    def __init__(self):
        super().__init__(sensor_type="temperature")
        config = settings.SENSORS["temperature"]
        self._min_val = config["min_value"]
        self._max_val = config["max_value"]

    def _generate_reading(self) -> SensorReading:
        """Genera una lectura aleatoria de temperatura entre 15°C y 45°C."""
        value = random.uniform(self._min_val, self._max_val)
        return SensorReading.create(
            sensor_id=self._sensor_id,
            sensor_type=self._sensor_type,
            value=value,
            unit=self._unit,
            location=self._location,
        )
