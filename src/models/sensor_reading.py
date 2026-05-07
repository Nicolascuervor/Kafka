"""
Modelo inmutable para lecturas de sensores IoT.

Este modelo es IDÉNTICO al de RabbitMQ. La serialización JSON es
agnóstica al broker: el sensor genera datos, los serializa a JSON,
y el broker los transporta como bytes. No importa si es RabbitMQ o Kafka.

Principios aplicados:
    - SRP: solo modela y serializa datos de lectura.
    - Inmutabilidad (frozen=True): evita mutaciones accidentales en tránsito.
"""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json


@dataclass(frozen=True)
class SensorReading:
    """Representa una lectura individual de un sensor IoT."""

    sensor_id: str
    sensor_type: str
    value: float
    unit: str
    location: str
    timestamp: str

    @staticmethod
    def create(sensor_id: str, sensor_type: str, value: float,
               unit: str, location: str) -> "SensorReading":
        """Factory method que genera el timestamp automáticamente."""
        return SensorReading(
            sensor_id=sensor_id,
            sensor_type=sensor_type,
            value=round(value, 2),
            unit=unit,
            location=location,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    def to_json(self) -> str:
        """Serializa la lectura a JSON string."""
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str: str) -> "SensorReading":
        """Deserializa un JSON string a SensorReading."""
        data = json.loads(json_str)
        return SensorReading(**data)
