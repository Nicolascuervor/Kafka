"""
Orquestador de sensores productores.

Estructura idéntica al de RabbitMQ: dos hilos daemon,
uno por sensor, ejecutándose en paralelo.
"""
import logging
import threading

from src.producers.temperature_sensor import TemperatureSensor
from src.producers.humidity_sensor import HumiditySensor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    temp_sensor = TemperatureSensor()
    hum_sensor = HumiditySensor()

    temp_thread = threading.Thread(target=temp_sensor.run, daemon=True)
    hum_thread = threading.Thread(target=hum_sensor.run, daemon=True)

    temp_thread.start()
    hum_thread.start()

    print("\nBoth sensors are running. Press Ctrl+C to stop.\n")

    try:
        temp_thread.join()
        hum_thread.join()
    except KeyboardInterrupt:
        print("\nSensors stopped.")


if __name__ == "__main__":
    main()
