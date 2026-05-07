"""Punto de entrada para el consumidor de Alertas."""
import logging

from src.consumers.alert_consumer import AlertConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    consumer = AlertConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
