"""Punto de entrada para el consumidor Dashboard."""
import logging

from src.consumers.dashboard_consumer import DashboardConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    consumer = DashboardConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
