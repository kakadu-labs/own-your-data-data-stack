import csv
import logging
import time
from dataclasses import astuple, dataclass
from datetime import datetime
from uuid import uuid4

from faker import Faker

import config

logger = logging.getLogger(__name__)


@dataclass
class Order:
    ts: datetime
    uuid: uuid4
    customer_name: str

    @classmethod
    def new(cls, customer_name: str):
        return Order(ts=datetime.now(), uuid=uuid4(), customer_name=customer_name)


def append_order_to_csv(order, csv_path):
    with open(csv_path, "a", newline="") as csvfile:
        order_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        order_writer.writerow(astuple(order))


def run():
    faker = Faker()
    while True:
        customer_name = f"{faker.first_name()} {faker.last_name()}"
        order_to_insert = Order.new(customer_name)
        logger.info(f"Appending new Order to csv!: {order_to_insert}")

        append_order_to_csv(order_to_insert, config.CSV_STREAM_PATH)
        time.sleep(10)  # sleep 10 seconds


if __name__ == "__main__":
    run()
