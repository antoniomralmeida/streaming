import json
import random
import threading
import time

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            data = {}
            id_ = random.randint(0, 1000)

            if data.__contains__(id(id_)):
                message = data.get(id_)
            else:
                streaming = {'idade': random.randint(10, 50), 'altura': random.randint(100, 200),
                            'peso': random.randint(30, 100)}
                message = [id_, streaming]
                data[id_] = message

            producer.send('topic', message)
            time.sleep(random.randint(0, 5))


class Consumer(threading.Thread):

    def run(self):
        stream = KafkaConsumer(bootstrap_servers='localhost:9092',
                               auto_offset_reset='latest')
        stream.subscribe(['topic'])
	for tuple in stream:
		print(tuple)


if __name__ == '__main__':
    threads = [
        Producer(),
        # Producer(),
        # Producer(),
        # Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()
