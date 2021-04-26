from faker import Faker # Mit faker generieren wird die Daten
from faker.providers import internet
from faker.providers import credit_card
from faker.providers import phone_number
from faker.providers import address

from kafka import KafkaProducer # Um Daten in Kafka zu bekommen nehmen wir den Producer aus dem modul kafka
import time
import datetime
import json
import atexit # Um 'exit' abzufangen
import random

global config
config = {
    'broker': 'kafka',
    'topic': 'customer-info'
}

global producer
producer=None

def exit_handler():
    global producer
    print ('My application is ending!')
    producer.close()
    exit(0)

atexit.register(exit_handler)

fake = Faker('de_DE')
fake.add_provider(internet)
fake.add_provider(credit_card)
fake.add_provider(phone_number)
fake.add_provider(address)

def main():

    global config
    print(config)
    try:
        admin = KafkaAdminClient(bootstrap_servers=f"{ config['broker'] }:9092")

        topic = NewTopic(name=config['topic'],
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception:
        pass

    global producer
    producer = KafkaProducer(bootstrap_servers=config['broker'], client_id='dwh-customer-data', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    print(producer.bootstrap_connected())


    while True:
        print(f"Still up! {datetime.datetime.now().isoformat()}")
        for _ in range(random.randint(0, 10)): # Simulieren wir fluktuationen in der Anzahl an Einträgen
            info = {
                'name': fake.name(),
                'birthdate': fake.date_of_birth().isoformat(), #strftime('%m/%d/%Y %H:%M:%S %Z'),
                'phone_number': fake.phone_number(),
                'ip': fake.ipv4_public(),
                'address': {
                    'street_name': fake.street_name(),
                    'building_number': fake.building_number(),
                    'city': fake.city(),
                    'plz': fake.postcode(),
                    'country': fake.country(),
                },
                'payment_info': [
                    {
                        'type': 'Credit Card',
                        'exp_date': fake.credit_card_expire(),
                        'card_number': fake.credit_card_number(),
                        'provider': fake.credit_card_provider(),
                        'code': fake.credit_card_security_code(),
                    }
                ],
            }
            # print(info)
            producer.send(config['topic'], info)
        time.sleep(1)
        producer.flush() # Falls der delay nicht reicht warten wir hier darauf, dass alles geschrieben wurde um das System nicht zu überlasten


if __name__ == "__main__":
    main()
