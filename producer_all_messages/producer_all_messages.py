import json
from kafka import KafkaConsumer, KafkaProducer
from producer_all_messages.db import collection

consumer = KafkaConsumer(
    'all.messages',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='all_messages_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for mail in consumer:
    mail = mail.value
    collection.insert_one(mail)
    print(f"Stored fraudulent transaction: {mail}")
    replacement_sentence = mail["sentences"][0]
    for i, sentence in enumerate(mail["sentences"]):
        if "hostage" in sentence:
            mail["sentences"][0] = mail["sentences"][i]
            mail["sentences"][i] = replacement_sentence
            producer.send('messages.hostage', value=mail)
        if "explos" in sentence:
            mail["sentences"][0] = mail["sentences"][i]
            mail["sentences"][i] = replacement_sentence
            producer.send('messages.explosive', value=mail)

    print(mail["sentences"])

