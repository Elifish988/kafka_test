import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'messages.explosive',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='messages_explosive_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)



for mail in consumer:
    mail = mail.value
    print(f"Received explosive transaction: {mail}")