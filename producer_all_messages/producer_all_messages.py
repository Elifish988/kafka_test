import json
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from bson import ObjectId

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['messages']
collection = db['messages_all']

consumer = KafkaConsumer(
    'all.messages',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='all_messages_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)


# Function to convert ObjectId to string
def convert_object_id(mail):
    if "_id" in mail:
        mail["_id"] = str(mail["_id"])
    return mail


for mail in consumer:
    mail = mail.value
    collection.insert_one(mail)
    print(f"Stored fraudulent transaction: {mail}")

    mail = convert_object_id(mail)

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
