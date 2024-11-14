import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import datetime
import psycopg2

#יצירת דאטא ביייס
engine = create_engine("postgresql://postgres:1234@localhost:5432/suspicious_sentences")
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    email = Column(String, unique=True)
    ip_address = Column(String)


    content_explosive_suspicious = relationship("ContentExplosiveSuspicious", back_populates="user", uselist=False)


class ContentExplosiveSuspicious(Base):
    __tablename__ = ' content_explosive_suspicious'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    user_id = Column(Integer, ForeignKey('users.id'))
    location = Column(JSON)
    device_info = Column(JSON)
    sentences = Column(JSON)

    user = relationship("User", back_populates="content_explosive_suspicious")


Base.metadata.create_all(engine)

# קבלת המיילים
consumer = KafkaConsumer(
    'messages.explosive',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='messages_explosive_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

#הכנסת המייל לדאטא בייס
from sqlalchemy.exc import SQLAlchemyError

# בתוך הלולאה שקוראת את המיילים
for mail in consumer:
    mail = mail.value
    print(f"Received mail: {mail}")

    try:
        # בדיקה אם המשתמש כבר קיים
        existing_user = session.query(User).filter_by(email=mail["email"]).first()

        if existing_user:
            user = existing_user
        else:
            user = User(
                username=mail["username"],
                email=mail["email"],
                ip_address=mail["ip_address"],
            )
            session.add(user)
            session.commit()  # שמירה ב-database

        user_details = ContentExplosiveSuspicious(
            user_id=user.id,
            created_at=datetime.datetime.fromisoformat(mail["created_at"]),
            location=mail["location"],
            device_info=mail["device_info"],
            sentences=mail["sentences"]
        )

        session.add(user_details)
        session.commit()

    except SQLAlchemyError as e:
        print(f"Error occurred while processing {mail['email']}: {str(e)}")
    finally:
        session.close()


