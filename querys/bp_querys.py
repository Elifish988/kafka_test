from flask import Blueprint, jsonify
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, relationship



#חיבור למונגו
client = MongoClient('mongodb://localhost:27017/')
db = client['messages']
collection = db['messages_all']

# חיבור לSQL
engine = create_engine("postgresql://postgres:1234@localhost:5432/suspicious_sentences")
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

bp_query = Blueprint('bp_query', __name__)

@bp_query.route('/api/get_email_by_email/<email>', methods=['GET'])
def get_email_by_email(email):
    query = {"email": email}
    email = collection.find_one(query)
    if email:
        email["_id"] = str(email["_id"])
        return jsonify(email)
    else:
        return jsonify({"error": f"No messages found for email: {query['email']}"}), 404




@bp_query.route('/api/most_common_word<email>', methods=['GET'])
def most_common_word(email):
    query = {"email": email}
    email = collection.find_one(query)

    with Session(engine) as session:
        user = session.query(User).filter_by(email=email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        # קבלת כל ההודעות החשודות של המשתמש
        sentences = []
        if user.content_explosive_suspicious:
            sentences.extend(user.content_explosive_suspicious.sentences)
        if user.content_hostage_suspicious:
            sentences.extend(user.content_hostage_suspicious.sentences)

        # איחוד כל ההודעות כטקסט בודד
        all_text = ' '.join(sentences).lower()
        words = re.findall(r'\b\w+\b', all_text)  # חלוקה למילים

        # חישוב המילה הנפוצה ביותר
        most_common = Counter(words).most_common(1)
        if most_common:
            word, count = most_common[0]
            return jsonify({"most_common_word": word, "count": count})
        else:
            return jsonify({"error": "No words found in messages"}), 404

if __name__ == '__main__':
    app.run()
