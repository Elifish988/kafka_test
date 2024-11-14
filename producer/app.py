from flask import Flask, request
from producer import get_mail
from querys.bp_querys import bp_query

app = Flask(__name__)
app.register_blueprint(bp_query)

@app.route('/api/email', methods=['POST'])
def mail():
    mail = request.get_json()
    get_mail(mail)
    return 'Email sent successfully', 200

if __name__ == '__main__':
    app.run()
