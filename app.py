
from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({'message': 'API REST de NASA TEMPO ETL funcionando correctamente.'})

@app.route('/status', methods=['GET'])
def status():
    return jsonify({'status': 'ok', 'service': 'nasa-tempo-etl'})

@app.route('/process', methods=['POST'])
def process():
    data = request.get_json()
    return jsonify({'message': 'Procesamiento iniciado', 'input': data}), 202


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
