
from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os
from src.controllers.data_procesor_controller import DataProcessorController

load_dotenv()

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({'message': 'API REST de NASA TEMPO ETL funcionando correctamente.'})

@app.route('/status', methods=['GET'])
def status():
    return jsonify({'status': 'ok', 'service': 'nasa-tempo-etl'})

@app.route('/process', methods=['GET'])
def process():
    return DataProcessorController.process()

@app.route('/historical/<string:date>', methods=['GET'])
def historical(date):
    return DataProcessorController.historical(date)

    
import os

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
