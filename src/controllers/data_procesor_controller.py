from flask.views import MethodView
import json
from flask import make_response
from src.services import data_processor_service


def build_response(data):
    response = json.dumps(data, default=str)
    return make_response(response)


class DataProcessorController(MethodView):

    @staticmethod
    def process():
        data = data_processor_service.process_data()
        return build_response(data)
    
    @staticmethod
    def historical(date):
        data = data_processor_service.historical_data(date)
        return build_response(data)
