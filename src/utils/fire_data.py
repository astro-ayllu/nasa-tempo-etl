import requests
from src.utils import storage


def save_fire_data(url, filename):
    response = requests.get(url)
    response.raise_for_status()
    storage.save_single_file(filename, response.content)
