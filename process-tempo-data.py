from dotenv import load_dotenv
import os

load_dotenv()

earthdata_username = os.getenv('EARTDATA_USERNAME')
earthdata_password = os.getenv('EARTDATA_PASSWORD')


