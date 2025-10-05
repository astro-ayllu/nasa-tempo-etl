import os
from supabase import create_client, Client
import src.config as config

url: str = config.SUPABASE_URL
key: str = config.SUPABASE_KEY
supabase: Client = create_client(url, key)

def save_processing(date: str, process_key: str = "default"):
    data = {
        "date": date,
        "process_key": process_key
    }
    supabase.table("processing").insert(data).execute()