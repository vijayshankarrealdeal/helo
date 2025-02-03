import os
from supabase import create_client, Client

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
def get_client():
    print(url)
    supabase: Client = create_client(url, key)
    return supabase
