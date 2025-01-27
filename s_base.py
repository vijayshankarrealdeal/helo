import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from a .env file
load_dotenv()

# Retrieve the Supabase URL and Key from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Validate that both SUPABASE_URL and SUPABASE_KEY are provided
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in environment variables.")


def get_client() -> Client:
    """
    Creates and returns a Supabase client.

    Returns:
        Client: An instance of the Supabase client.
    """
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase


# Example usage
if __name__ == "__main__":
    # Initialize the Supabase client
    supabase_client = get_client()

    # Example Query: Fetch all rows from a table named 'users'
    try:
        response = supabase_client.table("meme").select("*").execute()
        if response.data:
            users = response.data
            print("Users:", users)
        else:
            print(f"Error fetching data: {response}")
    except Exception as e:
        print("An error occurred while querying the database:", e)
