import os
from groq import Groq
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
try:
    models = client.models.list()
    print("Available models:")
    for m in models.data:
        print(" -", m.id)
except Exception as e:
    import traceback; traceback.print_exc()