import os
from groq import Groq

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

try:
    resp = client.chat.completions.create(
        model="llama-3.1-8b-instant",   # or any model from your list
        messages=[
            {"role": "user", "content": "Say a one-line hello and confirm which model you are."}
        ]
    )

    # corrected access
    print("MODEL RESPONSE:\n", resp.choices[0].message.content)

except Exception as e:
    import traceback; traceback.print_exc()
