import os
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
resp = client.responses.create(model="gpt-4o-mini", input="Hello from POC environment. Give one short sentence.")
print(resp.output[0].content[0].text)
