import time
import json
from google import genai
from google.genai import types
from pydantic import BaseModel
from typing import List, Optional

class PingOut(BaseModel):
    label: str
    sous_label: str
    lieu: Optional[str] = None
    key_word: List[str] = []

def run_audit(api_key: str, n: int = 5):
    client = genai.Client(api_key=api_key)

    prompt = """
Retourne UNIQUEMENT un JSON valide correspondant au schéma:
{ "label": "autre", "sous_label": "autre", "lieu": null, "key_word": ["test"] }
    """.strip()

    for i in range(n):
        t0 = time.time()
        try:
            resp = client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=PingOut,
                ),
            )

            dt = time.time() - t0
            ok = resp.parsed is not None
            print(f"[{i+1}/{n}] latency={dt:.2f}s parsed_ok={ok}")

            if not ok:
                print("  raw preview:", getattr(resp, "text", "")[:300])

        except Exception as e:
            dt = time.time() - t0
            print(f"[{i+1}/{n}] latency={dt:.2f}s ERROR={e}")

if __name__ == "__main__":
    api_key = open("api_key.txt", "r", encoding="utf-8").read().strip()
    run_audit(api_key, n=10)
