"""
DataTap — Entry Point
Run with: python main.py
Or:        uvicorn web.app:app --reload
"""

import sys
import os

# Ensure project root is on the path so all imports work
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv

load_dotenv()

import uvicorn

if __name__ == "__main__":
    print("""
  Generic API → Database pipeline
  ─────────────────────────────────
  Dashboard  →  http://localhost:8000
    """)
    uvicorn.run(
        "web.app:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", 8000)),
        reload=os.getenv("DEV", "true").lower() == "true",
        log_level="warning",  # suppress uvicorn noise; DataTap prints its own logs
    )
