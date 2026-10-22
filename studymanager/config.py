import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Pasta do app: onde ficam .env e docker-compose.yml (ao lado do .exe quando compilado).
APP_DIR = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent.parent
load_dotenv(APP_DIR / ".env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("STUDY_TOPIC", "study-events")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://study:study@localhost:5433/studymanager")
TIMEZONE = ZoneInfo(os.getenv("STUDY_TZ", "America/Sao_Paulo"))

# Quantos itens podem estar em "Doing" ao mesmo tempo.
WIP_LIMIT = int(os.getenv("WIP_LIMIT", "2"))

# Intervalos da revisão espaçada. REVIEW_UNIT=minutes ajuda a testar sem esperar dias.
REVIEW_INTERVALS = [1, 3, 7, 15, 30]
REVIEW_UNIT = os.getenv("REVIEW_UNIT", "days")

# Quadro web local.
WEB_PORT = int(os.getenv("WEB_PORT", "8092"))

# Integração com o site (vazio = desligada).
SITE_URL = os.getenv("SITE_URL") or None
SITE_API_KEY = os.getenv("SITE_API_KEY", "")
SITE_POLL_SECONDS = int(os.getenv("SITE_POLL_SECONDS", "60"))
