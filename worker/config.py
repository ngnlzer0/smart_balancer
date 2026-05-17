import os
import logging

# Налаштування логера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ControlPlane")


class Config:
    """
    Клас конфігурації.
    Всі параметри підтягуються зі змінних середовища (які інжектить Docker з .env файлу).
    Якщо змінної немає, використовується безпечне значення за замовчуванням (fallback).
    """

    # --- Infrastructure ---
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    NETWORK_NAME: str = os.getenv("NETWORK_NAME", "course_work_network")
    BACKEND_PORT: int = int(os.getenv("BACKEND_PORT", 8000))
    POLL_INTERVAL: float = float(os.getenv("POLL_INTERVAL", 1.0))

    # --- Math Model (Початкові конфіги) ---
    DEFAULT_MATH_CONFIG = {
        'ALPHA': float(os.getenv("ALPHA", 0.35)),
        'BETA': float(os.getenv("BETA", 0.05)),
        'GAMMA': float(os.getenv("GAMMA", 0.40)),
        'DELTA': float(os.getenv("DELTA", 0.20)),

        'T_MAX': float(os.getenv("T_MAX", 1000.0)),
        'A_MAX': float(os.getenv("A_MAX", 100.0)),
        'PENALTY': float(os.getenv("PENALTY", 10.0)),
        'RHO': float(os.getenv("RHO", 0.3)),

        'CB_THRESHOLD': int(os.getenv("CB_THRESHOLD", 3))
    }


config = Config()