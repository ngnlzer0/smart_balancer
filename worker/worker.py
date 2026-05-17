"""
Модуль Control Plane для Адаптивного Балансувальника Навантаження.
====================================================================

Цей скрипт виконує роль "мозку" нашої інфраструктури. Він працює як фоновий
демон (daemon), який безперервно аналізує стан бекенд-серверів та оновлює
правила маршрутизації для Nginx через спільну базу даних Redis.

Ключові реалізовані архітектурні патерни:
1. Service Discovery (Динамічне виявлення сервісів через Docker API).
2. Circuit Breaker (Запобіжник для відключення "мертвих" вузлів).
3. Hot Reload Configuration (Оновлення конфігів без перезавантаження).
4. Asynchronous I/O (Асинхронний збір метрик для максимальної швидкодії).
5. EWMA (Експоненційне згладжування для запобігання осциляцій трафіку).
"""

import os
import time
import asyncio
# Імпортуємо асинхронні бібліотеки для мережевих запитів та роботи з БД.
# Використання aiohttp замість requests дозволяє нам не блокувати головний потік (Event Loop)
# під час очікування відповіді від повільного бекенда.
import aiohttp
# Офіційний SDK для взаємодії з Docker Daemon.
# Він працює синхронно, тому ми будемо викликати його в окремому потоці (Thread).
import docker
import logging
import signal
import redis.asyncio as aioredis
from typing import Dict, List, Tuple

# ============================================================================
# Налаштування системи логування (Logging Configuration)
# ============================================================================
# У production-системах логи ніколи не пишуть через print().
# Ми налаштовуємо стандартний логер Python у форматі, який легко парсити
# системами збору логів (наприклад, ELK Stack - Elasticsearch, Logstash, Kibana
# або Grafana Loki).
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ControlPlane")


class AdaptiveLoadBalancer:
    """
        Головний клас, що інкапсулює логіку керування балансуванням.
        Використання ООП підходу дозволяє зручно зберігати стан системи (State)
        між ітераціями циклу, уникаючи глобальних змінних.
    """

    def __init__(self):
        """
                Ініціалізація балансувальника.
                Тут ми дотримуємося методології "12-Factor App", за якою всі
                інфраструктурні налаштування мають передаватися через змінні середовища (Environment Variables).
        """

        # 1. ІНФРАСТРУКТУРНІ НАЛАШТУВАННЯ
        # os.getenv дозволяє задати значення за замовчуванням ("redis"), якщо змінна не передана.
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.network_name = os.getenv("NETWORK_NAME", "course_work_network")
        self.backend_port = 8000
        self.poll_interval = 1.0

        # Клієнти
        self.docker_client = docker.from_env()
        self.redis = aioredis.Redis(host=self.redis_host, port=6379, decode_responses=True)

        # Стан (State)
        self.history_l_smooth: Dict[str, float] = {}
        self.failure_counters: Dict[str, int] = {}  # Для Circuit Breaker
        self.running = True

        # Дефолтні конфіги (будуть оновлюватись з Redis)
        self.config = {
            'ALPHA': 0.35, 'BETA': 0.05, 'GAMMA': 0.40, 'DELTA': 0.20,
            'T_MAX': 1000.0, 'A_MAX': 100.0, 'PENALTY': 10.0, 'RHO': 0.3,
            'CB_THRESHOLD': 3  # Скільки разів сервер має впасти для Circuit Breaker
        }

    async def update_config_from_redis(self):
        """Динамічно підтягує математичні коефіцієнти з Redis, якщо вони там є."""
        try:
            for key in self.config.keys():
                val = await self.redis.get(f"config_{key}")
                if val is not None:
                    self.config[key] = float(val)
        except Exception as e:
            logger.warning(f"Не вдалося оновити конфіги з Redis: {e}")

    def get_active_backends(self) -> List[str]:
        """Синхронний виклик Docker API для пошуку контейнерів."""
        active_ips = []
        try:
            for container in self.docker_client.containers.list():
                if container.labels.get('com.docker.compose.service') == 'backend':
                    networks = container.attrs['NetworkSettings']['Networks']
                    if self.network_name in networks:
                        active_ips.append(networks[self.network_name]['IPAddress'])
        except Exception as e:
            logger.error(f"Помилка Service Discovery: {e}")
        return active_ips

    async def fetch_metrics(self, session: aiohttp.ClientSession, ip: str) -> dict:
        """Асинхронний збір метрик (НЕ блокує інші сервери)."""
        # Circuit Breaker Logic
        if self.failure_counters.get(ip, 0) >= self.config['CB_THRESHOLD']:
            # Якщо сервер "впав" багато разів, даємо йому пінги рідше, щоб не спамити мережу
            logger.warning(f"Circuit Breaker: Сервер {ip} ігнорується (перевищено ліміт помилок).")
            return self.get_dead_metrics()

        url = f"http://{ip}:{self.backend_port}/health"
        start_time = time.time()

        try:
            async with session.get(url, timeout=0.8) as response:
                response.raise_for_status()
                data = await response.json()
                data['latency_ms'] = (time.time() - start_time) * 1000

                # Успішний запит - скидаємо лічильник помилок
                self.failure_counters[ip] = 0
                return data

        except asyncio.TimeoutError:
            logger.error(f"Таймаут підключення до {ip}")
            self.failure_counters[ip] = self.failure_counters.get(ip, 0) + 1
            return self.get_dead_metrics()

        except Exception as e:
            logger.error(f"Помилка доступу до {ip}: {str(e)}")
            self.failure_counters[ip] = self.failure_counters.get(ip, 0) + 1
            return self.get_dead_metrics()

    def get_dead_metrics(self) -> dict:
        """Повертає найгірші метрики для неробочого сервера."""
        return {
            'cpu_percent': 100.0,
            'ram_percent': 100.0,
            'active_connections': self.config['A_MAX'],
            'error_rate': 1.0,
            'latency_ms': self.config['T_MAX']
        }

    def calculate_weight(self, ip: str, metrics: dict) -> Tuple[int, float]:
        """Чиста математична функція (EWMA + багатокритеріальна оцінка)."""
        cfg = self.config

        # 1. Нормалізація
        c = min(metrics['cpu_percent'] / 100.0, 1.0)
        m = min(metrics['ram_percent'] / 100.0, 1.0)
        r = min(metrics['latency_ms'] / cfg['T_MAX'], 1.0)
        q = min(metrics['active_connections'] / cfg['A_MAX'], 1.0)
        e = metrics['error_rate']

        # 2. Базове навантаження
        l_base = (cfg['ALPHA'] * c) + (cfg['BETA'] * m) + (cfg['GAMMA'] * r) + (cfg['DELTA'] * q)

        # 3. Штраф за помилки
        l_curr = l_base * (1.0 + cfg['PENALTY'] * e)

        # 4. EWMA
        prev_l_smooth = self.history_l_smooth.get(ip, 0.0)
        l_smooth = (cfg['RHO'] * l_curr) + ((1.0 - cfg['RHO']) * prev_l_smooth)
        self.history_l_smooth[ip] = l_smooth

        # 5. Розрахунок кінцевої ваги
        c_avail = max(0.0, 1.0 - l_smooth)
        weight = max(1, int(c_avail * 100))

        return weight, l_smooth

    async def main_cycle(self):
        """Головний асинхронний цикл обробки."""
        async with aiohttp.ClientSession() as session:
            while self.running:
                cycle_start = time.time()

                # 1. Оновлення конфігів "на льоту" та Service Discovery
                await self.update_config_from_redis()
                # Робимо синхронний виклик Docker API в окремому потоці, щоб не блокувати Async Event Loop
                active_ips = await asyncio.to_thread(self.get_active_backends)

                if not active_ips:
                    logger.warning("Бекенди не знайдені. Очікування...")
                    await asyncio.sleep(self.poll_interval)
                    continue

                # 2. Асинхронний паралельний збір метрик
                # Ми відправляємо запити до всіх серверів ОДНОЧАСНО!
                tasks = [self.fetch_metrics(session, ip) for ip in active_ips]
                metrics_results = await asyncio.gather(*tasks)

                # 3. Підготовка транзакції до Redis
                pipeline = self.redis.pipeline()
                pipeline.delete("active_backends")
                pipeline.sadd("active_backends", *active_ips)

                # 4. Розрахунок і запис
                for ip, metrics in zip(active_ips, metrics_results):
                    weight, l_smooth = self.calculate_weight(ip, metrics)
                    pipeline.set(f"weight_{ip}", weight)
                    logger.info(
                        f"[{ip}] Load: {l_smooth:.2f} | Weight: {weight:3d} | Err: {metrics['error_rate']:.1f} | CPU: {metrics['cpu_percent']}%")

                # 5. Виконання транзакції та очищення сміття
                await pipeline.execute()
                self._garbage_collect(active_ips)

                # 6. Динамічний сон (щоб тримати ідеальний такт)
                elapsed = time.time() - cycle_start
                sleep_time = max(0, self.poll_interval - elapsed)
                await asyncio.sleep(sleep_time)

    def _garbage_collect(self, active_ips: List[str]):
        """Видаляє історію мертвих контейнерів (захист від витоку пам'яті)."""
        dead_ips = set(self.history_l_smooth.keys()) - set(active_ips)
        for ip in dead_ips:
            del self.history_l_smooth[ip]
            if ip in self.failure_counters:
                del self.failure_counters[ip]

    def shutdown(self, signum, frame):
        """Graceful Shutdown: ловить сигнали зупинки від Docker."""
        logger.info("Отримано сигнал зупинки. Завершуємо роботу...")
        self.running = False


async def main():
    balancer = AdaptiveLoadBalancer()

    # Реєстрація сигналів для коректного завершення (SIGTERM відкидає Docker при зупинці)
    signal.signal(signal.SIGINT, balancer.shutdown)
    signal.signal(signal.SIGTERM, balancer.shutdown)

    logger.info("=== Adaptive Load Balancer Control Plane Started ===")

    try:
        await balancer.main_cycle()
    finally:
        # Закриваємо з'єднання з Redis
        await balancer.redis.close()
        logger.info("Усі з'єднання закрито. Вихід.")


if __name__ == "__main__":
    # Пауза на старті, щоб Redis та Бекенди встигли піднятися
    time.sleep(3)
    asyncio.run(main())