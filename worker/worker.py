import os
import time
import docker
import redis
import requests
import logging

# ==========================================
# Налаштування та Константи
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
NETWORK_NAME = os.getenv("NETWORK_NAME", "course_work_network")
BACKEND_PORT = 8000
POLL_INTERVAL = 1.0  # Опитуємо кожну 1 секунду

# Математичні коефіцієнти алгоритму
ALPHA = 0.35  # Вага CPU
BETA = 0.05  # Вага RAM
GAMMA = 0.40  # Вага Latency
DELTA = 0.20  # Вага Connections

T_MAX = 1000.0  # Максимально допустимий час відгуку (мс)
A_MAX = 100.0  # Максимальна кількість з'єднань на сервер
PENALTY = 10.0  # Штраф за помилки
RHO = 0.3  # Коефіцієнт згладжування EWMA

# ==========================================
# Ініціалізація клієнтів
# ==========================================
docker_client = docker.from_env()
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# Словник для збереження історії згладженого навантаження (L_smooth)
history_l_smooth = {}

def get_active_backends():
    """Шукає IP-адреси всіх живих контейнерів сервісу 'backend' у Docker."""
    active_ips = []
    try:
        for container in docker_client.containers.list():
            if container.labels.get('com.docker.compose.service') == 'backend':
                networks = container.attrs['NetworkSettings']['Networks']
                if NETWORK_NAME in networks:
                    active_ips.append(networks[NETWORK_NAME]['IPAddress'])
    except Exception as e:
        logging.error(f"Помилка Docker API: {e}")
    return active_ips


def collect_metrics(ip):
    """Робить HTTP-запит до бекенда для отримання метрик."""
    url = f"http://{ip}:{BACKEND_PORT}/health"
    start_time = time.time()
    try:
        response = requests.get(url, timeout=0.5)
        response.raise_for_status()
        latency_ms = (time.time() - start_time) * 1000
        data = response.json()
        data['latency_ms'] = latency_ms
        return data
    except Exception:
        # Якщо сервер лежить або таймаут, повертаємо критичні метрики
        return {
            'cpu_percent': 100.0,
            'ram_percent': 100.0,
            'active_connections': A_MAX,
            'error_rate': 1.0,
            'latency_ms': T_MAX
        }


def calculate_weight(ip, metrics):
    """Застосовує математичну модель (EWMA + Penalty) для обчислення ваги."""
    # 1. Нормалізація (0.0 - 1.0)
    c = min(metrics['cpu_percent'] / 100.0, 1.0)
    m = min(metrics['ram_percent'] / 100.0, 1.0)
    r = min(metrics['latency_ms'] / T_MAX, 1.0)
    q = min(metrics['active_connections'] / A_MAX, 1.0)
    e = metrics['error_rate']

    # 2. Базове навантаження
    l_base = (ALPHA * c) + (BETA * m) + (GAMMA * r) + (DELTA * q)

    # 3. Штраф за помилки
    l_curr = l_base * (1.0 + PENALTY * e)

    # 4. Експоненційне згладжування (EWMA)
    prev_l_smooth = history_l_smooth.get(ip, 0.0)  # Якщо сервер новий, історія = 0
    l_smooth = (RHO * l_curr) + ((1.0 - RHO) * prev_l_smooth)
    history_l_smooth[ip] = l_smooth

    # 5. Розрахунок кінцевої ваги (від 1 до 100)
    c_avail = max(0.0, 1.0 - l_smooth)
    weight = max(1, int(c_avail * 100))

    return weight, l_smooth


def main_loop():
    logging.info("Control Plane запущено. Очікування контейнерів...")
    while True:
        try:
            # 1. Service Discovery
            active_ips = get_active_backends()

            if not active_ips:
                logging.warning("Не знайдено жодного живого бекенда!")
                time.sleep(POLL_INTERVAL)
                continue

            # Транзакція Redis для атомарного оновлення
            pipeline = redis_client.pipeline()

            # Оновлюємо список живих серверів (видаляємо старий, записуємо новий)
            pipeline.delete("active_backends")
            pipeline.sadd("active_backends", *active_ips)

            # 2. Збір метрик та розрахунок для кожного IP
            for ip in active_ips:
                metrics = collect_metrics(ip)
                weight, l_smooth = calculate_weight(ip, metrics)

                # Записуємо вагу в Redis
                pipeline.set(f"weight_{ip}", weight)

                logging.info(
                    f"Бекенд {ip} | Load: {l_smooth:.2f} | Weight: {weight} | Errors: {metrics['error_rate']:.1f}")

            # 3. Фіксація змін у Redis
            pipeline.execute()

            # Очищення історії для "мертвих" серверів, щоб не було витоку пам'яті
            for ip in list(history_l_smooth.keys()):
                if ip not in active_ips:
                    del history_l_smooth[ip]

        except Exception as e:
            logging.error(f"Неочікувана помилка в головному циклі: {e}")

        # Чекаємо до наступної ітерації
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    # Даємо іншим контейнерам час на старт
    time.sleep(5)
    main_loop()