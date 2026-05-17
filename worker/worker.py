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
import logging
import signal
from typing import Dict, List, Tuple

# Імпортуємо асинхронні бібліотеки для мережевих запитів та роботи з БД.
import aiohttp
import redis.asyncio as aioredis

# Офіційний SDK для взаємодії з Docker Daemon.
# Він працює синхронно, тому ми будемо викликати його в окремому потоці (Thread).
import docker

# ============================================================================
# Налаштування системи логування (Logging Configuration)
# ============================================================================

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

        # poll_interval визначає "такт" нашої системи (Heartbeat).
        # 1.0 секунда — це оптимальний баланс між швидкістю реакції на зміну навантаження
        # та економією мережевих ресурсів кластера.
        self.poll_interval = 1.0

        # 2. ІНІЦІАЛІЗАЦІЯ КЛІЄНТІВ
        # Підключаємося до сокета Docker. Контейнер повинен мати volume: /var/run/docker.sock
        self.docker_client = docker.from_env()
        # Створюємо пул з'єднань (Connection Pool) до Redis. decode_responses=True
        # автоматично конвертує байти з Redis у зручні Python-рядки (str).
        self.redis = aioredis.Redis(host=self.redis_host, port=6379, decode_responses=True)

        # 3. СТАН СИСТЕМИ (STATE MANAGEMENT)
        # Оскільки наша математична модель базується на EWMA (Moving Average),
        # нам потрібно пам'ятати, яким було навантаження на сервер у попередню секунду.
        # Формат словника: {"172.18.0.4": 0.45, "172.18.0.5": 0.80}
        self.history_l_smooth: Dict[str, float] = {}

        # Словник для патерну Circuit Breaker (Запобіжник).
        # Він рахує, скільки разів поспіль конкретний IP не відповів на запит.
        # Це захищає мережу від "шторму запитів" до мертвого вузла.
        self.failure_counters: Dict[str, int] = {}

        # Прапорець для безпечного (Graceful) завершення роботи скрипта.
        self.running = True

        # 4. ДИНАМІЧНА КОНФІГУРАЦІЯ (MATHEMATICAL MODEL CONFIG)
        # Ці параметри є ядром нашого алгоритму багатокритеріальної оцінки.
        # Ми зберігаємо їх у словнику, щоб пізніше мати змогу оновлювати їх "на льоту" з Redis,
        # без необхідності перезапускати Docker-контейнер.
        self.config = {
            # Вагові коефіцієнти для різних метрик (сума має дорівнювати 1.0)
            'ALPHA': 0.35,  # Пріоритет завантаження процесора (CPU)
            'BETA': 0.05,   # Пріоритет оперативної пам'яті (RAM) - зазвичай найменш важливий для веб
            'GAMMA': 0.40,  # Пріоритет часу відгуку (Latency) - найважливіший для клієнтського досвіду
            'DELTA': 0.20,  # Пріоритет черги активних з'єднань

            # Порогові значення для нормалізації (переведення в діапазон 0.0 - 1.0)
            'T_MAX': 1000.0, # SLA: якщо сервер відповідає довше 1000 мс, він вважається 100% завантаженим
            'A_MAX': 100.0,  # Максимальна кількість потоків/з'єднань, які сервер може обробити одночасно

            # Коефіцієнти впливу алгоритму
            'PENALTY': 10.0, # Штрафний мультиплікатор за 5xx помилки. 10.0 означає миттєве відключення трафіку
            'RHO': 0.3,      # Чутливість EWMA. 0.3 означає, що ми довіряємо поточним даним на 30%, а історії на 70%

            # Налаштування Circuit Breaker
            'CB_THRESHOLD': 3 # Кількість невдалих спроб, після яких сервер ігнорується до ручного втручання або таймауту
        }

    async def update_config_from_redis(self):
        """
        Метод реалізує патерн "Hot Reload Configuration" (Гаряче перезавантаження конфігів).

        Чому це важливо:
        В реальних production-системах зупинка балансувальника для зміни ваги CPU з 0.35 на 0.40
        є неприпустимою. Цей метод дозволяє адміністратору виконати команду в Redis:
        `SET config_ALPHA 0.40`
        І наш Python-скрипт підхопить цю зміну в наступну секунду, застосувавши нову логіку.
        """
        try:
            # Проходимося по всіх відомих ключах конфігурації
            for key in self.config.keys():
                # Шукаємо значення в Redis за префіксом 'config_'
                val = await self.redis.get(f"config_{key}")

                if val is not None:
                    # Redis зберігає все як рядки (string), тому обов'язково конвертуємо у float
                    self.config[key] = float(val)

        except Exception as e:
            # У разі проблеми з мережею ми не "падаємо", а просто залишаємо старі робочі конфіги
            logger.warning(f"Не вдалося оновити конфіги з Redis (використовуються локальні): {e}")

    def get_active_backends(self) -> List[str]:
        """
        Метод реалізує патерн "Service Discovery" (Виявлення сервісів).

        На відміну від статичних конфігів, де IP-адреси захардкоджені, цей метод динамічно
        опитує Docker Engine (через /var/run/docker.sock) і знаходить всі живі контейнери,
        які належать до сервісу 'backend'. Це дозволяє масштабувати бекенди "на льоту"
        (наприклад, `docker-compose up --scale backend=5`), і система сама знайде нові IP.

        Увага: Офіційна бібліотека `docker` працює синхронно. Щоб не заблокувати наш
        асинхронний цикл (asyncio event loop), цей метод буде викликатися через `asyncio.to_thread`.
        """
        active_ips = []
        try:
            # Отримуємо список усіх контейнерів на хост-машині
            for container in self.docker_client.containers.list():

                # Фільтруємо лише ті контейнери, які були запущені Docker Compose
                # з ім'ям сервісу 'backend'.
                if container.labels.get('com.docker.compose.service') == 'backend':

                    # Контейнер може бути підключений до декількох мереж.
                    # Нам потрібен його IP саме в нашій внутрішній мережі (course_work_network).
                    networks = container.attrs['NetworkSettings']['Networks']

                    if self.network_name in networks:
                        ip_address = networks[self.network_name]['IPAddress']
                        active_ips.append(ip_address)

        except Exception as e:
            # Перехоплюємо помилки доступу до сокета Docker або проблеми з правами
            logger.error(f"Критична помилка Service Discovery (Docker API): {e}")

        return active_ips

        # ... (попередні методи ініціалізації та update_config_from_redis залишаються вище) ...

        async def fetch_metrics(self, session: aiohttp.ClientSession, ip: str) -> dict:
            """
            Асинхронно збирає метрики з одного конкретного бекенд-сервера.

            Параметри:
            - session: спільна сесія aiohttp для виконання неблокуючих HTTP-запитів.
            - ip: IP-адреса цільового контейнера.

            Цей метод реалізує патерн Circuit Breaker (Запобіжник). Якщо сервер "мертвий",
            ми не чекаємо на нього кожну секунду, а повертаємо дефолтні погані метрики.
            """
            # Перевірка статусу Circuit Breaker
            if self.failure_counters.get(ip, 0) >= self.math_config['CB_THRESHOLD']:
                logger.warning(
                    f"Circuit Breaker [OPEN]: Сервер {ip} ігнорується через {self.math_config['CB_THRESHOLD']} невдалих спроб.")
                return self.get_dead_metrics()

            url = f"http://{ip}:{config.BACKEND_PORT}/health"
            start_time = time.time()

            try:
                # Виконуємо HTTP GET запит з жорстким таймаутом (напр. 0.8 сек).
                # Якщо сервер не відповів за 0.8с, ми його відкидаємо, щоб не гальмувати загальний цикл.
                async with session.get(url, timeout=0.8) as response:
                    # Перевіряємо, чи повернув сервер статус 200 OK
                    response.raise_for_status()
                    data = await response.json()

                    # Обчислюємо реальний час відгуку (Latency) у мілісекундах
                    data['latency_ms'] = (time.time() - start_time) * 1000

                    # Оскільки сервер успішно відповів, скидаємо лічильник помилок (Circuit Breaker [CLOSED])
                    self.failure_counters[ip] = 0
                    return data

            except asyncio.TimeoutError:
                # Сервер завис
                logger.error(f"Таймаут підключення до {ip}")
                self.failure_counters[ip] = self.failure_counters.get(ip, 0) + 1
                return self.get_dead_metrics()

            except Exception as e:
                # Сервер впав, відхилив з'єднання або повернув 5xx помилку
                logger.error(f"Помилка доступу до {ip}: {str(e)}")
                self.failure_counters[ip] = self.failure_counters.get(ip, 0) + 1
                return self.get_dead_metrics()

        def get_dead_metrics(self) -> dict:
            """
            Допоміжний метод. Повертає набір метрик, що імітують 100% завантажений або "мертвий" сервер.
            Це змусить математичну функцію надати цьому серверу мінімальну вагу (Weight = 1),
            тим самим відвівши від нього трафік користувачів у Nginx.
            """
            return {
                'cpu_percent': 100.0,
                'ram_percent': 100.0,
                'active_connections': self.math_config['A_MAX'],
                'error_rate': 1.0,  # 100% помилок активує штрафний мультиплікатор
                'latency_ms': self.math_config['T_MAX']
            }

        def calculate_weight(self, ip: str, metrics: dict) -> Tuple[int, float]:
            """
            Ядро курсової роботи! Математичний рушій алгоритму адаптивного балансування.
            Він перетворює 5 сирих метрик сервера на одне ціле число (Weight) для Nginx.
            """
            cfg = self.math_config

            # КРОК 1: Нормалізація (Normalization)
            # Зводимо всі метрики до діапазону від 0.0 (вільний) до 1.0 (максимальне навантаження).
            # Використовуємо min(), щоб значення не перевищило 1.0 у разі екстремальних сплесків.
            c = min(metrics['cpu_percent'] / 100.0, 1.0)
            m = min(metrics['ram_percent'] / 100.0, 1.0)
            r = min(metrics['latency_ms'] / cfg['T_MAX'], 1.0)
            q = min(metrics['active_connections'] / cfg['A_MAX'], 1.0)
            e = metrics['error_rate']  # Відсоток помилок (від 0.0 до 1.0)

            # КРОК 2: Розрахунок базового навантаження (Base Load Score)
            # Застосовуємо вагові коефіцієнти багатокритеріального аналізу.
            l_base = (cfg['ALPHA'] * c) + (cfg['BETA'] * m) + (cfg['GAMMA'] * r) + (cfg['DELTA'] * q)

            # КРОК 3: Штрафна функція за помилки (Error Penalty)
            # Якщо сервер видає помилки (e > 0), його базове навантаження множиться на великий коефіцієнт.
            # Це примусово робить його "перевантаженим" в очах алгоритму.
            l_curr = l_base * (1.0 + cfg['PENALTY'] * e)

            # КРОК 4: Експоненційне згладжування (EWMA - Exponentially Weighted Moving Average)
            # Змішуємо поточне розраховане навантаження (l_curr) із навантаженням за попередню секунду.
            # Це рятує систему від різких стрибків ваги і забезпечує плавний перерозподіл трафіку.
            prev_l_smooth = self.history_l_smooth.get(ip, 0.0)  # Для нового сервера історія дорівнює 0
            l_smooth = (cfg['RHO'] * l_curr) + ((1.0 - cfg['RHO']) * prev_l_smooth)

            # Зберігаємо згладжене навантаження для наступної ітерації циклу
            self.history_l_smooth[ip] = l_smooth

            # КРОК 5: Конвертація в Nginx Weight
            # Nginx очікує вагу як ціле число. Чим більше число - тим більше трафіку.
            # Тому ми інвертуємо навантаження (1 - навантаження = вільна потужність).
            c_avail = max(0.0, 1.0 - l_smooth)
            weight = max(1, int(c_avail * 100))  # Вага ніколи не падає нижче 1

            return weight, l_smooth

        async def main_cycle(self):
            """
            Серце Control Plane. Головний асинхронний безкінечний цикл (Event Loop).
            Виконується 1 раз на секунду (залежить від POLL_INTERVAL).
            """
            # Відкриваємо єдину HTTP сесію для ефективного повторного використання з'єднань (Connection Pooling)
            async with aiohttp.ClientSession() as session:

                while self.running:
                    cycle_start = time.time()

                    # 1. ДИНАМІЧНЕ ОНОВЛЕННЯ ТА ПОШУК СЕРВЕРІВ
                    await self.update_config_from_redis()
                    # Викликаємо синхронний код Docker API в окремому потоці,
                    # щоб не заблокувати головний асинхронний цикл.
                    active_ips = await asyncio.to_thread(self.get_active_backends)

                    if not active_ips:
                        logger.warning("Бекенди не знайдені. Очікування...")
                        await asyncio.sleep(config.POLL_INTERVAL)
                        continue

                    # 2. ПАРАЛЕЛЬНИЙ ЗБІР МЕТРИК (Asynchronous Gathering)
                    # Магія asyncio: ми формуємо список задач (запитів до кожного сервера)
                    # і запускаємо їх усі ОДНОЧАСНО. Цикл чекатиме рівно стільки часу,
                    # скільки відповідає найповільніший сервер.
                    tasks = [self.fetch_metrics(session, ip) for ip in active_ips]
                    metrics_results = await asyncio.gather(*tasks)

                    # 3. ПІДГОТОВКА ТРАНЗАКЦІЇ REDIS (Atomic Operation)
                    # Використання pipeline() гарантує, що Nginx не прочитає напівпорожні дані
                    # під час оновлення списку серверів.
                    pipeline = self.redis.pipeline()
                    pipeline.delete("active_backends")
                    pipeline.sadd("active_backends", *active_ips)

                    # 4. ОБЧИСЛЕННЯ ТА ФОРМУВАННЯ КОМАНД ДЛЯ БАЗИ
                    for ip, metrics in zip(active_ips, metrics_results):
                        weight, l_smooth = self.calculate_weight(ip, metrics)

                        # Додаємо команду оновлення ваги в нашу транзакцію
                        pipeline.set(f"weight_{ip}", weight)
                        logger.info(
                            f"[{ip}] Навантаження: {l_smooth:.2f} | Вага (Nginx): {weight:3d} | Err: {metrics['error_rate']:.1f} | CPU: {metrics['cpu_percent']}%")

                    # 5. ВИКОНАННЯ ЗМІН ТА ОЧИЩЕННЯ
                    await pipeline.execute()  # Всі зміни застосовуються в Redis за 1 мілісекунду
                    self._garbage_collect(active_ips)  # Прибираємо сміття за "мертвими" контейнерами

                    # 6. СИНХРОНІЗАЦІЯ ЧАСУ (Dynamic Sleep)
                    # Якщо збір метрик і розрахунки зайняли 0.2с, ми спимо 0.8с.
                    # Якщо зайняли 0.9с, ми спимо 0.1с. Це тримає ідеальний такт в 1.0с.
                    elapsed = time.time() - cycle_start
                    sleep_time = max(0, config.POLL_INTERVAL - elapsed)
                    await asyncio.sleep(sleep_time)

        def _garbage_collect(self, active_ips: List[str]):
            """
            Memory Management (Garbage Collection).
            Видаляє зі словників стану історію тих серверів, які були видалені або зупинені.
            Без цього методу, якщо постійно створювати та видаляти контейнери,
            оперативна пам'ять воркера поступово переповниться (Memory Leak).
            """
            dead_ips = set(self.history_l_smooth.keys()) - set(active_ips)
            for ip in dead_ips:
                del self.history_l_smooth[ip]
                if ip in self.failure_counters:
                    del self.failure_counters[ip]

    # ============================================================================
    # ТОЧКА ВХОДУ ПРОГРАМИ (Entry Point)
    # ============================================================================
    async def main():
        balancer = AdaptiveLoadBalancer()

        # Реєстрація системних сигналів (SIGINT, SIGTERM).
        # Коли Docker намагається зупинити контейнер, він посилає сигнал SIGTERM.
        # Наш застосунок ловить його, змінює self.running на False, дозволяє циклу
        # безпечно завершитися і закрити з'єднання з базою.
        signal.signal(signal.SIGINT, lambda s, f: setattr(balancer, 'running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(balancer, 'running', False))

        logger.info("=== Control Plane (Адаптивний Балансувальник) Запущено ===")

        try:
            # Запуск головного асинхронного циклу
            await balancer.main_cycle()
        finally:
            # Цей блок виконається завжди при зупинці програми, звільняючи ресурси
            await balancer.redis.close()
            logger.info("З'єднання з Redis закрито. Виконано Graceful Shutdown.")

    if __name__ == "__main__":
        # Пауза перед стартом, щоб Docker встиг повністю підняти контейнери Redis та Nginx
        time.sleep(3)
        # Запуск асинхронної програми (Event Loop)
        asyncio.run(main())