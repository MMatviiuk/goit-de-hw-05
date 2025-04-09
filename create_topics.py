from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config

# Задаємо ваше ім'я або унікальний ідентифікатор
my_name = "m_matviiuk"

# Список імен топіків, які потрібно створити (для даних сенсорів та сповіщень)
required_topic_names = [
    f'{my_name}_building_sensors',     # дані з сенсорів
    f'{my_name}_temperature_alerts',    # сповіщення по температурі
    f'{my_name}_humidity_alerts'        # сповіщення по вологості
]

# Ініціалізація AdminClient
admin_client = AdminClient({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "security.protocol": kafka_config["security_protocol"],
    "sasl.mechanism": kafka_config["sasl.mechanism"],
    "sasl.username": kafka_config["username"],
    "sasl.password": kafka_config["password"]
})

# Отримання списку існуючих топіків
existing_topics = admin_client.list_topics(timeout=10).topics.keys()

# Формування списку топіків, які потрібно створити
topics_to_create = []
for topic in required_topic_names:
    if topic not in existing_topics:
        topics_to_create.append(NewTopic(topic, num_partitions=2, replication_factor=1))

# Створення нових топіків (якщо потрібно)
if topics_to_create:
    futures = admin_client.create_topics(topics_to_create)
    for topic, future in futures.items():
        try:
            future.result()  # Чекаємо результат створення
            print(f"Топік '{topic}' створено успішно.")
        except Exception as e:
            print(f"Виникла помилка при створенні топіку '{topic}': {e}")
else:
    print("Усі ваші топіки вже існують, створення не потрібне.")

# Отримання та вивід списку всіх топіків
all_topics = sorted(admin_client.list_topics(timeout=10).topics.keys())
my_topics = [t for t in all_topics if t.startswith(my_name)]
other_topics = [t for t in all_topics if not t.startswith(my_name)]

print("\n=== Ваші топіки ===")
for topic in my_topics:
    print(topic)

print("\n=== Інші топіки ===")
for topic in other_topics:
    print(topic)

# Бібліотека confluent-kafka не вимагає виклику close() для AdminClient

