# Файл: producer.py
from confluent_kafka import Producer
from configs import kafka_config
import json, uuid, time, random

# Встановлення унікального ідентифікатора для сенсора (однаковий протягом одного запуску)
sensor_id = str(uuid.uuid4())

# Задаємо унікальний префікс (ваше ім'я) і назву топіку для даних сенсорів
my_name = "m_matviiuk"
topic_name = f"{my_name}_building_sensors"

# Створення продюсера
producer = Producer({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "security.protocol": kafka_config["security_protocol"],
    "sasl.mechanism": kafka_config["sasl.mechanism"],
    "sasl.username": kafka_config["username"],
    "sasl.password": kafka_config["password"]
})

# Генерація даних сенсора:
# Температура від 25 до 45°C, вологість від 15 до 85%
for i in range(30):
    temperature = random.randint(25, 45)
    humidity = random.randint(15, 85)
    
    # Формування повідомлення з даними сенсора
    data = {
        "sensor_id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": time.time()
    }
    try:
        # Надсилання повідомлення у топік з даними сенсорів
        producer.produce(topic_name, value=json.dumps(data))
        producer.flush()  # Очікування відправлення
        print(f"Повідомлення {i} успішно надіслано до топіку '{topic_name}': {data}")
        time.sleep(2)  # Затримка між відправленнями
    except Exception as e:
        print(f"Виникла помилка: {e}")

producer.flush()
