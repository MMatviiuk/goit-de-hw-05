# Файл: sensor_consumer.py
from confluent_kafka import Consumer, Producer
from configs import kafka_config
import json

# Встановлення унікального префіксу (ваше ім'я)
my_name = "m_matviiuk"
# Назви топіків
building_topic = f"{my_name}_building_sensors"         # топік з даними сенсорів
temperature_topic = f"{my_name}_temperature_alerts"      # топік для сповіщень по температурі
humidity_topic = f"{my_name}_humidity_alerts"            # топік для сповіщень по вологості

# Створення споживача для топіку з даними сенсорів
consumer = Consumer({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "security.protocol": kafka_config["security_protocol"],
    "sasl.mechanism": kafka_config["sasl.mechanism"],
    "sasl.username": kafka_config["username"],
    "sasl.password": kafka_config["password"],
    "group.id": "sensor_group",
    "auto.offset.reset": "earliest"
})
consumer.subscribe([building_topic])

# Створення продюсера для відправки сповіщень
producer = Producer({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "security.protocol": kafka_config["security_protocol"],
    "sasl.mechanism": kafka_config["sasl.mechanism"],
    "sasl.username": kafka_config["username"],
    "sasl.password": kafka_config["password"]
})


def process_message(msg):
    # Розбір отриманого повідомлення
    data = json.loads(msg.value().decode("utf-8"))
    sensor_id = data["sensor_id"]
    temperature = data["temperature"]
    humidity = data["humidity"]
    
    # Вивід отриманих даних
    print(f"\nОтримано дані від сенсора {sensor_id}: Температура = {temperature}°C, Вологість = {humidity}%")
    
    # Фільтрація за температурою:
    if temperature > 40:
        alert = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "timestamp": data["timestamp"],
            "message": "Температура перевищує 40°C!"
        }
        producer.produce(temperature_topic, key=sensor_id, value=json.dumps(alert))
        producer.flush()
        print(f"--> Сповіщення відправлено до топіку {temperature_topic} (температура)")
    
    # Фільтрація за вологістю:
    if humidity < 20 or humidity > 80:
        alert = {
            "sensor_id": sensor_id,
            "humidity": humidity,
            "timestamp": data["timestamp"],
            "message": "Вологість поза межами 20-80%!"
        }
        producer.produce(humidity_topic, key=sensor_id, value=json.dumps(alert))
        producer.flush()
        print(f"--> Сповіщення відправлено до топіку {humidity_topic} (вологість)")
    
    print("Дані успішно оброблено.\n")

print("Початок отримання даних з топіку сенсорів...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Помилка: {msg.error()}")
        else:
            process_message(msg)
except KeyboardInterrupt:
    print("Зупинка споживача даних...")
finally:
    consumer.close()
    print("Споживач зупинено.")

