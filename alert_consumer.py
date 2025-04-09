# Файл: alert_consumer.py
from confluent_kafka import Consumer
from configs import kafka_config
import json

# Коментар українською: Унікальний префікс (ваше ім'я)
my_name = "m_matviiuk"
temperature_topic = f"{my_name}_temperature_alerts"
humidity_topic = f"{my_name}_humidity_alerts"

# Коментар українською: Створення споживача для топіків сповіщень
consumer = Consumer({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "security.protocol": kafka_config["security_protocol"],
    "sasl.mechanism": kafka_config["sasl.mechanism"],
    "sasl.username": kafka_config["username"],
    "sasl.password": kafka_config["password"],
    "group.id": "alert_group",
    "auto.offset.reset": "earliest"
})

# Коментар українською: Підписуємося на температурний та вологісний топіки
consumer.subscribe([temperature_topic, humidity_topic])

print(f"Підписано на топіки: {temperature_topic}, {humidity_topic}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Помилка: {msg.error()}")
        else:
            # Коментар українською: перетворюємо тіло повідомлення з JSON
            alert = json.loads(msg.value().decode("utf-8"))

            # Коментар українською: Витягаємо потрібні поля з обмеженнями довжини (щоб скоротити рядок)
            sensor_id = str(alert.get("sensor_id", ""))[:12]  # перші 12 символів
            temperature = alert.get("temperature")
            humidity = alert.get("humidity")
            message = str(alert.get("message", ""))[:30]  # перші 30 символів
            timestamp = alert.get("timestamp", "")

            # Коментар українською: Формуємо рядок виводу: виводимо все основне, але коротко
            # Приклад: [m_matviiuk_temperature_alerts] sensor='06b35ed2-4f' temp=41 hum=None msg='Температура перевищує 40°' ts=1680952738.116...
            print(
                f"[{msg.topic()}] "
                f"sensor='{sensor_id}' "
                f"temp={temperature if temperature is not None else 'None'} "
                f"hum={humidity if humidity is not None else 'None'} "
                f"msg='{message}' "
                f"ts={timestamp}"
            )
except KeyboardInterrupt:
    print("Зупинка споживача сповіщень...")
finally:
    consumer.close()
    print("Споживач сповіщень зупинено.")



