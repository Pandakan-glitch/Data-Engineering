from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',          # IMPORTANT
    retries=5
)

while True:
    data = {
        "Order_ID": random.randint(1, 10000),
        "Customer_Name": random.choice(["Alice", "Bob", "John"]),
        "Region": random.choice(["North", "South", "East", "West"]),
        "Order_Date": "2026-04-20",
        "Product": random.choice(["Laptop", "Phone", "Tablet"]),
        "Quantity": random.randint(1, 5),
        "Unit_Price": random.randint(100, 1000),
        "Total_Amount": 0,
        "Payment_Method": random.choice(["Cash", "Card"])
    }

    data["Total_Amount"] = data["Quantity"] * data["Unit_Price"]

    future = producer.send("sales_topic", data)
    producer.flush()   # 🔥 THIS IS THE KEY FIX

    print("Sent:", data)
    time.sleep(1)
