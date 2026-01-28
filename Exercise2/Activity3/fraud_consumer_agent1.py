# This agent calculates a running average for each user and flags transactions
# that are significantly higher than their usual behavior (e.g., $3\sigma$ outliers).

import json
import statistics
from kafka import KafkaConsumer

# Configuration

# In-memory store for user spending patterns
user_spending_profiles = {} 

def analyze_pattern(data):
    user_id = data['user_id']
    amount = float(data['amount'])

    if user_id not in user_spending_profiles:
        user_spending_profiles[user_id] = []

    history = user_spending_profiles[user_id]

    # Analyze if transaction is an outlier (Need at least 3 transactions to judge)
    is_anomaly = False
    if len(history) >= 3:
        avg = statistics.mean(history)
        stdev = statistics.stdev(history) if len(history) > 1 else 0

        # If amount is > 3x the average (Simple heuristic)
        if amount > (avg * 3) and amount > 500:
            is_anomaly = True

    # Update profile
    history.append(amount)
    # Keep only last 50 transactions per user for memory efficiency
    if len(history) > 50: history.pop(0)
    
    return is_anomaly

print("🧬 Anomaly Detection Agent started...")


# Create consumer
consumer = KafkaConsumer(
    'dbserver1.public.transactions',  # topic name
    bootstrap_servers=['127.0.0.1:9094'],  # Kafka server
    auto_offset_reset='earliest',  # Starting from beginning
    enable_auto_commit=False,
    group_id='fraud-consumer-group',  # Consumer group ID
    request_timeout_ms=30_000,      # 20 seconds
    connections_max_idle_ms=45_000,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Parse JSON
)

for message in consumer: #consumer has to be implemented before!
    payload = message.value.get('payload', {})
    data = payload.get('after')

    if data:
        # Match the variable name here...
        is_fraudulent_pattern = analyze_pattern(data)

        # ...with the variable name here
        if is_fraudulent_pattern:
            print(f"🚨 ANOMALY DETECTED: User {data['user_id']} spent ${data['amount']} (Significantly higher than average)")
        else:
            print(f"📊 Profile updated for User {data['user_id']}")
