#This agent uses a sliding window (simulated) to perform velocity checks and score the transaction
import json
from collections import deque
import time
from kafka import KafkaConsumer

# Simulated In-Memory State for Velocity Checks.
user_history = {}


def analyze_fraud(transaction):
    user_id = transaction['user_id']
    amount = float(transaction['amount'])

    # 1. Velocity Check (Recent transaction count)
    now = time.time()
    if user_id not in user_history:
        user_history[user_id] = deque()

    # Keep only last 60 seconds of history
    user_history[user_id].append(now)
    while user_history[user_id] and user_history[user_id][0] < now - 60:
        user_history[user_id].popleft()

    velocity = len(user_history[user_id])

    # 2. Heuristic Fraud Scoring
    score = 0
    if velocity > 5: score += 40  # Too many transactions in a minute
    if amount > 4000: score += 50 # High value transaction

    # 3. Simulate ML Model Hand-off
    # model.predict([[velocity, amount]])

    return score

consumer = KafkaConsumer(
    'dbserver1.public.transactions',  # topic name
    bootstrap_servers=['127.0.0.1:9094'],  # Kafka server
    auto_offset_reset='earliest',  # Starting from beginning
    enable_auto_commit=False,
    group_id='fraud-consumer-group2',  # Consumer group ID
    request_timeout_ms=30_000,
    connections_max_idle_ms=45_000,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Parse JSON
)

print("Agent started. Listening for CDC events...")
for message in consumer:  #consumer has to be implemented before!
    # Debezium wraps data in an 'after' block
    payload = message.value.get('payload', {})
    data = payload.get('after')
    
    if data:
        fraud_score = analyze_fraud(data)
        if fraud_score > 70:
            print(f"⚠️ HIGH FRAUD ALERT: User {data['user_id']} | Score: {fraud_score} | Amt: {data['amount']}")
        else:
            print(f"✅ Transaction OK: {data['id']} (Score: {fraud_score})")