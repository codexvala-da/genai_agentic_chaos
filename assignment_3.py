import json
import time
import os
import threading
from datetime import datetime
from openai import OpenAI
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# ==========================================
# 1. SETUP & CONFIGURATION
# ==========================================
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def setup_kafka_topics():
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='setup_client')
    topics = [
        NewTopic(name='customer-queries', num_partitions=1, replication_factor=1),
        NewTopic(name='intent-classification', num_partitions=1, replication_factor=1),
        NewTopic(name='response-generation', num_partitions=1, replication_factor=1),
        NewTopic(name='final-responses', num_partitions=1, replication_factor=1)
    ]
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        print("✓ Topics created successfully!")
    except TopicAlreadyExistsError:
        print("✓ Topics already exist")
    finally:
        admin_client.close()

# ==========================================
# 2. AI AGENTS
# ==========================================

class QueryIntakeAgent:
    def __init__(self, agent_id):
        self.agent_id = agent_id
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.sample_queries = ["I can't log into my account", "Refund policy?", "Order #12345 status", "Double charged"]

    def start_publishing(self, duration):
        start_time = time.time()
        count = 0
        while (time.time() - start_time) < duration:
            query = self.sample_queries[count % len(self.sample_queries)]
            message = {'query_id': f'Q-{int(time.time())}-{count}', 'customer_query': query, 'customer_id': f'CUST-{1000 + count}'}
            self.producer.send('customer-queries', value=message)
            print(f"📩 [{self.agent_id}] New Query: {query}")
            count += 1
            time.sleep(10)

class IntentClassificationAgent:
    def __init__(self, agent_id):
        self.agent_id = agent_id
        self.consumer = KafkaConsumer('customer-queries', bootstrap_servers='localhost:9092',
                                     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                     group_id='intent-group')
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def start_processing(self, duration):
        start_time = time.time()
        while (time.time() - start_time) < duration:
            records = self.consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    query = msg.value['customer_query']
                    try:
                        # FIX: Added 'json' to the prompt to satisfy OpenAI requirements
                        res = client.chat.completions.create(
                            model="gpt-4o-mini",
                            messages=[{"role": "user", "content": f"Classify this query category and priority. Respond in JSON format: {query}"}],
                            response_format={"type": "json_object"}
                        )
                        msg.value['classification'] = json.loads(res.choices[0].message.content)
                        self.producer.send('intent-classification', value=msg.value)
                        print(f"🔍 [{self.agent_id}] Classified query.")
                    except Exception as e: print(f"Error: {e}")

class ResponseGenerationAgent:
    def __init__(self, agent_id):
        self.agent_id = agent_id
        self.consumer = KafkaConsumer('intent-classification', bootstrap_servers='localhost:9092',
                                     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                     group_id='response-group')
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def start_processing(self, duration):
        start_time = time.time()
        while (time.time() - start_time) < duration:
            records = self.consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    data = msg.value
                    res = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "system", "content": "You are helpful support."}, 
                                  {"role": "user", "content": f"Query: {data['customer_query']}"}]
                    )
                    data['generated_response'] = res.choices[0].message.content.strip()
                    self.producer.send('response-generation', value=data)
                    print(f"✍️  [{self.agent_id}] Generated response.")

class QualityReviewAgent:
    def __init__(self, agent_id):
        self.agent_id = agent_id
        self.consumer = KafkaConsumer('response-generation', bootstrap_servers='localhost:9092',
                                     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                     group_id='quality-group')
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def start_processing(self, duration):
        start_time = time.time()
        while (time.time() - start_time) < duration:
            records = self.consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    data = msg.value
                    try:
                        # FIX: Added 'json' to the prompt to satisfy OpenAI requirements
                        res = client.chat.completions.create(
                            model="gpt-4o-mini",
                            messages=[{"role": "user", "content": f"Review this response: {data['generated_response']}. Score 1-5 and 'approved' true/false. Provide the result in JSON."}],
                            response_format={"type": "json_object"}
                        )
                        data['quality_review'] = json.loads(res.choices[0].message.content)
                        self.producer.send('final-responses', value=data)
                        print(f"📋 [{self.agent_id}] Review complete.")
                    except Exception as e: print(f"Review Error: {e}")

class DashboardAgent:
    def __init__(self, agent_id):
        self.agent_id = agent_id
        self.consumer = KafkaConsumer('final-responses', bootstrap_servers='localhost:9092',
                                     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                     group_id='dashboard-group')

    def start_monitoring(self, duration):
        start_time = time.time()
        while (time.time() - start_time) < duration:
            records = self.consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    d = msg.value
                    print(f"\n--- FINAL OUTPUT ---\nID: {d['query_id']}\nResponse: {d['generated_response']}\nApproved: {d['quality_review'].get('approved')}\n--------------------")

# ==========================================
# 3. RUNTIME
# ==========================================
def run_system(duration=60):
    setup_kafka_topics()
    agents = [
        (QueryIntakeAgent("INTAKE"), "start_publishing"),
        (IntentClassificationAgent("INTENT"), "start_processing"),
        (ResponseGenerationAgent("RESPONSE"), "start_processing"),
        (QualityReviewAgent("QUALITY"), "start_processing"),
        (DashboardAgent("DASHBOARD"), "start_monitoring")
    ]
    threads = [threading.Thread(target=getattr(a, m), args=(duration,)) for a, m in agents]
    for t in threads: t.start()
    for t in threads: t.join()

if __name__ == "__main__":
    run_system(duration=120)