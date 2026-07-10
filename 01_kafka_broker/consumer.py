import json
from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'dvd-rental-realtime-group-v2', 
    'auto.offset.reset': 'earliest', 
    'enable.auto.commit': True
}

consumer = Consumer(conf)

TARGET_TOPIC = 'cdc.public.customer'
consumer.subscribe([TARGET_TOPIC])

print(f"🎧 Pipeline Thông Mạch! Đang lắng nghe luồng dữ liệu tại Topic: '{TARGET_TOPIC}'...")
print("🚀 Hãy thực hiện INSERT/UPDATE vào RDS Postgres để xem phép thuật...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Lỗi mạng Kafka rồi đại ca: {msg.error()}")
                continue
                
        if msg.value() is None:  
            deleted_id = msg.key().decode('utf-8') if msg.key() else "Unknown"
            print(f"🪦 [TOMBSTONE EVENT] Biến động DELETE cứng phát hiện! Kafka đang dọn rác cho ID: {deleted_id}")
            continue

        raw_json = msg.value().decode('utf-8')
        event = json.loads(raw_json)
        
        payload = event.get('payload')
        if not payload:
            continue
            
        op = payload.get('op') 
        
        if op == 'c': 
            after = payload.get('after')
            full_name = f"{after.get('first_name', '')} {after.get('last_name', '')}"
            print(f"🟢 [INSERT DETECTED] -> ID Khách: {after.get('customer_id')} | Tên: {full_name} | Email: {after.get('email')}")
            
        elif op == 'u': 
            before = payload.get('before')
            after = payload.get('after')
            print(f"🟡 [UPDATE DETECTED] -> ID Khách: {after.get('customer_id')} | Email cũ: {before.get('email')} ➡️ Email mới: {after.get('email')}")
            
        elif op == 'd':
            before = payload.get('before')
            print(f"🔴 [DELETE DETECTED] -> Khách hàng có ID: {before.get('customer_id')} vừa bị xóa khỏi Database!")

except KeyboardInterrupt:
    print("\n🚪 Đã ngắt kết nối an toàn!")
finally:
    consumer.close()