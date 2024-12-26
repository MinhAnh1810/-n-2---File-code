import snowflake.connector
import pandas as pd
from vnstock3 import Vnstock
from datetime import datetime
from kafka import KafkaProducer
import json

# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Địa chỉ Kafka, sửa nếu cần
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kết nối đến Snowflake
snowflake_connection = snowflake.connector.connect(
    user='MINHANH',
    password='Minhanh@1810',
    account='csjqrjc-cu38499',
    warehouse='COMPUTE_WH',
    database='STOCK_DATA',
    schema='ALL_STOCK_INFORMATION'
)

# Hàm kiểm tra dữ liệu trùng lặp trong Snowflake
def is_duplicate(cursor, symbol):
    query = f"""
    SELECT 1 FROM SYMBOLS_BY_EXCHANGE 
    WHERE SYMBOL = '{symbol}'
    LIMIT 1
    """
    cursor.execute(query)
    return cursor.fetchone() is not None

# Hàm định dạng giá trị để tránh lỗi cú pháp SQL
def format_value(value):
    if pd.isnull(value):
        return "NULL"
    elif isinstance(value, str):
        return f"'{value}'"
    else:
        return str(value)

# Hàm gửi danh sách cổ phiếu và tổ chức vào Kafka
def send_symbols_to_kafka(symbols_data):
    print("Sending symbols and organizations to Kafka...")
    for symbol_data in symbols_data:
        # Gửi dữ liệu vào Kafka (dữ liệu sẽ được gửi vào topic 'symbols_by_exchange')
        producer.send('symbols_by_exchange', value=symbol_data)
        print(f"Sent symbol {symbol_data['symbol']} to Kafka.")
    producer.flush()

# Hàm gửi danh sách cổ phiếu và tổ chức vào Snowflake
def send_symbols_to_snowflake(symbols_data, table_name):
    cursor = snowflake_connection.cursor()
    try:
        print("Sending symbols and organizations to Snowflake...")
        
        # Gửi danh sách cổ phiếu vào Kafka trước khi gửi tới Snowflake
        send_symbols_to_kafka(symbols_data)
        
        # Xử lý từng cổ phiếu và tổ chức
        for symbol_data in symbols_data:
            symbol = symbol_data['symbol']
            # Kiểm tra nếu symbol đã tồn tại trong Snowflake
            if is_duplicate(cursor, symbol):
                print(f"Symbol {symbol} already exists in Snowflake. Skipping insertion.")
                continue  # Bỏ qua nếu dữ liệu đã tồn tại
            
            # Tạo câu lệnh INSERT INTO động
            insert_query = f"""
            INSERT INTO {table_name} (SYMBOL, ID, TYPE, EXCHANGE, EN_ORGAN_NAME, EN_ORGAN_SHORT_NAME, ORGAN_SHORT_NAME, ORGAN_NAME) 
            VALUES (
                {format_value(symbol_data['symbol'])}, 
                {format_value(symbol_data['id'])},
                {format_value(symbol_data['type'])},
                {format_value(symbol_data['exchange'])},
                {format_value(symbol_data['en_organ_name'])},
                {format_value(symbol_data['en_organ_short_name'])},
                {format_value(symbol_data['organ_short_name'])},
                {format_value(symbol_data['organ_name'])}
            )
            """
            
            # Gửi truy vấn vào Snowflake
            cursor.execute(insert_query)
        
        # Commit các thay đổi
        snowflake_connection.commit()
        print(f"Symbols and organizations successfully sent to {table_name}.")
    except Exception as e:
        print(f"Error inserting symbols into Snowflake: {e}")
    finally:
        cursor.close()

# Hàm lấy danh sách tất cả các cổ phiếu và tên tổ chức từ từng sàn giao dịch
def fetch_all_symbols_by_exchange_and_send():
    try:
        stock = Vnstock().stock(symbol='ACB', source='VCI')  # Lấy một cổ phiếu bất kỳ để có danh sách
        all_symbols_data = stock.listing.symbols_by_exchange()  # Lấy tất cả các mã cổ phiếu theo sàn giao dịch
        
        # Tạo danh sách gồm các trường SYMBOL, ID, TYPE, EXCHANGE, EN_ORGAN_NAME, EN_ORGAN_SHORT_NAME, ORGAN_SHORT_NAME, ORGAN_NAME
        symbols_data = [{
            'symbol': row['symbol'],
            'id': row['id'],
            'type': row['type'],
            'exchange': row['exchange'],
            'en_organ_name': row['en_organ_name'],
            'en_organ_short_name': row['en_organ_short_name'],
            'organ_short_name': row['organ_short_name'],
            'organ_name': row['organ_name']
        } for index, row in all_symbols_data.iterrows()]
        
        # Gửi danh sách cổ phiếu và tổ chức vào Snowflake
        send_symbols_to_snowflake(symbols_data, table_name='SYMBOLS_BY_EXCHANGE')
    except Exception as e:
        print(f"Error fetching or sending symbols: {e}")

# Thực thi để gửi danh sách cổ phiếu và tổ chức vào Snowflake
fetch_all_symbols_by_exchange_and_send()
