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
def is_duplicate(cursor, ticker):
    query = f"""
    SELECT 1 FROM ALL_SYMBOLS 
    WHERE TICKER = '{ticker}'
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
    for ticker, organ_name in symbols_data:
        # Gửi dữ liệu vào Kafka (dữ liệu sẽ được gửi vào topic 'all_symbols')
        producer.send('all_symbols', value={'ticker': ticker, 'organ_name': organ_name})
        print(f"Sent symbol {ticker} with organization {organ_name} to Kafka.")
    producer.flush()

# Hàm gửi danh sách cổ phiếu và tổ chức vào Snowflake
def send_symbols_to_snowflake(symbols_data, table_name):
    cursor = snowflake_connection.cursor()
    try:
        print("Sending symbols and organizations to Snowflake...")
        
        # Gửi danh sách cổ phiếu vào Kafka trước khi gửi tới Snowflake
        send_symbols_to_kafka(symbols_data)
        
        # Xử lý từng cổ phiếu và tổ chức
        for ticker, organ_name in symbols_data:
            # Kiểm tra nếu ticker đã tồn tại trong Snowflake
            if is_duplicate(cursor, ticker):
                print(f"Ticker {ticker} already exists in Snowflake. Skipping insertion.")
                continue  # Bỏ qua nếu dữ liệu đã tồn tại
            
            # Tạo câu lệnh INSERT INTO động
            insert_query = f"""
            INSERT INTO {table_name} (TICKER, ORGAN_NAME) 
            VALUES ({format_value(ticker)}, {format_value(organ_name)})
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

# Hàm lấy danh sách tất cả các cổ phiếu và tên tổ chức
def fetch_all_symbols_and_send():
    try:
        stock = Vnstock().stock(symbol='ACB', source='VCI')  # Lấy một cổ phiếu bất kỳ để có danh sách
        all_symbols_data = stock.listing.all_symbols()  # Lấy tất cả các mã cổ phiếu và tổ chức
        
        # Tạo danh sách gồm TICKER và ORGAN_NAME
        symbols_data = [(row['ticker'], row['organ_name']) for index, row in all_symbols_data.iterrows()]
        
        # Gửi danh sách cổ phiếu và tổ chức vào Snowflake
        send_symbols_to_snowflake(symbols_data, table_name='ALL_SYMBOLS')
    except Exception as e:
        print(f"Error fetching or sending symbols: {e}")

# Thực thi để gửi danh sách cổ phiếu và tổ chức vào Snowflake
fetch_all_symbols_and_send()
