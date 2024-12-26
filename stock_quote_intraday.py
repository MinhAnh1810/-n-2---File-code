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

# Hàm định dạng giá trị để tránh lỗi cú pháp SQL
def format_value(value):
    if pd.isnull(value):
        return "NULL"
    elif isinstance(value, str):
        return f"'{value}'"
    else:
        return str(value)
    
def format_dataframe(df):
    if 'time' in df.columns:
        # Chuyển cột 'time' sang kiểu datetime và định dạng chuẩn
        df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        # Loại bỏ các dòng có giá trị NaT (lỗi định dạng)
        df = df.dropna(subset=['time'])
    return df

# Hàm gửi dữ liệu vào Kafka trước khi gửi tới Snowflake
def send_data_to_kafka(df):
    print("Sending data to Kafka...")
    for _, row in df.iterrows():
        row_data = row.to_dict()
        # Gửi dữ liệu vào Kafka (dữ liệu sẽ được gửi vào topic 'stock_intraday')
        producer.send('stock_intraday', value=row_data)
        print(f"Sent data for {row['ticker']} at {row['time']} to Kafka.")
    producer.flush()

# Hàm gửi dữ liệu vào Snowflake
def send_data_to_snowflake_dynamic(df, table_name):
    cursor = snowflake_connection.cursor()
    try:
        print("Sending data to Snowflake...")
        
        # Làm sạch và định dạng DataFrame
        df = format_dataframe(df)  # Gọi hàm format_dataframe trước
        
        if df.empty:
            print("No data to send.")
            return
        
        # Gửi dữ liệu vào Kafka trước khi gửi tới Snowflake
        send_data_to_kafka(df)
        
        # Xử lý từng hàng trong DataFrame
        for _, row in df.iterrows():
            # Lấy danh sách các cột hiện có trong DataFrame
            available_columns = [col for col in df.columns if col in row.index]
            
            # Tạo danh sách giá trị tương ứng, đảm bảo 'time' được bao quanh bởi dấu nháy đơn
            values = [format_value(row[col]) for col in available_columns]
            
            # Tạo câu lệnh INSERT INTO động
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(available_columns)}) 
            VALUES ({', '.join(values)})
            """
            
            # Gửi truy vấn vào Snowflake
            cursor.execute(insert_query)
        
        # Commit các thay đổi
        snowflake_connection.commit()
        print(f"Data successfully sent to {table_name}.")
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")
    finally:
        cursor.close()

# Hàm lấy dữ liệu intraday cổ phiếu và gửi vào Snowflake
def fetch_stock_intraday_and_send(symbol, table_name):
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')

        # Lấy dữ liệu intraday
        df = stock.quote.intraday()

        # Thêm cột mã cổ phiếu (ticker)
        df['ticker'] = symbol

        # Gửi dữ liệu lên Snowflake
        send_data_to_snowflake_dynamic(df, table_name)
    except Exception as e:
        print(f"Error fetching or sending intraday data for {symbol}: {e}")

# Hàm lấy danh sách tất cả các cổ phiếu và gửi dữ liệu intraday
def fetch_all_symbols_and_send_intraday_data():
    try:
        stock = Vnstock().stock(symbol='ACB', source='VCI')  # Lấy một cổ phiếu bất kỳ để có danh sách
        all_symbols = stock.listing.all_symbols()['ticker']  # Lấy tất cả các mã cổ phiếu

        # Gửi dữ liệu intraday cho từng cổ phiếu trong danh sách
        for symbol in all_symbols:
            fetch_stock_intraday_and_send(symbol, table_name='stock_quote_intraday')
    except Exception as e:
        print(f"Error fetching or sending intraday data for all symbols: {e}")

# Thực thi để gửi dữ liệu intraday cho tất cả cổ phiếu
fetch_all_symbols_and_send_intraday_data()
