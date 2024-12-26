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

# Hàm định dạng kiểu dữ liệu để khớp với Snowflake
def format_dataframe(df):
    # Chuyển 'TIME' thành kiểu datetime, sau đó chỉ giữ ngày
    df['time'] = pd.to_datetime(df['time']).dt.date
    
    # Loại bỏ các dòng có giá trị NaT trong 'TIME'
    df = df.dropna(subset=['time'])
    
    return df

# Hàm kiểm tra dữ liệu trùng lặp trong Snowflake
def is_duplicate(cursor, ticker, time):
    query = f"""
    SELECT 1 FROM STOCK_DATA.ALL_STOCK_INFORMATION.STOCK_HISTORY
    WHERE TICKER = '{ticker}' AND TIME = TO_DATE('{time}', 'YYYY-MM-DD')
    LIMIT 1
    """
    cursor.execute(query)
    return cursor.fetchone() is not None

# Hàm gửi dữ liệu vào Kafka
def send_data_to_kafka(df):
    print("Sending data to Kafka...")
    for _, row in df.iterrows():
        data = {
            'time': row['time'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'ticker': row['ticker']
        }
        # Gửi dữ liệu vào Kafka (dữ liệu sẽ được gửi vào topic 'stock_history')
        producer.send('stock_history', value=data)
        print(f"Sent data for {row['ticker']} on {row['time']} to Kafka.")

    # Đảm bảo dữ liệu được gửi hết
    producer.flush()

# Hàm gửi dữ liệu vào Snowflake
def send_data_to_snowflake(df):
    cursor = snowflake_connection.cursor()
    try:
        print("Sending data to Snowflake...")

        # Làm sạch và định dạng DataFrame
        df = format_dataframe(df)

        # Kiểm tra nếu DataFrame rỗng hoặc không có dữ liệu hợp lệ
        if df.empty:
            return  # Không in thông báo nếu không có dữ liệu hợp lệ

        for _, row in df.iterrows():
            ticker = row['ticker']
            time = row['time']

            # Kiểm tra trùng lặp
            if is_duplicate(cursor, ticker, time):
                continue  # Bỏ qua nếu dữ liệu đã có sẵn trong Snowflake

            # Tạo câu lệnh INSERT
            insert_query = f"""
            INSERT INTO STOCK_DATA.ALL_STOCK_INFORMATION.STOCK_HISTORY (
                TIME, OPEN, HIGH, LOW, CLOSE, VOLUME, TICKER
            ) VALUES (
                TO_DATE('{row['time']}', 'YYYY-MM-DD'), {row['open']}, {row['high']}, {row['low']}, 
                {row['close']}, {row['volume']}, '{row['ticker']}'
            )
            """
            cursor.execute(insert_query)

            # In ra thông báo khi gửi thành công
            print(f"Data for {ticker} on {time} has been successfully inserted into Snowflake.")

        snowflake_connection.commit()
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")
    finally:
        cursor.close()

# Hàm lấy dữ liệu lịch sử cổ phiếu và gửi vào Snowflake và Kafka
def fetch_stock_history_and_send(symbol):
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')

        # Lấy dữ liệu lịch sử giá cổ phiếu từ đầu năm đến hôm nay
        today = datetime.today().strftime('%Y-%m-%d')
        df = stock.quote.history(start='2024-01-01', end=today, interval='1D')

        # Kiểm tra nếu DataFrame rỗng hoặc không chứa cột 'time'
        if df.empty or 'time' not in df.columns:
            return  # Bỏ qua nếu không có dữ liệu hợp lệ

        # Thêm cột mã cổ phiếu (ticker)
        df['ticker'] = symbol

        # Gửi dữ liệu lên Kafka trước khi gửi vào Snowflake
        send_data_to_kafka(df)

        # Gửi dữ liệu vào Snowflake nếu không rỗng
        send_data_to_snowflake(df)
    except Exception as e:
        print(f"Error fetching or sending data for {symbol}: {e}")

# Hàm lấy danh sách tất cả các cổ phiếu
def fetch_all_symbols_and_send_data():
    try:
        stock = Vnstock().stock(symbol='ACB', source='VCI')  # Lấy một cổ phiếu bất kỳ để có thể lấy danh sách
        all_symbols = stock.listing.all_symbols()['ticker']  # Lấy tất cả các mã cổ phiếu

        # Gửi dữ liệu cho từng cổ phiếu trong danh sách
        for symbol in all_symbols:
            fetch_stock_history_and_send(symbol)
    except Exception as e:
        print(f"Error fetching or sending data for all symbols: {e}")

# Thực thi để gửi dữ liệu cho tất cả cổ phiếu
fetch_all_symbols_and_send_data()
