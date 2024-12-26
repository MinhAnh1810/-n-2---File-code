import snowflake.connector
import pandas as pd
from vnstock3 import Vnstock
from kafka import KafkaProducer
import json

# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Địa chỉ Kafka
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

# Hàm thay đổi tên cột để thay thế ký tự '/' thành '_'
def clean_column_names(df):
    df.columns = df.columns.str.replace(' / ', '/', regex=False)
    return df

# Hàm định dạng giá trị, thay thế dữ liệu rỗng bằng NULL và xử lý ký tự đặc biệt
def format_value(value):
    if pd.isna(value) or value == "":
        return "NULL"
    elif isinstance(value, str):
        value = value.replace("'", "\\'").replace("’", "\\’")
        return f"'{value}'"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return "NULL"

# Hàm kiểm tra dữ liệu trùng lặp trong Snowflake
def is_duplicate(cursor, cp, year):
    query = f"""
    SELECT 1 FROM balance_sheet
    WHERE "CP" = '{cp}' AND "Năm" = {year}
    LIMIT 1
    """
    cursor.execute(query)
    return cursor.fetchone() is not None

# Hàm lấy danh sách cột hiện có trong bảng Snowflake
def get_existing_columns(cursor):
    # Lấy danh sách các cột hiện có trong bảng balance_sheet
    cursor.execute("DESCRIBE TABLE balance_sheet")
    return [col[0].upper() for col in cursor.fetchall()]

def add_new_columns_to_snowflake(df, cursor):
    existing_columns = get_existing_columns(cursor)
    for col in df.columns:
        # Kiểm tra xem cột có trong bảng Snowflake không (không phân biệt chữ hoa, chữ thường)
        if col.upper() not in existing_columns:
            print(f"Column {col} not found in Snowflake. Adding it...")
            add_column_query = f"""
            ALTER TABLE balance_sheet ADD COLUMN "{col}" STRING
            """
            try:
                cursor.execute(add_column_query)
                print(f"Successfully added column: {col}")
            except Exception as e:
                print(f"Error adding column {col}: {e}")


# Hàm gửi dữ liệu vào Snowflake
def send_data_to_snowflake(df):
    cursor = snowflake_connection.cursor()
    try:
        print("Sending data to Snowflake...")
        df = clean_column_names(df)
        print(f"Data to insert:\n{df.head()}")

        # Thêm cột mới nếu có
        add_new_columns_to_snowflake(df, cursor)

        for _, row in df.iterrows():
            cp = row["CP"]
            year = row["Năm"]

            # Kiểm tra trùng lặp
            if is_duplicate(cursor, cp, year):
                print(f"Data for {cp} in {year} already exists. Skipping...")
                continue

            available_columns = [col for col in df.columns if col in row.index]
            escaped_columns = [f'"{col}"' for col in available_columns]
            values = [format_value(row[col]) for col in available_columns]
            insert_query = f"""
            INSERT INTO balance_sheet (
                {', '.join(escaped_columns)}
            ) VALUES (
                {', '.join(values)}
            )
            """
            print(f"Executing SQL: {insert_query}")
            cursor.execute(insert_query)
        
        snowflake_connection.commit()
        print("Successfully sent data to Snowflake.")
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")
    finally:
        cursor.close()

# Hàm gửi dữ liệu vào Kafka trước khi gửi đến Snowflake
def send_data_to_kafka(df):
    print("Sending data to Kafka...")
    for _, row in df.iterrows():
        row_data = row.to_dict()
        # Gửi dữ liệu vào Kafka (dữ liệu sẽ được gửi vào topic 'stock_data')
        producer.send('stock_data', value=row_data)
        print(f"Sent data for {row['CP']} in {row['Năm']} to Kafka.")
    producer.flush()

# Hàm lấy dữ liệu từ Vnstock và xử lý
def fetch_and_send_all_balance_sheets():
    try:
        print("Fetching list of all stock symbols...")
        stock = Vnstock().stock(symbol='ACB', source='VCI')
        
        # Lấy danh sách mã cổ phiếu từ cột 'ticker'
        all_symbols = stock.listing.all_symbols()['ticker'].tolist()

        print(f"Found {len(all_symbols)} stock symbols.")
        
        for symbol in all_symbols:
            try:
                print(f"Fetching balance sheet for {symbol}...")

                # Lấy dữ liệu bảng cân đối kế toán
                stock_data = stock.finance.balance_sheet(symbol=symbol, period='year', lang='vi', dropna=True)
                
                # Loại bỏ MultiIndex nếu có
                if isinstance(stock_data.columns, pd.MultiIndex):
                    stock_data.columns = stock_data.columns.droplevel(0)

                # Thêm cột "CP" (Mã cổ phiếu)
                stock_data["CP"] = symbol

                # Gửi dữ liệu vào Kafka trước khi gửi lên Snowflake nếu không rỗng
                if not stock_data.empty:
                    send_data_to_kafka(stock_data)
                    send_data_to_snowflake(stock_data)
                else:
                    print(f"No data for {symbol}.")
            except Exception as e:
                print(f"Error fetching or sending data for {symbol}: {e}")
    except Exception as e:
        print(f"Error fetching all stock symbols: {e}")

# Thực thi
fetch_and_send_all_balance_sheets()
