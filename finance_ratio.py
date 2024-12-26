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
    SELECT 1 FROM income_statement
    WHERE "CP" = '{cp}' AND "Năm" = {year}
    LIMIT 1
    """
    cursor.execute(query)
    return cursor.fetchone() is not None

# Hàm lấy danh sách cột hiện có trong bảng Snowflake
def get_existing_columns(cursor):
    cursor.execute("DESCRIBE TABLE income_statement")
    return [col[0].upper() for col in cursor.fetchall()]

def add_new_columns_to_snowflake(df, cursor):
    existing_columns = get_existing_columns(cursor)
    for col in df.columns:
        if col.upper() not in existing_columns:
            print(f"Column {col} not found in Snowflake. Adding it...")
            add_column_query = f"""
            ALTER TABLE income_statement ADD COLUMN "{col}" STRING
            """
            try:
                cursor.execute(add_column_query)
                print(f"Successfully added column: {col}")
            except Exception as e:
                print(f"Error adding column {col}: {e}")

# Hàm gửi dữ liệu vào Kafka trước khi gửi đến Snowflake
def send_data_to_kafka(df):
    print("Sending data to Kafka...")
    for _, row in df.iterrows():
        row_data = row.to_dict()
        # Gửi dữ liệu vào Kafka (dữ liệu sẽ được gửi vào topic 'income_statement')
        producer.send('income_statement', value=row_data)
        print(f"Sent data for {row['CP']} in {row['Năm']} to Kafka.")
    producer.flush()

# Hàm gửi dữ liệu vào Snowflake
def send_data_to_snowflake(df):
    cursor = snowflake_connection.cursor()
    try:
        print("Sending data to Snowflake...")
        df = clean_column_names(df)
        print(f"Data to insert:\n{df.head()}")

        add_new_columns_to_snowflake(df, cursor)

        for _, row in df.iterrows():
            cp = row["CP"]
            year = row["Năm"]

            if is_duplicate(cursor, cp, year):
                print(f"Data for {cp} in {year} already exists. Skipping...")
                continue

            available_columns = [col for col in df.columns if col in row.index]
            escaped_columns = [f'"{col}"' for col in available_columns]
            values = [format_value(row[col]) for col in available_columns]
            insert_query = f"""
            INSERT INTO income_statement (
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

# Hàm lấy dữ liệu từ Vnstock và xử lý
def fetch_and_send_all_ratios():
    try:
        print("Fetching list of all stock symbols...")
        stock = Vnstock().stock(symbol='ACB', source='VCI')

        all_symbols = stock.listing.all_symbols()['ticker'].tolist()
        print(f"Found {len(all_symbols)} stock symbols.")

        for symbol in all_symbols:
            try:
                print(f"Fetching ratios for {symbol}...")

                # Thay đổi từ income_statement thành ratio
                data = stock.finance.ratio(symbol=symbol, period='year', lang='vi', dropna=True)

                # Loại bỏ MultiIndex nếu có
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.droplevel(0)

                # Thêm cột "CP" (Mã cổ phiếu)
                data["CP"] = symbol

                # Gửi dữ liệu vào Kafka trước khi gửi lên Snowflake nếu không rỗng
                if not data.empty:
                    send_data_to_kafka(data)
                    send_data_to_snowflake(data)
                else:
                    print(f"No data for {symbol}.")
            except Exception as e:
                print(f"Error fetching or sending data for {symbol}: {e}")
    except Exception as e:
        print(f"Error fetching all stock symbols: {e}")

# Thực thi
fetch_and_send_all_ratios()
