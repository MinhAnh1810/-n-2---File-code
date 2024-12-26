import multiprocessing
import time
import subprocess
from datetime import datetime

# Hàm chạy file all_symbols.py
def run_all_symbols():
    while True:
        print("Running all_symbols.py...")
        subprocess.run(["python", "all_symbols.py"])
        time.sleep(30 * 24 * 60 * 60)  # Kiểm tra mỗi tháng (30 ngày)

# Hàm chạy file symbols_by_exchange.py
def run_symbols_by_exchange():
    while True:
        print("Running symbols_by_exchange.py...")
        subprocess.run(["python", "symbols_by_exchange.py"])
        time.sleep(30 * 24 * 60 * 60)  # Kiểm tra mỗi tháng (30 ngày)

# Hàm chạy file symbols_by_industrie.py
def run_symbols_by_industrie():
    while True:
        print("Running symbols_by_industrie.py...")
        subprocess.run(["python", "symbols_by_industrie.py"])
        time.sleep(30 * 24 * 60 * 60)  # Kiểm tra mỗi tháng (30 ngày)

# Hàm chạy file balance_sheet.py
def run_balance_sheet():
    while True:
        print("Running balance_sheet.py...")
        subprocess.run(["python", "balance_sheet.py"])
        time.sleep(30 * 24 * 60 * 60)  # Kiểm tra mỗi tháng (30 ngày)

# Hàm chạy file income_statement.py
def run_income_statement():
    while True:
        print("Running income_statement.py...")
        subprocess.run(["python", "income_statement.py"])
        time.sleep(30 * 24 * 60 * 60)  # Kiểm tra mỗi tháng (30 ngày)

# Hàm chạy file finance_ratio.py
def run_finance_ratio():
    while True:
        print("Running finance_ratio.py...")
        subprocess.run(["python", "finance_ratio.py"])
        time.sleep(30 * 24 * 60 * 60)  # Kiểm tra mỗi tháng (30 ngày)

# Hàm chạy file stock_data_history.py vào 9h sáng và 15h chiều
def run_stock_data_history():
    while True:
        now = datetime.now()
        if now.hour == 9 and now.minute == 0:  # Kiểm tra lúc 9h sáng
            print("Running stock_data_history.py at 9 AM...")
            subprocess.run(["python", "stock_data_history.py"])
            time.sleep(60)  # Đảm bảo không chạy lại trong cùng 1 phút
        elif now.hour == 15 and now.minute == 0:  # Kiểm tra lúc 15h chiều
            print("Running stock_data_history.py at 3 PM...")
            subprocess.run(["python", "stock_data_history.py"])
            time.sleep(60)  # Đảm bảo không chạy lại trong cùng 1 phút
        else:
            time.sleep(30)  # Kiểm tra lại sau 30 giây

# Hàm chạy file stock_quote_intraday.py mỗi 10 giây từ 9h sáng tới 15h chiều
def run_stock_quote_intraday():
    while True:
        now = datetime.now()
        if 9 <= now.hour < 15:  # Kiểm tra trong khoảng từ 9h sáng đến 15h chiều
            print("Running stock_quote_intraday.py...")
            subprocess.run(["python", "stock_quote_intraday.py"])
            time.sleep(10)  # Kiểm tra mỗi 10 giây
        else:
            time.sleep(60)  # Kiểm tra lại sau 1 phút nếu ngoài khoảng 9h-15h

# Hàm chạy file stock_quote_price_depth.py mỗi 10 giây từ 9h sáng tới 15h chiều
def run_stock_quote_price_depth():
    while True:
        now = datetime.now()
        if 9 <= now.hour < 15:  # Kiểm tra trong khoảng từ 9h sáng đến 15h chiều
            print("Running stock_quote_price_depth.py...")
            subprocess.run(["python", "stock_quote_price_depth.py"])
            time.sleep(10)  # Kiểm tra mỗi 10 giây
        else:
            time.sleep(60)  # Kiểm tra lại sau 1 phút nếu ngoài khoảng 9h-15h

if __name__ == "__main__":
    # Tạo các process riêng biệt cho từng file
    processes = [
        multiprocessing.Process(target=run_all_symbols),
        multiprocessing.Process(target=run_symbols_by_exchange),
        multiprocessing.Process(target=run_symbols_by_industrie),
        multiprocessing.Process(target=run_balance_sheet),
        multiprocessing.Process(target=run_income_statement),
        multiprocessing.Process(target=run_finance_ratio),
        multiprocessing.Process(target=run_stock_data_history),
        multiprocessing.Process(target=run_stock_quote_intraday),
        multiprocessing.Process(target=run_stock_quote_price_depth),
    ]

    # Khởi chạy các process
    for process in processes:
        process.start()

    # Đảm bảo các process không kết thúc
    for process in processes:
        process.join()
