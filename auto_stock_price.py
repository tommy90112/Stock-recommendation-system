import pandas as pd
import psycopg2
from datetime import datetime
import logging
import schedule
import time
from pathlib import Path
import os

class StockPriceScheduler:
    def __init__(self, excel_path):
        # 設定日誌
        log_dir = Path.home() / "stock_logs"
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / "stock_update.log"
        
        logging.basicConfig(
            filename=str(log_file),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(console_handler)
        
        self.excel_path = excel_path
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"找不到 Excel 檔案: {excel_path}")
            
        self.db_params = {
            'dbname': 'stockprice',
            'user': 'test',
            'password': '123456',
            'host': 'localhost',
            'port': '5433'
        }

    def process_value(self, value): 
        try:
            if isinstance(value, str):
                cleaned_value = value.replace('$', '').strip()
                return round(float(cleaned_value), 2)
            return round(float(value), 2)
        except ValueError as e:
            logging.error(f"價格轉換錯誤: {value}, 錯誤: {str(e)}")
            raise

    def read_latest_prices(self):
        try:
            logging.info(f"開始讀取Excel檔案: {self.excel_path}")
            
            # 讀取 Excel 文件
            df = pd.read_excel(self.excel_path, header=None, skiprows=2)
            df_codes = pd.read_excel(self.excel_path, header=None, nrows=1)
            logging.info(f"Excel檔案大小: {df.shape}")
            
            # 處理日期列
            # 將數值型日期轉換為 datetime
            base_date = pd.Timestamp('1899-12-30')
            latest_date_idx = len(df) - 1
            days = df.iloc[latest_date_idx, 0]
            actual_date = base_date + pd.Timedelta(days=int(days))
            
            logging.info(f"使用Excel中的最新日期: {actual_date.strftime('%Y-%m-%d')}")
            
            latest_prices = []
            
            # 處理每支股票的代碼和對應價格
            for col in range(0, df.shape[1], 2):
                try:
                    stock_code = df_codes.iloc[0, col]
                    if not isinstance(stock_code, str) or not stock_code.startswith('XTAI:'):
                        continue
                    
                    # 取得對應日期的價格
                    price = df.iloc[latest_date_idx, col + 1]
                    
                    if pd.notna(price):
                        try:
                            price_value = self.process_value(price)
                            
                            if price_value <= 0:
                                logging.warning(f"股票 {stock_code} 價格異常: {price_value}")
                                continue
                            
                            stock_data = {
                                'stock_code': stock_code,
                                'date': actual_date.date(),
                                'close_price': price_value
                            }
                            latest_prices.append(stock_data)
                            logging.info(f"成功讀取: {stock_code}, 日期={actual_date.strftime('%Y-%m-%d')}, 價格={price_value}")
                            
                        except Exception as e:
                            logging.error(f"處理股票 {stock_code} 價格時發生錯誤: {str(e)}")
                            continue
                    
                except Exception as e:
                    logging.error(f"處理第 {col} 欄資料時發生錯誤: {str(e)}")
                    continue
            
            logging.info(f"總共讀取到 {len(latest_prices)} 支股票的資料")
            return latest_prices
            
        except Exception as e:
            logging.error(f"讀取Excel檔案失敗: {str(e)}")
            return None

    def update_stock_prices(self):
        """更新股票價格到資料庫"""
        logging.info("開始執行股價更新")
        conn = None
        cur = None
        
        try:
            logging.info("嘗試連接資料庫...")
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            logging.info("資料庫連接成功")
            
            cur.execute("DROP TABLE IF EXISTS stock_prices")
            conn.commit()
            
            cur.execute("""
                CREATE TABLE stock_prices (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) UNIQUE,
                    date DATE,
                    close_price FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            logging.info("資料表建立成功")
            
            latest_prices = self.read_latest_prices()
            if not latest_prices:
                logging.error("無法取得股價資料")
                return
            
            updated_count = 0
            for price_data in latest_prices:
                cur.execute("""
                    INSERT INTO stock_prices (stock_code, date, close_price)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (stock_code) 
                    DO UPDATE SET 
                        date = EXCLUDED.date,
                        close_price = EXCLUDED.close_price,
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    price_data['stock_code'],
                    price_data['date'],
                    price_data['close_price']
                ))
                updated_count += 1
            
            conn.commit()
            logging.info(f"成功更新 {updated_count} 支股票的價格")
            print(f"成功更新 {updated_count} 支股票的價格")
            
        except Exception as e:
            logging.error(f"更新資料時發生錯誤: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def run_scheduler(self):
        """執行排程"""
        try:
            print("執行初始更新...")
            self.update_stock_prices()
            
            schedule.every().day.at("20:00").do(self.update_stock_prices)
            print("排程器已啟動，將於每天晚上8點執行更新")
            print("程式持續運行中... (按 Ctrl+C 可終止程式)")
            
            while True:
                schedule.run_pending()
                time.sleep(60)
                
        except KeyboardInterrupt:
            print("\n程式已停止")
            logging.info("程式被使用者終止")
        except Exception as e:
            logging.error(f"排程執行時發生錯誤: {str(e)}")
            raise

def main():
    try:
        excel_path = "/Users/tommy/Desktop/資料庫/資料庫20大股池資料.xlsx"
        if not os.path.exists(excel_path):
            print(f"錯誤: 找不到 Excel 檔案: {excel_path}")
            return
            
        scheduler = StockPriceScheduler(excel_path)
        scheduler.run_scheduler()
    except Exception as e:
        logging.error(f"程式執行時發生錯誤: {str(e)}")
        print(f"錯誤: {str(e)}")
        raise

if __name__ == "__main__":
    main()