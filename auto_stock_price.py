import pandas as pd
import psycopg2
from datetime import datetime
import logging
from pathlib import Path
import os

class ExcelSheetImporter:
    def __init__(self):
        log_dir = Path.home() / "stock_logs"
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / "stock_import.log"
        
        logging.basicConfig(
            filename=str(log_file),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        self.db_params = {
            'dbname': 'stock_recommendation_system',
            'user': 'test',
            'password': '123456',
            'host': 'localhost',
            'port': '5433'
        }
        
        self.sheet_table_mapping = {
            '金融': 'finance_prices',
            '營建': 'construction_prices',
            '航運': 'shipping_prices',
            '半導體': 'semiconductor_prices',
            '電子零組件': 'electronic_component_prices',
            'ETF': 'etf_prices'
        }

    def create_tables(self, conn):
        cur = conn.cursor()
        
        for table_name in self.sheet_table_mapping.values():
            cur.execute(f"""
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20),
                    date DATE,
                    close_price FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX idx_{table_name}_stock_code 
                ON {table_name}(stock_code);
            """)
        
        conn.commit()
        logging.info("所有資料表建立完成")

    def process_value(self, value): 
        try:
            if isinstance(value, str):
                cleaned_value = value.replace('$', '').strip()
                return round(float(cleaned_value), 2)
            return round(float(value), 2)
        except ValueError as e:
            logging.error(f"價格轉換錯誤: {value}, 錯誤: {str(e)}")
            raise

    def read_sheet_data(self, excel_path, sheet_name):
        try:
            logging.info(f"開始讀取頁籤 {sheet_name}")
            
            # 讀取Excel檔案，獲取股票代碼行
            df_codes = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, nrows=1)
            # 讀取價格數據，跳過前兩行
            df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, skiprows=2)
            
            # 處理日期欄
            base_date = pd.Timestamp('1899-12-30')
            latest_date_idx = len(df) - 1
            days = df.iloc[latest_date_idx, 0]
            actual_date = base_date + pd.Timedelta(days=int(days))
            
            stock_data = []
            
            # 處理每支股票的代碼和對應價格
            for col in range(0, df.shape[1], 2):
                try:
                    stock_code = df_codes.iloc[0, col]
                    if not isinstance(stock_code, str) or not stock_code.startswith('XTAI:'):
                        continue
                    
                    price = df.iloc[latest_date_idx, col + 1]
                    
                    if pd.notna(price):
                        try:
                            price_value = self.process_value(price)
                            
                            if price_value <= 0:
                                logging.warning(f"股票 {stock_code} 價格異常: {price_value}")
                                continue
                                
                            stock_data.append({
                                'stock_code': stock_code,
                                'date': actual_date.date(),
                                'close_price': price_value
                            })
                            
                        except Exception as e:
                            logging.error(f"處理股票 {stock_code} 數據時發生錯誤: {str(e)}")
                            continue
                            
                except Exception as e:
                    logging.error(f"處理第 {col} 列數據時發生錯誤: {str(e)}")
                    continue
            
            logging.info(f"頁籤 {sheet_name} 共讀取到 {len(stock_data)} 支股票的數據")
            return stock_data
            
        except Exception as e:
            logging.error(f"讀取頁籤 {sheet_name} 時發生錯誤: {str(e)}")
            return []

    def import_all_sheets(self, excel_path):
        conn = None
        try:
            logging.info("開始匯入所有頁籤數據")
            conn = psycopg2.connect(**self.db_params)
            
            # 建立所有必要的表
            self.create_tables(conn)
            
            total_imported = 0
            
            # 處理每個頁籤
            for sheet_name, table_name in self.sheet_table_mapping.items():
                try:
                    stock_data = self.read_sheet_data(excel_path, sheet_name)
                    
                    if stock_data:
                        cur = conn.cursor()
                        imported_count = 0
                        
                        for data in stock_data:
                            cur.execute(f"""
                                INSERT INTO {table_name} (stock_code, date, close_price)
                                VALUES (%s, %s, %s)
                            """, (
                                data['stock_code'],
                                data['date'],
                                data['close_price']
                            ))
                            imported_count += 1
                        
                        conn.commit()
                        total_imported += imported_count
                        logging.info(f"成功匯入 {imported_count} 筆數據到表 {table_name}")
                        
                except Exception as e:
                    logging.error(f"處理頁籤 {sheet_name} 時發生錯誤: {str(e)}")
                    conn.rollback()
                    continue
            
            logging.info(f"所有頁籤匯入完成，共匯入 {total_imported} 筆數據")
            return total_imported
            
        except Exception as e:
            logging.error(f"匯入過程中發生錯誤: {str(e)}")
            if conn:
                conn.rollback()
            return 0
            
        finally:
            if conn:
                conn.close()

def main():
    try:
        excel_path = "/Users/tommy/Desktop/資料庫/資料庫20大股池資料.xlsx"
        if not os.path.exists(excel_path):
            print(f"錯誤: 找不到Excel檔案: {excel_path}")
            return
            
        importer = ExcelSheetImporter()
        total_imported = importer.import_all_sheets(excel_path)
        print(f"成功匯入 {total_imported} 筆數據")
        
    except Exception as e:
        logging.error(f"程式執行時發生錯誤: {str(e)}")
        print(f"錯誤: {str(e)}")
        raise

if __name__ == "__main__":
    main()