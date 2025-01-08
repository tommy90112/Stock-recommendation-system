import psycopg2
import logging
from typing import Dict, List
from datetime import datetime

class StockDataMerger:
    def __init__(self):
        self.db_params = {
            'dbname': 'stock_recommendation_system',
            'user': 'test',
            'password': '123456',
            'host': 'localhost',
            'port': '5433'
        }

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        self.industries = [
            "金融",
            "營建",
            "航運",
            "半導體",
            "電子零組件",
            "ETF"
        ]
        self.table_mapping = {
            "金融": ("finance_value", "finance_prices"),
            "營建": ("construction_value", "construction_prices"),
            "航運": ("shipping_value", "shipping_prices"),
            "半導體": ("semiconductor_value", "semiconductor_prices"),
            "電子零組件": ("electronic_components_value", "electronic_component_prices"),
            "ETF": ("etf_value", "etf_prices")
        }

    def create_merged_table(self, conn) -> None:
        try:
            cursor = conn.cursor()

            cursor.execute("""
                DROP TABLE IF EXISTS stock_all_industry_merge;
                CREATE TABLE stock_all_industry_merge (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20),
                    stock_name VARCHAR(50),
                    industry_type VARCHAR(20),
                    date DATE,
                    avg_5_year_dividend_yield FLOAT,
                    close_price FLOAT,
                    fair_value_range VARCHAR(30),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.commit()
            logging.info("成功創建合併表 stock_all_industry_merge")
            
        except Exception as e:
            conn.rollback()
            logging.error(f"創建合併表時發生錯誤: {str(e)}")
            raise

    def merge_industry_data(self, conn, industry: str) -> None:
        try:
            cursor = conn.cursor()
            value_table, price_table = self.table_mapping[industry]
            
            insert_query = f"""
                INSERT INTO stock_all_industry_merge (
                    stock_code,
                    stock_name,
                    industry_type,
                    date,
                    avg_5_year_dividend_yield,
                    close_price,
                    fair_value_range
                )
                SELECT 
                    v.stock_code,
                    v.stock_name,
                    %s as industry_type,
                    p.date,
                    v.avg_5_year_dividend_yield,
                    p.close_price,
                    v.fair_price_range
                FROM {value_table} v
                JOIN {price_table} p ON v.stock_code = p.stock_code;
            """
            
            cursor.execute(insert_query, (industry,))
            conn.commit()
            
            cursor.execute("SELECT COUNT(*) FROM stock_all_industry_merge WHERE industry_type = %s", (industry,))
            count = cursor.fetchone()[0]
            logging.info(f"成功合併 {industry} 產業資料，插入 {count} 筆記錄")
            
        except Exception as e:
            conn.rollback()
            logging.error(f"合併 {industry} 產業資料時發生錯誤: {str(e)}")
            raise

    def create_indexes(self, conn) -> None:
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_all_merge_stock_code 
                ON stock_all_industry_merge(stock_code);
                
                CREATE INDEX IF NOT EXISTS idx_all_merge_industry_type 
                ON stock_all_industry_merge(industry_type);
                
                CREATE INDEX IF NOT EXISTS idx_all_merge_date 
                ON stock_all_industry_merge(date);
            """)
            
            conn.commit()
            logging.info("成功創建索引")
            
        except Exception as e:
            conn.rollback()
            logging.error(f"創建索引時發生錯誤: {str(e)}")
            raise

    def merge_all_data(self) -> None:
        conn = None
        try:
            # 建立資料庫連線
            conn = psycopg2.connect(**self.db_params)
            
            # 創建合併表
            self.create_merged_table(conn)
            
            # 合併每個產業的資料
            for industry in self.industries:
                self.merge_industry_data(conn, industry)
            
            # 創建索引
            self.create_indexes(conn)
            
            logging.info("所有資料合併完成")
            
        except Exception as e:
            logging.error(f"資料合併過程中發生錯誤: {str(e)}")
            raise
            
        finally:
            if conn:
                conn.close()

def main():
    try:
        merger = StockDataMerger()
        merger.merge_all_data()
        print("資料合併完成！")
        
    except Exception as e:
        print(f"錯誤: {str(e)}")
        raise

if __name__ == "__main__":
    main()