import pandas as pd
import psycopg2
from typing import Dict, List
from enum import Enum

class IndustryType(Enum):
    FINANCIAL = "金融"
    CONSTRUCTION = "營建"
    SHIPPING = "航運"
    SEMICONDUCTOR = "半導體"
    ELECTRONIC = "電子零組件"
    ETF = "ETF"

def create_tables(conn):
    """建立所有必要的資料表並新增欄位"""
    
    def add_fair_price_range_column(cursor, table_name):
        """檢查並添加合理價格區間欄位"""
        try:
            cursor.execute(f"""
            ALTER TABLE {table_name}
            ADD COLUMN IF NOT EXISTS fair_price_range VARCHAR(30)
            """)
        except Exception as e:
            print(f"添加欄位到 {table_name} 時發生錯誤: {str(e)}")
            raise
    cursor = conn.cursor()
    
    # 金融業和營建業的表結構
    for table_name in ["finance_value", "construction_value"]:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            stock_code VARCHAR(20) UNIQUE NOT NULL,
            stock_name VARCHAR(50) NOT NULL,
            avg_5_year_dividend_yield FLOAT,
            net_value_per_share FLOAT,
            book_to_net_value_ratio FLOAT,
            fair_price_range VARCHAR(30),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    
    # 航運業、半導體和電子零組件的表結構
    for table_name in ["shipping_value", "semiconductor_value", "electronic_components_value"]:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            stock_code VARCHAR(20) UNIQUE NOT NULL,
            stock_name VARCHAR(50) NOT NULL,
            avg_5_year_dividend_yield FLOAT,
            earnings_per_share FLOAT,
            price_to_earnings_ratio FLOAT,
            fair_price_range VARCHAR(30),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    
    # ETF的表結構
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etf_value (
        id SERIAL PRIMARY KEY,
        stock_code VARCHAR(20) UNIQUE NOT NULL,
        stock_name VARCHAR(50) NOT NULL,
        avg_5_year_dividend_yield FLOAT,
        net_asset_value_per_etf FLOAT,
        fair_price_range VARCHAR(30),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 為所有資料表添加合理價格區間欄位
    for table_name in ["finance_value", "construction_value", "shipping_value", 
                      "semiconductor_value", "electronic_components_value", "etf_value"]:
        add_fair_price_range_column(cursor, table_name)
    
    conn.commit()
    print("所有資料表建立完成並確保欄位存在")

def read_excel_data(file_path: str, sheet_name: str) -> List[Dict]:
    """從Excel檔案讀取特定頁籤的股票數據"""
    try:
        # 讀取Excel檔案的特定頁籤
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # 根據不同頁籤設定不同的欄位映射
        column_mappings = {
            '金融': {
                '股票代碼': 'stock_code',
                '股票名稱': 'stock_name',
                '近5年平均殖利率': 'avg_5_year_dividend_yield',
                '每股淨值': 'net_value_per_share',
                '股淨比': 'book_to_net_value_ratio'
            },
            '營建': {
                '股票代碼': 'stock_code',
                '股票名稱': 'stock_name',
                '近5年平均殖利率': 'avg_5_year_dividend_yield',
                '每股淨值': 'net_value_per_share',
                '股淨比': 'book_to_net_value_ratio'
            },
            '航運': {
                '股票代碼': 'stock_code',
                '股票名稱': 'stock_name',
                '近5年平均殖利率': 'avg_5_year_dividend_yield',
                '每股盈餘': 'earnings_per_share',
                '本益比': 'price_to_earnings_ratio'
            },
            '半導體': {
                '股票代碼': 'stock_code',
                '股票名稱': 'stock_name',
                '近5年平均殖利率': 'avg_5_year_dividend_yield',
                '每股盈餘': 'earnings_per_share',
                '本益比': 'price_to_earnings_ratio'
            },
            '電子零組件': {
                '股票代碼': 'stock_code',
                '股票名稱': 'stock_name',
                '近5年平均殖利率': 'avg_5_year_dividend_yield',
                '每股盈餘': 'earnings_per_share',
                '本益比': 'price_to_earnings_ratio'
            },
            'ETF': {
                '股票代碼': 'stock_code',
                '股票名稱': 'stock_name',
                '近5年平均殖利率': 'avg_5_year_dividend_yield',
                'ETF淨值': 'net_asset_value_per_etf'
            }
        }
        
        # 重新命名欄位
        if sheet_name in column_mappings:
            df = df.rename(columns=column_mappings[sheet_name])
        else:
            raise ValueError(f"不支援的頁籤名稱: {sheet_name}")
        
        # 對殖利率進行四捨五入處理
        if 'avg_5_year_dividend_yield' in df.columns:
            df['avg_5_year_dividend_yield'] = df['avg_5_year_dividend_yield'].round(2)
            
        # 轉換為字典列表
        stocks_data = df.to_dict('records')
        return stocks_data
        
    except Exception as e:
        print(f"讀取Excel檔案錯誤: {str(e)}")
        return []

def save_to_database(stocks_data: List[Dict], industry_type: str):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="stock_recommendation_system",
            user="test",
            password="123456",
            host="localhost",
            port="5433"
        )
        cursor = conn.cursor()

        # 產業對應的資料表名稱映射
        table_mapping = {
            "金融": "finance_value",
            "營建": "construction_value",
            "航運": "shipping_value",
            "半導體": "semiconductor_value",
            "電子零組件": "electronic_components_value",
            "ETF": "etf_value"
        }
        
        table_name = table_mapping.get(industry_type)
        if not table_name:
            raise ValueError(f"未支援的產業類型: {industry_type}")

        # 批次插入或更新數據
        for stock in stocks_data:
            # 計算合理價格區間
            fair_price_range = None
            
            if industry_type in ['金融', '營建']:
                # 使用個股的淨值和本淨比計算
                bps = stock['net_value_per_share']
                current_pb = stock['book_to_net_value_ratio']
                
                if industry_type == '金融':
                    pb_low = current_pb * 0.8
                    pb_high = current_pb * 1.2
                else:  # 營建
                    pb_low = current_pb * 0.7
                    pb_high = current_pb * 1.1
                    
                low_price = round(bps * pb_low, 2)
                high_price = round(bps * pb_high, 2)
                
            elif industry_type in ['航運', '半導體', '電子零組件']:
                # 使用個股的EPS和本益比計算
                eps = stock['earnings_per_share']
                current_pe = stock['price_to_earnings_ratio']
                
                # 確保 EPS 和 PE 都是有效值
                if eps and current_pe and eps != 0 and current_pe > 0:
                    if industry_type == '航運':
                        pe_low = current_pe * 0.6
                        pe_high = current_pe * 0.9
                    else:  # 半導體和電子零組件
                        pe_low = current_pe * 0.8
                        pe_high = current_pe * 1.2
                        
                    low_price = round(eps * pe_low, 2)
                    high_price = round(eps * pe_high, 2)
                else:
                    # 如果數據無效，使用目前股價作為基準
                    current_price = eps * current_pe if eps and current_pe else 0
                    if current_price > 0:
                        low_price = round(current_price * 0.8, 2)
                        high_price = round(current_price * 1.2, 2)
                    else:
                        low_price = high_price = 0
                
            else:  # ETF
                nav = stock['net_asset_value_per_etf']
                if nav and nav > 0:
                    low_price = round(nav * 0.98, 2)
                    high_price = round(nav * 1.02, 2)
                else:
                    low_price = high_price = 0

            # 設定合理價格區間字串
            if low_price > 0 and high_price > 0:
                fair_price_range = f"{low_price} ~ {high_price}"
            else:
                fair_price_range = "無法計算"

            # UPSERT 語句（根據不同產業類型）
            if industry_type in ['金融', '營建']:
                upsert_sql = f"""
                INSERT INTO {table_name} (
                    stock_code, 
                    stock_name, 
                    avg_5_year_dividend_yield,
                    net_value_per_share,
                    book_to_net_value_ratio,
                    fair_price_range
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code) 
                DO UPDATE SET 
                    stock_name = EXCLUDED.stock_name,
                    avg_5_year_dividend_yield = EXCLUDED.avg_5_year_dividend_yield,
                    net_value_per_share = EXCLUDED.net_value_per_share,
                    book_to_net_value_ratio = EXCLUDED.book_to_net_value_ratio,
                    fair_price_range = EXCLUDED.fair_price_range
                """
                values = (
                    stock['stock_code'],
                    stock['stock_name'],
                    stock['avg_5_year_dividend_yield'],
                    stock['net_value_per_share'],
                    stock['book_to_net_value_ratio'],
                    fair_price_range
                )
            elif industry_type in ['航運', '半導體', '電子零組件']:
                upsert_sql = f"""
                INSERT INTO {table_name} (
                    stock_code,
                    stock_name,
                    avg_5_year_dividend_yield,
                    earnings_per_share,
                    price_to_earnings_ratio,
                    fair_price_range
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (stock_code) 
                DO UPDATE SET 
                    stock_name = EXCLUDED.stock_name,
                    avg_5_year_dividend_yield = EXCLUDED.avg_5_year_dividend_yield,
                    earnings_per_share = EXCLUDED.earnings_per_share,
                    price_to_earnings_ratio = EXCLUDED.price_to_earnings_ratio,
                    fair_price_range = EXCLUDED.fair_price_range
                """
                values = (
                    stock['stock_code'],
                    stock['stock_name'],
                    stock['avg_5_year_dividend_yield'],
                    stock['earnings_per_share'],
                    stock['price_to_earnings_ratio'],
                    fair_price_range
                )
            else:  # ETF
                upsert_sql = f"""
                INSERT INTO {table_name} (
                    stock_code,
                    stock_name,
                    avg_5_year_dividend_yield,
                    net_asset_value_per_etf,
                    fair_price_range
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (stock_code) 
                DO UPDATE SET 
                    stock_name = EXCLUDED.stock_name,
                    avg_5_year_dividend_yield = EXCLUDED.avg_5_year_dividend_yield,
                    net_asset_value_per_etf = EXCLUDED.net_asset_value_per_etf,
                    fair_price_range = EXCLUDED.fair_price_range
                """
                values = (
                    stock['stock_code'],
                    stock['stock_name'],
                    stock['avg_5_year_dividend_yield'],
                    stock['net_asset_value_per_etf'],
                    fair_price_range
                )
            
            cursor.execute(upsert_sql, values)
        
        conn.commit()
        print(f"成功儲存 {industry_type} 產業的數據")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"保存到資料庫時發生錯誤: {str(e)}")
        
    finally:
        if conn:
            conn.close()

def main():
    excel_file = "/Users/tommy/Desktop/資料庫/資料庫.xlsx"
    
    for industry in IndustryType:
        print(f"\n處理 {industry.value} 產業數據...")
        
        # 讀取Excel特定頁籤的數據
        stocks_data = read_excel_data(excel_file, industry.value)
        if not stocks_data:
            continue
            
        # 保存到資料庫
        save_to_database(stocks_data, industry.value)
        
        print(f"已完成 {industry.value} 產業的資料處理")

if __name__ == "__main__":
    main()