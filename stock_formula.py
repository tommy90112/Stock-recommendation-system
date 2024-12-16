import psycopg2
import pandas as pd
from typing import Dict, List
from enum import Enum

class IndustryType(Enum):
    FINANCIAL = "金融"
    CONSTRUCTION = "營建"
    SHIPPING = "航運"
    SEMICONDUCTOR = "半導體"
    ELECTRONIC = "電子零組件"
    ETF = "ETF"

def get_industry_data(industry_type: str) -> List[Dict]:
    """從PostgreSQL數據庫獲取特定產業的股票數據"""
    try:
        conn = psycopg2.connect(
            dbname="stockpool",
            user="test",
            password="123456",
            host="localhost",
            port="5433"
        )
        
        table_map = {
            IndustryType.FINANCIAL.value: "finance",
            IndustryType.CONSTRUCTION.value: "construction",
            IndustryType.SHIPPING.value: "shipping",
            IndustryType.SEMICONDUCTOR.value: "semiconductor",
            IndustryType.ELECTRONIC.value: "electronic_components",
            IndustryType.ETF.value: "etf"
        }
        
        table_name = table_map.get(industry_type)
        if not table_name:
            raise ValueError(f"不支援的產業類型: {industry_type}")
            
        # 根據不同產業類型選擇不同的查詢
        if industry_type == IndustryType.ETF.value:
            query = f"""
            SELECT 
                stock_code,
                stock_name,
                avg_5_year_dividend_yield,
                net_asset_value_per_etf as nav,
                net_asset_value_per_etf as current_price
            FROM public.{table_name}
            """
        elif industry_type in [IndustryType.SEMICONDUCTOR.value, IndustryType.ELECTRONIC.value, IndustryType.SHIPPING.value]:
            query = f"""
            SELECT 
                stock_code,
                stock_name,
                earnings_per_share as eps,
                price_to_earnings_ratio as pe,
                price_to_earnings_ratio * earnings_per_share as current_price,
                avg_5_year_dividend_yield
            FROM public.{table_name}
            """
        else:  # 金融業和營建業
            query = f"""
            SELECT 
                stock_code,
                stock_name,
                net_value_per_share as bps,
                book_to_net_value_ratio as pb,
                book_to_net_value_ratio * net_value_per_share as current_price
            FROM public.{table_name}
            """
        
        # 使用pandas讀取數據
        df = pd.read_sql_query(query, conn)
        stocks_data = df.to_dict('records')
        
        # 添加產業平均值
        for stock in stocks_data:
            if industry_type in [IndustryType.FINANCIAL.value, IndustryType.CONSTRUCTION.value]:
                stock['industry_pb'] = 1.5
            elif industry_type in [IndustryType.SEMICONDUCTOR.value, IndustryType.ELECTRONIC.value, IndustryType.SHIPPING.value]:
                stock['industry_pe'] = 20
        
        return stocks_data
        
    except Exception as e:
        print(f"數據庫錯誤: {str(e)}")
        return []
        
    finally:
        if 'conn' in locals():
            conn.close()

def evaluate_stock_price(current_price: float, low_price: float, high_price: float) -> Dict:
    """
    評估股票價格狀態
    
    Args:
        current_price: 目前股價
        low_price: 合理價區間下限
        high_price: 合理價區間上限
    
    Returns:
        Dict: 包含評估結果和詳細資訊的字典
    """
    # 計算合理價區間的平均價
    avg_price = (low_price + high_price) / 2
    
    # 判斷股價所在區間
    if current_price < low_price:
        evaluation = "加碼"
        price_zone = "低於合理價下限"
    elif current_price < avg_price:
        evaluation = "便宜"
        price_zone = "位於合理價下半區間"
    elif current_price <= high_price:
        evaluation = "合理"
        price_zone = "位於合理價上半區間"
    else:
        evaluation = "昂貴"
        price_zone = "高於合理價上限"
    
    return {
        'evaluation': evaluation,
        'price_zone': price_zone,
        'avg_price': round(avg_price, 2)
    }

def calculate_industry_fair_value(industry_type: IndustryType, current_price: float, eps: float = None, bps: float = None, nav: float = None, industry_pe: float = None, industry_pb: float = None) -> Dict:
    """計算特定產業股票的合理價格"""
    fair_value = {}
    valuation_method = ""
    metrics = {}

    # 航運業使用P/E估值
    if industry_type == IndustryType.SHIPPING:
        if eps is not None and industry_pe is not None:
            pe_low = industry_pe * 0.6  # 航運業週期性強，使用較低本益比
            pe_high = industry_pe * 0.9
            fair_value = {
                'low': round(eps * pe_low, 2),
                'high': round(eps * pe_high, 2)
            }
            valuation_method = "P/E估值法"
            metrics = {'EPS': eps, 'Industry PE': industry_pe}
    
    # 半導體和電子零組件使用P/E估值
    elif industry_type in [IndustryType.SEMICONDUCTOR, IndustryType.ELECTRONIC]:
        if eps is not None and industry_pe is not None:
            pe_low = industry_pe * 0.8
            pe_high = industry_pe * 1.2
            fair_value = {
                'low': round(eps * pe_low, 2),
                'high': round(eps * pe_high, 2)
            }
            valuation_method = "P/E估值法"
            metrics = {'EPS': eps, 'Industry PE': industry_pe}
    
    # 金融業使用P/B估值
    elif industry_type == IndustryType.FINANCIAL:
        if bps is not None and industry_pb is not None:
            pb_low = industry_pb * 0.8
            pb_high = industry_pb * 1.2
            fair_value = {
                'low': round(bps * pb_low, 2),
                'high': round(bps * pb_high, 2)
            }
            valuation_method = "P/B估值法"
            metrics = {'BPS': bps, 'Industry PB': industry_pb}
    
    # 營建業使用P/B估值
    elif industry_type == IndustryType.CONSTRUCTION:
        if bps is not None and industry_pb is not None:
            pb_low = industry_pb * 0.7
            pb_high = industry_pb * 1.1
            fair_value = {
                'low': round(bps * pb_low, 2),
                'high': round(bps * pb_high, 2)
            }
            valuation_method = "P/B估值法"
            metrics = {'BPS': bps, 'Industry PB': industry_pb}
    
    # ETF使用NAV估值
    elif industry_type == IndustryType.ETF:
        if nav is not None:
            fair_value = {
                'low': round(nav * 0.98, 2),
                'high': round(nav * 1.02, 2)
            }
            premium = (current_price - nav) / nav * 100
            valuation_method = "NAV估值法"
            metrics = {'NAV': nav, 'Premium/Discount': f"{round(premium, 2)}%"}

    # 評估股價狀態
    if fair_value:
        evaluation_result = evaluate_stock_price(
            current_price,
            fair_value['low'],
            fair_value['high']
        )
    else:
        evaluation_result = {
            'evaluation': "無法評估",
            'price_zone': "無法判斷",
            'avg_price': None
        }

    return {
        'fair_value': fair_value,
        'valuation_method': valuation_method,
        'metrics': metrics,
        'industry': industry_type.value,
        'current_price': current_price,
        'evaluation': evaluation_result['evaluation'],
        'price_zone': evaluation_result['price_zone'],
        'avg_price': evaluation_result['avg_price']
    }

def batch_calculate_fair_value(industry_type: IndustryType):
    """批次計算特定產業所有股票的合理價格"""
    stocks = get_industry_data(industry_type.value)
    
    results = []
    for stock in stocks:
        try:
            params = {
                'industry_type': industry_type,
                'current_price': stock['current_price'],
                'eps': stock.get('eps'),
                'bps': stock.get('bps'),
                'nav': stock.get('nav'),
                'industry_pe': stock.get('industry_pe'),
                'industry_pb': stock.get('industry_pb')
            }
            
            result = calculate_industry_fair_value(**params)
            result.update({
                'stock_id': stock['stock_code'],
                'stock_name': stock['stock_name']
            })
            
            results.append(result)
            
        except Exception as e:
            print(f"計算錯誤 - 股票代碼 {stock['stock_code']}: {str(e)}")
            continue
    
    return results

def print_batch_results(results: List[Dict]):
    """格式化打印批次計算結果"""
    if not results:
        print("沒有找到符合條件的股票")
        return
        
    print(f"\n{results[0]['industry']}產業股價評估結果")
    print("=" * 80)
    
    for result in results:
        print(f"\n股票: {result['stock_id']} {result['stock_name']}")
        print(f"評估方法: {result['valuation_method']}")
        print(f"目前價格: {result['current_price']}")
        if result['fair_value']:
            print(f"合理價格區間: {result['fair_value']['low']} ~ {result['fair_value']['high']}")
            print(f"合理價平均值: {result['avg_price']}")
        print("關鍵指標:")
        for metric, value in result['metrics'].items():
            print(f"  {metric}: {value}")
        print(f"評估結果: {result['evaluation']} ")
        print("-" * 40)

#加入新的儲存功能
def save_evaluation_results(results: List[Dict], industry_type: str):
    """
    將評估結果儲存到對應的資料庫表中
    
    Args:
        results: 評估結果列表
        industry_type: 產業類型
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="stockresult",  # 修改為 stockresult
            user="test",
            password="123456",
            host="localhost",
            port="5433"
        )
        cursor = conn.cursor()
        
        # 產業對應的評估結果表
        table_map = {
            "營建": "construction_evaluation",
            "電子零組件": "electronic_components_evaluation",
            "ETF": "etf_evaluation",
            "金融": "finance_evaluation",
            "半導體": "semiconductor_evaluation",
            "航運": "shipping_evaluation"
        }
        
        table_name = table_map.get(industry_type)
        if not table_name:
            raise ValueError(f"未支援的產業類型: {industry_type}")
        
        # 先清空對應表的舊數據
        cursor.execute(f"DELETE FROM {table_name}")
        
        # 準備 INSERT 語句
        insert_sql = f"""
        INSERT INTO {table_name} (stock_code, stock_name, fair_value_range, evaluation)
        VALUES (%s, %s, %s, %s)
        """
        
        # 批次插入新數據
        for result in results:
            if result['fair_value']:  # 確保有合理價格區間
                fair_value_range = f"{result['fair_value']['low']} ~ {result['fair_value']['high']}"
                
                cursor.execute(
                    insert_sql,
                    (
                        result['stock_id'],
                        result['stock_name'],
                        fair_value_range,
                        result['evaluation']
                    )
                )
        
        # 提交事務
        conn.commit()
        print(f"成功儲存 {industry_type} 產業的評估結果")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"儲存評估結果時發生錯誤: {str(e)}")
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # 計算並儲存所有產業的股票評估結果
    for industry in IndustryType:
        print(f"\n開始計算{industry.value}產業股票價值...")
        # 計算評估結果
        results = batch_calculate_fair_value(industry)
        
        # 顯示結果
        print_batch_results(results)
        
        # 儲存結果到資料庫
        save_evaluation_results(results, industry.value)