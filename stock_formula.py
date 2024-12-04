import numpy as np
from enum import Enum

class IndustryType(Enum):
    FINANCIAL = "金融"
    CONSTRUCTION = "營建"
    BIOTECH = "生技"
    SHIPPING = "航運"
    SEMICONDUCTOR = "半導體"
    ELECTRONIC = "電子零組件"
    ETF = "ETF"

def calculate_industry_fair_value(
    industry_type: IndustryType,
    current_price: float,
    # 基本面數據
    eps: float = None,
    bps: float = None,
    free_cash_flow: float = None,
    nav: float = None,
    # 產業參數
    industry_pe: float = None,
    industry_pb: float = None,
    # DCF相關參數
    growth_rate: float = None,
    discount_rate: float = 0.1,
    terminal_growth: float = 0.02,
    forecast_years: int = 5
):
    """
    根據不同產業特性計算股價合理區間
    
    Parameters:
    industry_type: IndustryType - 產業類型
    current_price: float - 目前股價
    eps: float - 每股盈餘 (適用於P/E估值)
    bps: float - 每股淨值 (適用於P/B估值)
    free_cash_flow: float - 每股自由現金流量 (適用於DCF估值)
    nav: float - 淨值 (適用於ETF估值)
    industry_pe: float - 產業平均本益比
    industry_pb: float - 產業平均股價淨值比
    growth_rate: float - 預期年成長率(%)
    discount_rate: float - 折現率(%)
    terminal_growth: float - 永續成長率(%)
    forecast_years: int - 預測年數
    
    Returns:
    dict: 包含評估方法、合理價格區間和相關指標
    """
    results = {
        'industry': industry_type.value,
        'current_price': current_price,
        'valuation_method': None,
        'fair_value': None,
        'metrics': {}
    }
    
    # 金融業和營建業使用P/B估值
    if industry_type in [IndustryType.FINANCIAL, IndustryType.CONSTRUCTION]:
        if bps is None or industry_pb is None:
            raise ValueError(f"{industry_type.value}業需要提供每股淨值(BPS)和產業平均P/B")
        
        # 根據產業特性調整P/B倍數
        if industry_type == IndustryType.FINANCIAL:
            pb_low = industry_pb * 0.8
            pb_high = industry_pb * 1.2
        else:  # 營建業
            pb_low = industry_pb * 0.7  # 較保守
            pb_high = industry_pb * 1.1
        
        results.update({
            'valuation_method': 'P/B估值法',
            'fair_value': {
                'low': round(bps * pb_low, 2),
                'high': round(bps * pb_high, 2)
            },
            'metrics': {
                'current_pb': round(current_price / bps, 2),
                'industry_pb': industry_pb,
                'bps': bps
            }
        })
    
    # 生技業使用DCF估值
    elif industry_type == IndustryType.BIOTECH:
        if free_cash_flow is None or growth_rate is None:
            raise ValueError("生技業需要提供自由現金流量和預期成長率")
        
        # DCF計算
        future_cash_flows = []
        fcf = free_cash_flow
        
        # 計算預測期間現金流量
        for year in range(1, forecast_years + 1):
            fcf *= (1 + growth_rate/100)
            future_cash_flows.append(fcf)
        
        # 計算終值
        terminal_value = future_cash_flows[-1] * (1 + terminal_growth) / (discount_rate - terminal_growth)
        
        # 現值計算
        present_values = [
            cf / ((1 + discount_rate) ** i) 
            for i, cf in enumerate(future_cash_flows, 1)
        ]
        terminal_present_value = terminal_value / ((1 + discount_rate) ** forecast_years)
        
        # 計算合理價格區間
        base_value = sum(present_values) + terminal_present_value
        
        results.update({
            'valuation_method': 'DCF估值法',
            'fair_value': {
                'low': round(base_value * 0.8, 2),  # 考慮風險給予折價
                'high': round(base_value * 1.2, 2)
            },
            'metrics': {
                'growth_rate': growth_rate,
                'discount_rate': discount_rate,
                'terminal_growth': terminal_growth
            }
        })
    
    # 航運、半導體、電子零組件使用P/E估值
    elif industry_type in [IndustryType.SHIPPING, IndustryType.SEMICONDUCTOR, IndustryType.ELECTRONIC]:
        if eps is None or industry_pe is None:
            raise ValueError(f"{industry_type.value}需要提供EPS和產業平均P/E")
        
        # 根據產業特性調整P/E倍數
        if industry_type == IndustryType.SHIPPING:
            # 航運業較波動，使用較保守的倍數
            pe_low = industry_pe * 0.6
            pe_high = industry_pe * 0.9
        else:  # 科技業（半導體和電子零組件）
            pe_low = industry_pe * 0.8
            pe_high = industry_pe * 1.2
        
        results.update({
            'valuation_method': 'P/E估值法',
            'fair_value': {
                'low': round(eps * pe_low, 2),
                'high': round(eps * pe_high, 2)
            },
            'metrics': {
                'current_pe': round(current_price / eps, 2),
                'industry_pe': industry_pe,
                'eps': eps
            }
        })
    
    # ETF使用NAV估值
    elif industry_type == IndustryType.ETF:
        if nav is None:
            raise ValueError("ETF需要提供淨值(NAV)")
        
        # 計算溢價/折價率
        premium = (current_price - nav) / nav * 100
        
        results.update({
            'valuation_method': 'NAV估值法',
            'fair_value': {
                'low': round(nav * 0.98, 2),  # 允許2%折價
                'high': round(nav * 1.02, 2)  # 允許2%溢價
            },
            'metrics': {
                'nav': nav,
                'premium_discount': round(premium, 2)
            }
        })
    
    return results

# 輔助函數：印出評估結果
def print_valuation_result(result):
    """
    格式化打印估值結果
    """
    print(f"\n{result['industry']}股價評估結果")
    print("="*40)
    print(f"評估方法：{result['valuation_method']}")
    print(f"目前價格：{result['current_price']}")
    print(f"合理價格區間：{result['fair_value']['low']} ~ {result['fair_value']['high']}")
    
    print("\n關鍵指標：")
    for metric, value in result['metrics'].items():
        print(f"{metric}: {value}")
