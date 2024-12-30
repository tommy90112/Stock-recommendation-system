from flask import Flask, jsonify, render_template_string
import psycopg2
from typing import Dict, List
import logging
from datetime import datetime

# 創建 Flask 應用
app = Flask(__name__)

class StockEvaluationSystem:
    def __init__(self):
        # 設定資料庫連線參數
        self.db_params = {
            'dbname': 'stock_recommendation_system',
            'user': 'test',
            'password': '123456',
            'host': 'localhost',
            'port': '5433'
        }
        
        # 設定日誌
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_stock_evaluations(self) -> List[Dict]:
        """從 stock_all_industry_merge 表獲取並評估所有股票"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    stock_code,
                    stock_name,
                    industry_type,
                    date,
                    close_price,
                    fair_value_range,
                    avg_5_year_dividend_yield
                FROM stock_all_industry_merge 
                ORDER BY industry_type, stock_code;
            """
            
            cursor.execute(query)
            stocks = cursor.fetchall()
            
            results = []
            for stock in stocks:
                stock_code, stock_name, industry, date, price, fair_range, dividend_yield = stock
                
                # 計算評價
                evaluation = self.evaluate_stock(price, fair_range)
                
                results.append({
                    "stock_code": stock_code.replace('XTAI:', ''),
                    "stock_name": stock_name,
                    "industry_type": industry,
                    "close_price": round(price, 2),
                    "fair_value_range": fair_range,
                    "rating": evaluation,
                    "avg_5_year_dividend_yield": round(dividend_yield, 2) if dividend_yield else 0,
                    "date": date.strftime("%Y-%m-%d")
                })
            
            return results
            
        except Exception as e:
            logging.error(f"獲取股票資料時發生錯誤: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def evaluate_stock(self, current_price: float, fair_value_range: str) -> str:
        """評估股票買賣建議"""
        try:
            # 解析合理價格區間
            low_str, high_str = fair_value_range.split("~")
            low_price = float(low_str.strip())
            high_price = float(high_str.strip())
            
            # 計算合理價區間的平均價
            avg_price = (low_price + high_price) / 2
            
            # 根據原始評價邏輯判斷
            if current_price < low_price:
                return "加碼"
            elif current_price < avg_price:
                return "便宜"
            elif current_price <= high_price:
                return "合理"
            else:
                return "昂貴"
     
        except Exception as e:
            logging.error(f"評價計算錯誤: {str(e)}")
            return "不予評價"

# HTML 模板
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>股票推薦系統</title>
    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }

        .dashboard-header {
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }

        .title {
            font-size: 24px;
            font-weight: bold;
            margin-bottom: 20px;
        }

        .filters {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }

        .filter-group {
            display: flex;
            flex-direction: column;
        }

        .filter-group label {
            margin-bottom: 8px;
            font-weight: 500;
        }

        select, input {
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }

        .table-container {
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }

        th {
            background-color: #f8f9fa;
            font-weight: 600;
        }

        tr:hover {
            background-color: #f8f9fa;
        }

        .rating {
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }

        .rating-cheap {
            background-color: #d4edda;
            color: #155724;
        }

        .rating-fair {
            background-color: #cce5ff;
            color: #004085;
        }

        .rating-expensive {
            background-color: #f8d7da;
            color: #721c24;
        }

        .loading {
            text-align: center;
            padding: 20px;
            font-size: 18px;
        }

        .right-align {
            text-align: right;
        }
        .sortable {
            cursor: pointer;
            position: relative;
            padding-right: 20px !important;
        }

        .sortable:hover {
            background-color: #edf2f7;
        }

        .sortable::after {
            content: '⇅';
            position: absolute;
            right: 8px;
            color: #718096;
        }

        .sortable.asc::after {
            content: '↑';
            color: #2b6cb0;
        }

        .sortable.desc::after {
            content: '↓';
            color: #2b6cb0;
        }      
    </style>
</head>
<body>
    <div class="container">
        <div class="dashboard-header">
            <h1 class="title">股票推薦系統</h1>
            <div class="filters">
                <div class="filter-group">
                    <label for="industry">產業別</label>
                    <select id="industry" onchange="filterStocks()">
                        <option value="">全部</option>
                        <option value="金融">金融</option>
                        <option value="營建">營建</option>
                        <option value="航運">航運</option>
                        <option value="半導體">半導體</option>
                        <option value="電子零組件">電子零組件</option>
                        <option value="ETF">ETF</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label for="rating">買賣評等</label>
                    <select id="rating" onchange="filterStocks()">
                        <option value="">全部</option>
                        <option value="加碼">加碼</option>
                        <option value="便宜">便宜</option>
                        <option value="合理">合理</option>
                        <option value="昂貴">昂貴</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label for="minYield">近5年平均殖利率 (%)</label>
                    <input type="number" id="minYield" onchange="filterStocks()" placeholder="輸入殖利率">
                </div>
            </div>
        </div>

        <div class="table-container">
            <table id="stockTable">
                <thead>
                    <tr>
                        <th>代碼</th>
                        <th>名稱</th>
                        <th>產業別</th>
                        <th>日期</th>
                        <th class="sortable right-align" data-sort="close price">股價</th>
                        <th class="sortable right-align" data-sort="avg_5_years_dividend_yield">5年平均殖利率</th>
                        <th>合理價區間</th>
                        <th class="sortable" data-sort="rating">買賣評等</th>
                    </tr>
                </thead>
                <tbody id="stockTableBody">
                    <tr>
                        <td colspan="8" class="loading">載入中...</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        let stocks = [];

        // 初始化載入數據
        async function fetchStocks() {
            try {
                const response = await fetch('/api/stocks');
                stocks = await response.json();
                filterStocks();
            } catch (error) {
                console.error('Error fetching stocks:', error);
                document.querySelector('.loading').textContent = '載入失敗，請重試';
            }
        }

        // 過濾股票
        function filterStocks() {
            const industry = document.getElementById('industry').value;
            const rating = document.getElementById('rating').value;
            const minYield = document.getElementById('minYield').value;

            const filteredStocks = stocks.filter(stock => {
                const matchIndustry = !industry || stock.industry_type === industry;
                const matchRating = !rating || stock.rating === rating;
                const matchYield = !minYield || stock.avg_5_year_dividend_yield >= parseFloat(minYield);
                return matchIndustry && matchRating && matchYield;
            });

            displayStocks(filteredStocks);
        }

        // 顯示股票數據
        function displayStocks(stocksToDisplay) {
            const tbody = document.getElementById('stockTableBody');
            tbody.innerHTML = '';

            stocksToDisplay.forEach(stock => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${stock.stock_code}</td>
                    <td>${stock.stock_name}</td>
                    <td>${stock.industry_type}</td>
                    <td>${stock.date}</td>
                    <td class="right-align">${stock.close_price}</td>
                    <td class="right-align">${stock.avg_5_year_dividend_yield}%</td>
                    <td>${stock.fair_value_range}</td>
                    <td>
                        <span class="rating ${getRatingClass(stock.rating)}">
                            ${stock.rating}
                        </span>
                    </td>
                `;
                tbody.appendChild(row);
            });
        }

        // 獲取評等的樣式類別
        function getRatingClass(rating) {
            switch (rating) {
                case '加碼':
                case '便宜':
                    return 'rating-cheap';
                case '合理':
                    return 'rating-fair';
                case '昂貴':
                    return 'rating-expensive';
                default:
                    return '';
            }
        }

        let currentSort = {
            column: null,
            direction: 'asc'
        };

        // 初始化載入數據
        async function fetchStocks() {
            try {
                const response = await fetch('/api/stocks');
                stocks = await response.json();
                filterStocks();
            } catch (error) {
                console.error('Error fetching stocks:', error);
                document.querySelector('.loading').textContent = '載入失敗，請重試';
            }
        }

        // 初始化排序功能
        function initializeSorting() {
            const headers = document.querySelectorAll('th.sortable');
            headers.forEach(header => {
                header.addEventListener('click', () => {
                    const column = header.dataset.sort;
                    sortStocks(column);
                });
            });
        }

        // 排序股票
        function sortStocks(column) {
            const headers = document.querySelectorAll('th.sortable');
            headers.forEach(header => {
                if (header.dataset.sort == column) {
                    if (currentSort.column === column) {
                        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
                    } else {
                        currentSort.direction = 'asc';
                    }
                
                    header.classList.add(currentSort.direction);
                } else {
                    header.classList.remove ('asc', 'desc');
                }
            });

        currentSort.column = column;

        const sortedStocks = [...stocks].sort((a, b) => {
            let valueA = a[column];
            let valueB = b[column];

            // 處理數字類型的排序
            if (column === 'close_price' || column === 'avg_5_years_dividend_yield') {
                valueA = parseFloat(valueA);
                valueB = parseFloat(valueB);
            }

            if (column === 'rating') {
                const ratingWeight = {
                    '加碼': 1,
                    '便宜': 2,
                    '合理': 3,
                    '昂貴': 4
                };
                valueA = ratingWeight[valueA] || 999;
                valueB = ratingWeight[valueB] || 999;

                }

                if (valueA < valueB) return currentSort.direction === 'asc' ? -1 : 1;
                if (valueA > valueB) return currentSort.direction === 'asc' ? 1 : -1;
                return 0;
            });
            
            displayStocks(sortedStocks);
        }
        
        function filterStocks() {
            const industry = document.getElementById('industry').value;
            const rating = document.getElementById('rating').value;
            const minYield = document.getElementById('minYield').value;

            const filteredStocks = stocks.filter(stock => {
                const matchIndustry = !industry || stock.industry_type === industry;
                const matchRating = !rating || stock.rating === rating;
                const matchYield = !minYield || stock.avg_5_year_dividend_yield >= parseFloat(minYield);
                return matchIndustry && matchRating && matchYield;
            });

            if (currentSort.column) {
                sortStocks(currentSort.column);
            } else {
                displayStocks(filteredStocks);
            }
        
        }

        // 確保載入完成後會呼叫初始化
        document.addEventListener('DOMContentLoaded', () => {
            fetchStocks();
        });

        // 頁面載入時獲取數據
        fetchStocks();
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """渲染主頁"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/stocks')
def get_stocks():
    """API端點：獲取股票數據"""
    try:
        system = StockEvaluationSystem()
        stocks = system.get_stock_evaluations()
        return jsonify(stocks)
    except Exception as e:
        logging.error(f"API錯誤: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8000)