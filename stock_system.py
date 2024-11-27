# config/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = "postgresql://user:password@localhost:5432/stock_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# models/stock.py
from sqlalchemy import Column, Integer, String, Float, Date
from config.database import Base

class Stock(Base):
    __tablename__ = "stocks"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True)
    name = Column(String)
    current_price = Column(Float)
    date = Column(Date)
    
class StockPrice(Base):
    __tablename__ = "stock_prices"
    
    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))
    date = Column(Date)
    open_price = Column(Float)
    close_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    volume = Column(Integer)

# services/excel_import.py
import pandas as pd
from sqlalchemy.orm import Session
from models.stock import Stock, StockPrice

class ExcelImportService:
    def __init__(self, db: Session):
        self.db = db
    
    def import_from_excel(self, file_path: str):
        df = pd.read_excel(file_path)
        for _, row in df.iterrows():
            stock = Stock(
                code=row['代碼'],
                name=row['名稱'],
                current_price=row['股價']
            )
            self.db.add(stock)
        self.db.commit()

# services/stock_analysis.py
class StockAnalysisService:
    def __init__(self, db: Session):
        self.db = db
    
    def calculate_reasonable_price(self, stock_code: str) -> tuple:
        # 實作合理價計算邏輯
        stock_data = self.db.query(StockPrice).filter(Stock.code == stock_code).all()
        # 這裡實作您的計算公式
        return min_price, max_price
    
    def evaluate_stock(self, stock_code: str) -> str:
        # 實作股票評等邏輯
        stock = self.db.query(Stock).filter(Stock.code == stock_code).first()
        current_price = stock.current_price
        min_price, max_price = self.calculate_reasonable_price(stock_code)
        
        if current_price < min_price:
            return "便宜"
        elif current_price > max_price:
            return "昂貴"
        else:
            return "合理"

# api/main.py
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from config.database import SessionLocal
from services.stock_analysis import StockAnalysisService

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/stocks/{stock_code}/analysis")
def get_stock_analysis(stock_code: str, db: Session = Depends(get_db)):
    analysis_service = StockAnalysisService(db)
    min_price, max_price = analysis_service.calculate_reasonable_price(stock_code)
    evaluation = analysis_service.evaluate_stock(stock_code)
    
    return {
        "stock_code": stock_code,
        "reasonable_price_range": f"{min_price}-{max_price}",
        "evaluation": evaluation
    }

# dashboard/app.py
import streamlit as st
import requests

def main():
    st.title("股票推薦系統")
    
    # 從資料庫獲取所有股票代碼
    stocks = get_all_stocks()
    
    # 建立下拉選單
    selected_stock = st.selectbox("選擇股票", stocks)
    
    if selected_stock:
        # 呼叫API獲取分析結果
        analysis = requests.get(f"/api/stocks/{selected_stock}/analysis").json()
        
        st.write("合理價區間:", analysis["reasonable_price_range"])
        st.write("買賣評等:", analysis["evaluation"])

if __name__ == "__main__":
    main()