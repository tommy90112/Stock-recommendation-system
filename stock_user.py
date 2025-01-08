from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
from flask_cors import CORS
import logging

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# 資料庫連線設定
db_params = {
    'dbname': 'stock_recommendation_system',
    'user': 'test',
    'password': '123456',
    'host': 'localhost',
    'port': '5433'
}

# 註冊端點
@app.route('/register', methods=['POST'])
def register():
    print("收到註冊請求")
    data = request.get_json()
    print("請求數據:", data)

    username = data.get('username')
    password = data.get('password')
    email = data.get('email')

    if not all([username, password, email]):
        return jsonify({'success': False, 'message': '請填寫完整信息'})
    
    # 密碼加密
    password_hash = generate_password_hash(password)
    
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # 檢查用戶名是否已存在
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        if cur.fetchone():
            return jsonify({'success': False, 'message': '用戶名已存在'})
            
        # 插入新用戶
        cur.execute(
            "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s)",
            (username, password_hash, email)
        )
        conn.commit()
        return jsonify({'success': True, 'message': '註冊成功'})
        
    except Exception as e:
        print("數據庫錯誤:", str(e))
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if conn:
            conn.close()

# 登入端點
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # 檢查用戶名和密碼
        cur.execute("SELECT password_hash, email FROM users WHERE username = %s", (username,))
        result = cur.fetchone()
        
        if result and check_password_hash(result[0], password):
            # 更新最後登入時間
            cur.execute("""
                UPDATE users 
                SET last_login = CURRENT_TIMESTAMP 
                WHERE username = %s
            """, (username,))
            conn.commit()
            
            return jsonify({
                'success': True, 
                'message': '登入成功',
                'data': {
                    'username': username,
                    'email': result[1]
                }
            })
        else:
            return jsonify({'success': False, 'message': '用戶名或密碼錯誤'})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if conn:
            conn.close()

# 獲取用戶資訊端點
@app.route('/user/info/<username>', methods=['GET'])
def get_user_info(username):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        cur.execute("SELECT username, email FROM users WHERE username = %s", (username,))
        user = cur.fetchone()
        
        if user:
            return jsonify({
                'success': True,
                'data': {
                    'username': user[0],
                    'email': user[1]
                }
            })
        return jsonify({'success': False, 'message': '找不到用戶'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        if conn:
            conn.close()

# 更新密碼端點
@app.route('/user/update-password', methods=['POST', 'OPTIONS'])
def update_password():
    # 處理 OPTIONS 請求
    if request.method == 'OPTIONS':
        return jsonify({'success': True}), 200
        
    try:
        data = request.get_json()
        if not data:
            logger.error("未收到 JSON 數據")
            return jsonify({'success': False, 'message': '未接收到數據'}), 400

        username = data.get('username')
        old_password = data.get('oldPassword')
        new_password = data.get('newPassword')
        
        # 輸入驗證
        if not all([username, old_password, new_password]):
            logger.warning(f"缺少必要參數: username={bool(username)}, old_password={bool(old_password)}, new_password={bool(new_password)}")
            return jsonify({'success': False, 'message': '所有欄位都必須填寫'}), 400

        if len(new_password) < 6:
            return jsonify({'success': False, 'message': '新密碼長度不能小於6個字符'}), 400

        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        try:
            # 驗證舊密碼
            cur.execute("SELECT id, password_hash FROM users WHERE username = %s", (username,))
            result = cur.fetchone()
            
            if not result:
                logger.warning(f"找不到用戶: {username}")
                return jsonify({'success': False, 'message': '找不到用戶'}), 404
                
            user_id, stored_password_hash = result
            
            if not check_password_hash(stored_password_hash, old_password):
                logger.warning(f"密碼驗證失敗: {username}")
                return jsonify({'success': False, 'message': '舊密碼錯誤'}), 401
            
            # 更新密碼
            new_password_hash = generate_password_hash(new_password)
            cur.execute("""
                UPDATE users 
                SET password_hash = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (new_password_hash, user_id))
            
            if cur.rowcount == 0:
                logger.error(f"更新密碼失敗: {username}")
                conn.rollback()
                return jsonify({'success': False, 'message': '更新密碼失敗'}), 500
                
            conn.commit()
            logger.info(f"更新密碼成功: {username}")
            return jsonify({'success': True, 'message': '更新密碼成功'}), 200
            
        except Exception as e:
            conn.rollback()
            logger.error(f"更新密碼時發生錯誤: {str(e)}")
            raise
        finally:
            cur.close()
            
    except Exception as e:
        logger.error(f"處理更新密碼時發生錯誤: {str(e)}")
        return jsonify({'success': False, 'message': f'系統錯誤: {str(e)}'}), 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    logger.info("啟動Flask應用程序")
    app.run(port=3000)