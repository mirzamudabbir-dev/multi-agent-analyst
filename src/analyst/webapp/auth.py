import os
import sqlite3
import jwt
import datetime
from pathlib import Path
from typing import Optional, Dict
from cryptography.fernet import Fernet
import bcrypt

# Security Configuration
SECRET_KEY = os.environ.get("ANALYST_SECRET_KEY", "super-secret-dev-key-change-in-prod")
DB_PATH = Path("output/users.db")
MASTER_KEY_FILE = Path("output/master.key")

def get_master_key() -> bytes:
    """Load or generate the master encryption key for API keys."""
    if MASTER_KEY_FILE.exists():
        return MASTER_KEY_FILE.read_bytes()
    key = Fernet.generate_key()
    MASTER_KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    MASTER_KEY_FILE.write_bytes(key)
    return key

# Initialize SQLite
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            encrypted_api_key TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

class AuthManager:
    def __init__(self):
        self.fernet = Fernet(get_master_key())

    def hash_password(self, password: str) -> str:
        pwd_bytes = password.encode('utf-8')
        if len(pwd_bytes) > 72:
            pwd_bytes = pwd_bytes[:72]
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(pwd_bytes, salt)
        return hashed.decode('utf-8')

    def verify_password(self, password: str, hashed: str) -> bool:
        pwd_bytes = password.encode('utf-8')
        if len(pwd_bytes) > 72:
            pwd_bytes = pwd_bytes[:72]
        hash_bytes = hashed.encode('utf-8')
        return bcrypt.checkpw(pwd_bytes, hash_bytes)

    def encrypt_key(self, api_key: str) -> str:
        if not api_key:
            return None
        return self.fernet.encrypt(api_key.encode()).decode()

    def decrypt_key(self, encrypted_key: str) -> str:
        if not encrypted_key:
            return None
        return self.fernet.decrypt(encrypted_key.encode()).decode()

    def create_access_token(self, username: str) -> str:
        payload = {
            "sub": username,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7)
        }
        return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

    def decode_token(self, token: str) -> Optional[str]:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            return payload.get("sub")
        except:
            return None

    def register_user(self, username: str, password: str) -> bool:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO users (username, password_hash) VALUES (?, ?)",
                (username, self.hash_password(password))
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()

    def authenticate_user(self, username: str, password: str) -> Optional[str]:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()
        conn.close()
        
        if row and self.verify_password(password, row[0]):
            return self.create_access_token(username)
        return None

    def set_user_api_key(self, username: str, api_key: str):
        encrypted = self.encrypt_key(api_key)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET encrypted_api_key = ? WHERE username = ?", (encrypted, username))
        conn.commit()
        conn.close()

    def get_user_api_key(self, username: str) -> Optional[str]:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT encrypted_api_key FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()
        conn.close()
        
        if row and row[0]:
            return self.decrypt_key(row[0])
        return None

auth_manager = AuthManager()
