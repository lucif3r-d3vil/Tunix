from datetime import datetime, timedelta
from jose import jwt
from passlib.context import CryptContext

SECRET = "hometunes-secret-change-me"
ALGO   = "HS256"
pwd    = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def hash_password(p):    return pwd.hash(p)
def verify_password(p,h):return pwd.verify(p,h)

def create_token(uid, role):
    payload = {"sub": uid, "role": role, "exp": datetime.utcnow() + timedelta(hours=72)}
    return jwt.encode(payload, SECRET, algorithm=ALGO)

def decode_token(token):
    return jwt.decode(token, SECRET, algorithms=[ALGO])