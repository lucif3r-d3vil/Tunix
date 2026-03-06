from fastapi import Header, HTTPException
from security import decode_token


def require_user(authorization: str = Header(None)):

    if not authorization:
        raise HTTPException(401, "Login required")

    token = authorization.replace("Bearer ", "")

    try:
        return decode_token(token)
    except:
        raise HTTPException(401, "Invalid token")


def require_admin(user=Header(None)):

    data = require_user(user)

    if data["role"] != "admin":
        raise HTTPException(403, "Admin required")

    return data