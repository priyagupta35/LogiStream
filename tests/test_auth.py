import pytest
from api.security import get_password_hash, verify_password, create_access_token
from jose import jwt
from api.config import settings

def test_password_hashing():
    raw_pass = "SecureSecret123"
    hashed = get_password_hash(raw_pass)
    assert hashed != raw_pass
    assert verify_password(raw_pass, hashed) is True
    assert verify_password("WrongPassword", hashed) is False

def test_jwt_token_generation_and_decoding():
    data = {"sub": "user-uuid-123", "role": "CUSTOMER"}
    token = create_access_token(data)
    decoded = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    assert decoded["sub"] == "user-uuid-123"
    assert decoded["role"] == "CUSTOMER"
    assert "exp" in decoded
