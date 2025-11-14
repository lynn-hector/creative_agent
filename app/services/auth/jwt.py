import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from app.settings import settings
from typing import Dict, Any
from Crypto.Cipher import AES
from binascii import unhexlify
from starlette.datastructures import Headers


def aes_cbc_decrypt(uid_code: str) -> str:
    """
    使用AES CBC模式解密uid_code。
    参数：
        uid_code: 需要解密的字符串（16进制编码）
    说明：
        解密用的密钥（key_hex）和初始化向量（iv_hex）均从配置文件settings.py读取。
    返回：
        解密后的明文字符串
    """
    key_hex = settings.AES_CBC_KEY_HEX
    iv_hex = settings.AES_CBC_IV_HEX
    key = unhexlify(key_hex)
    iv = unhexlify(iv_hex)
    data = unhexlify(uid_code)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(data)
    pad_len = decrypted[-1]
    decrypted = decrypted[:-pad_len]
    return decrypted.decode('utf-8')


def aes_ecb_decrypt(uid_code: str) -> str:
    """
    使用AES ECB模式解密uid_code。
    参数：
        uid_code: 需要解密的字符串（16进制编码）
    说明：
        解密用的密钥（key）从配置文件settings.py读取。
    返回：
        解密后的明文字符串
    """
    key = settings.AES_ECB_KEY
    key_bytes = key.encode('utf-8')
    data = unhexlify(uid_code)
    cipher = AES.new(key_bytes, AES.MODE_ECB)
    decrypted = cipher.decrypt(data)
    pad_len = decrypted[-1]
    decrypted = decrypted[:-pad_len]
    return decrypted.decode('utf-8')


def uid_decode(uid_code: str, mode: str = "cbc") -> int:
    """
    解密uid_code，得到用户ID。
    参数：
        uid_code: 需要解密的字符串（16进制编码）
        mode: 解密模式，支持"cbc"和"ecb"，默认"cbc"
    返回：
        用户ID（int类型），解密失败返回0
    """
    try:
        if mode == "cbc":
            uid_str = aes_cbc_decrypt(uid_code)
        elif mode == "ecb":
            uid_str = aes_ecb_decrypt(uid_code)
        else:
            return 0
        return int(uid_str)
    except Exception:
        return 0


def get_user_id_from_header(headers: Headers) -> Dict[str, Any]:
    """
    从请求头中获取用户ID。
    参数：
        headers: 请求头字典，需包含Authorization字段
    返回：
        字典，包含：
            id: 用户ID（int类型），获取失败为None
            error: 错误信息，可能为"missing"（缺少token）、"invalid"（无效token）、"expired"（过期）、"other"（其他错误），无错误为None
    """
    auth_header = headers.get("Authorization")
    if not auth_header:
        return {"id": None, "error": "missing"}
    # 检查Authorization头是否以Bearer开头，如果是则去掉
    if auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()
    else:
        token = auth_header.strip()
    try:
        payload = jwt.decode(token, settings.JWT_TOKEN_AUTH_SECRET, algorithms=[settings.JWT_ALGORITHM])
        uid_code = payload.get("id")
        user_id = uid_decode(uid_code, mode="cbc") if uid_code else None
        if user_id == 0:
            return {"id": None, "error": "invalid"}
        return {"id": user_id, "error": None}
    except ExpiredSignatureError:
        return {"id": None, "error": "expired"}
    except InvalidTokenError:
        return {"id": None, "error": "invalid"}
    except Exception as e:
        print(e)
        return {"id": None, "error": "other"}
