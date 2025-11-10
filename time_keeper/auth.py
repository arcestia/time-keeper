import os
import hashlib
import hmac
from typing import Tuple

ALGO = "pbkdf2_sha256"
ITERATIONS = 390000
SALT_BYTES = 16
KEY_LEN = 32


def hash_passcode(passcode: str) -> str:
    if not isinstance(passcode, str) or passcode == "":
        raise ValueError("passcode must be a non-empty string")
    salt = os.urandom(SALT_BYTES)
    dk = hashlib.pbkdf2_hmac("sha256", passcode.encode("utf-8"), salt, ITERATIONS, dklen=KEY_LEN)
    return f"{ALGO}${ITERATIONS}${salt.hex()}${dk.hex()}"


def verify_passcode(passcode: str, stored: str) -> bool:
    try:
        algo, iterations_s, salt_hex, hash_hex = stored.split("$")
        if algo != ALGO:
            return False
        iterations = int(iterations_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
        dk = hashlib.pbkdf2_hmac("sha256", passcode.encode("utf-8"), salt, iterations, dklen=len(expected))
        return hmac.compare_digest(dk, expected)
    except Exception:
        return False
