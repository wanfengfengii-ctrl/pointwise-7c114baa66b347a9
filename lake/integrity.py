import hashlib


class IntegrityError(Exception):
    pass


def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def verify_digest(data, expected):
    if digest_bytes(data) != expected:
        raise IntegrityError("content digest mismatch")
