import uuid
from datetime import datetime, timezone

################################################################################
# Constants
################################################################################

################################################################################
# Utility Methods
################################################################################


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_uid(prefix: str, length: int, sep: str = "_") -> str:
    if length <= 0:
        length = 16

    uid = uuid.uuid4().hex
    extended_uid = (uid * ((length // 32) + 1))[:length]

    return f"{prefix}{sep}{extended_uid}"
