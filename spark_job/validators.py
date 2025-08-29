import re
from datetime import datetime

IIN_RE = re.compile(r"^\d{12}$")
DATE_FORMATS = ["%Y-%m-%d, %H:%M:%S", "%d.%m.%y %H:%M", "%d/%m/%Y %H:%M:%S"]

def parse_dt(s: str):
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def valid_iin(s: str) -> bool:
    return bool(IIN_RE.match(str(s)))

def normalize_city(address: str) -> str:
    if not address or address in ("", "unknown"):
        return ""
    
    return address.split(",", 1)[0].strip()

def normalize_address(address: str) -> str:
    if not address or address in ("", "unknown"):
        return ""
    
    return address

def valid_vat(rate: str) -> str | None:
    m = re.search(r"\d+", rate)
    if m:
        return m.group(0) + "%"   # цифры + %
    return None