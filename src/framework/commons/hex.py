__all__ = (
    "h2b",
    "b2h",
    "h2lb",
    "b2hs",
    "b2i",
    "i2b",
    "h2s",
    "s2h",
    "h2i",
    "i2h",
    "s2b",
    "b2s",
    "i2s",
    "s2i",
    "l2s",
    "s2l",
    "m0h",
    "h2h",
    "lhi",
    "lhh",
    "llh",
    "loi",
    "t2h",
    "r0h",
    "hhh",
    "mhh",
    "lshi",
    "lski",
    "h2k",
    "_xor",
)

import ast
import re
from typing import Optional


def t2h(t: str) -> str:
    if not t:
        return None
    return b2h(t.encode())


def hhh(h: str) -> str:
    if not h or not h.strip():
        return h
    h = re.sub(r"[^0-9a-fA-F]+", "", h.strip())
    return (
        f" {' '.join(h[i : (i + 2)] for i in range(0, len(h), 2)).strip()} "
        if len(h) % 2 < 1
        else None
    )


def mhh(match):
    return hhh(match.group(0))


def h2h(h: str) -> str:
    if not h or not h.strip():
        return h
    return re.sub(r"(?<=\})[^{]+(?=\{)*", mhh, "{}" + h)[2:].strip()


def h2b(h: str) -> bytes:
    return bytes.fromhex(h)


def b2h(b: bytes) -> str:
    if b is None:
        return ""
    return " ".join([f"{i:02x}" for i in b])


def b2hs(b: bytes) -> str:
    if b is None:
        return ""
    return "".join([f"{i:02x}" for i in b])


def h2lb(h: str):
    return list(bytes.fromhex(h))


def b2i(b: bytes) -> int:
    # return int(b2h(a).replace(' ', ''), base=16)
    return int.from_bytes(b, byteorder="big")


def i2b(i: int) -> bytes:
    return i.to_bytes(max(1, (i.bit_length() + 7) // 8), "big")


def h2s(h: str, encoding="latin-1") -> str:
    return bytes.fromhex(h).decode(encoding)


def x2x(x):
    return f"{ord(x):02x}" if ord(x) < 256 else x.encode("utf-8").hex()


def s2h(s: str, encoding="latin-1") -> str:
    if not s or s == "None":
        return None
    return b2h(s.encode(encoding)) or None


def h2i(h: str) -> int:
    return int.from_bytes(bytes.fromhex(h), byteorder="big")


def i2h(i: int) -> str:
    if not isinstance(i, int) or i < 0:
        raise ValueError("Expected a nonnegative integer")
    return i2b(i).hex()


def s2b(s: str, encoding="latin-1") -> bytes:
    return s.encode(encoding)


def b2s(b: bytes, encoding="latin-1") -> str:
    return bytes(b).decode(encoding)


def i2s(i: int) -> str:
    return b2s(i2b(i))


def s2i(s: str) -> int:
    return b2i(s2b(s))


def l2s(l):
    """Transform unsigned bytes to a Latin-1 string."""
    return bytes(l).decode("latin-1")


def s2l(s):
    """Transform a Latin-1 string to unsigned bytes."""
    return list(s.encode("latin-1"))


def m0h(hex="", start=0, offset=0):
    return b2h(h2b(hex)[start:offset])


def h2k(hex: Optional[str] = ""):
    return [ast.literal_eval(i) for i in re.findall(r"{[^{}]*}", hex)]


def loi(i: int = 0, lim: Optional[int] = 255):
    """Number of base-(lim+1) digits; default is unsigned byte width."""
    if not isinstance(i, int) or i < 0 or not isinstance(lim, int) or lim < 1:
        raise ValueError("Require a nonnegative integer and lim >= 1")
    length = 1
    while i > lim:
        length += 1
        i //= lim + 1
    return length


def lshi(hex: Optional[str] = ""):
    # rem keys
    hex = re.sub(r"{[^{}]*}", "", hex.strip()) if hex and hex.strip() else ""
    # rem no hex
    hex = re.sub(r"[^0-9a-fA-F]+", "", hex).strip()
    hex = hex if len(hex) % 2 < 1 else ""
    return len(h2b(hex)) or 0


def lski(hex: Optional[str] = ""):
    return (
        sum(
            obj["l"]
            for obj in [
                # eval string dict from extract keys
                ast.literal_eval(i)
                for i in re.findall(r"{[^{}]*}", hex)
            ]
            if ("l" in obj and type(obj["l"]) is int)
        )
        or 0
    )


def lhi(hex: Optional[str] = ""):
    return lshi(hex) + lski(hex) or 0


def llh(i: Optional[int] = 0, lim: Optional[int] = 255, ber: Optional[bool] = False):
    length = loi(i, lim)
    if not ber and i > 65535:
        raise ValueError("APDU Lc cannot exceed 65535 bytes")
    if ber and length > 127:
        raise ValueError("BER length-of-length cannot exceed 127 bytes")
    return (
        f"{0x80 | length:02x} {i:0{2 * length}x}"
        if ber
        else (f"{i:02x}" if i <= lim else f"{i:06x}")
    )


def lhh(hex: Optional[str] = "", lim: Optional[int] = 255, ber: Optional[bool] = False):
    return llh(i=lhi(hex), lim=lim, ber=ber)


def r0h(a, b):
    return " ".join(["{:02X}".format(x) for x in range(a, b)])


def _xor(kifd, kicc):
    # x =hex(int(kifd, 16) ^ int(kicc, 16)).upper()[2:]
    x = i2h(h2i(kifd) ^ h2i(kicc))
    return x
