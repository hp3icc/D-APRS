#!/usr/bin/env python3
"""
lrrp_rx.py - Decoder LRRP MOTOTRBO (payload UDP puerto 4001 -> dict con lat/lon/etc).
Portado de lrrpdec.py (KD8EYF/ruso) parselrrp + helpers. Decodifica los reportes de
localizacion (0x0D triggered, 0x07 immediate response) que manda la radio.
Formula lat/lon corroborada por TRBO-NET: lat = BE32*180/0xFFFFFFFF, lon = BE32*360/0xFFFFFFFF.
"""
from datetime import datetime

LRRP_TYPES = {0x05: "ImmediateLocationRequest", 0x07: "ImmediateLocationResponse",
              0x09: "TriggeredLocationStartRequest", 0x0B: "TriggeredLocationStartResponse",
              0x0D: "TriggeredLocationData", 0x0F: "TriggeredLocationStopRequest",
              0x11: "TriggeredLocationStopResponse", 0x14: "ProtocolVersionRequest",
              0x15: "ProtocolVersionResponse"}

def _wlat(raw):
    v = raw * 180.0 / 0xFFFFFFFF
    return round(v - 180.0 if v > 90.0 else v, 5)

def _wlon(raw):
    v = raw * 360.0 / 0xFFFFFFFF
    return round(v - 360.0 if v > 180.0 else v, 5)

def readFloat(bs):
    return int(bs[0]) + int(bs[1]) * 0.01

def readDateTime(bs):
    year  = bs[0] << 6 | (bs[1] >> 2 & 0x3f)
    month = (bs[1] & 3) << 2 | bs[2] >> 6 & 3
    day   = bs[2] >> 1 & 31
    hour  = ((bs[2] & 1) << 4) | (bs[3] >> 4 & 0xf)
    minute = (bs[3] & 15) << 2 | bs[4] >> 6 & 3
    second = bs[4] & 0x3f
    try:
        return datetime(year, month, day, hour, minute, second).strftime("%Y/%m/%d %H:%M:%S")
    except Exception:
        return None

def parselrrp(udpdata):
    """udpdata = payload UDP (bytes). Devuelve dict; con 'Latitude'/'Longitude' si hay fix."""
    result = {}
    i = 0
    try:
        ptype = udpdata[i]
        if ptype not in LRRP_TYPES:
            result["Error"] = "Unknown packet type 0x%02x" % ptype
            return result
        result["PacketType"] = LRRP_TYPES[ptype]
        i = 2   # salta opcode + byte de longitud
        if ptype in (0xd, 0x7, 0xb, 0x11):
            while i < len(udpdata):
                t = udpdata[i]; i += 1
                if t == 0x22:
                    rl = udpdata[i]; i += 1
                    result["requestID"] = str(int.from_bytes(udpdata[i:i+rl], "big")); i += rl
                elif t == 0x51:
                    lat = int.from_bytes(udpdata[i:i+4], "big"); lon = int.from_bytes(udpdata[i+4:i+8], "big")
                    result["Latitude"] = _wlat(lat); result["Longitude"] = _wlon(lon)
                    result["Radius"] = round(readFloat(udpdata[i+8:i+10]), 2); i += 10
                elif t == 0x34:
                    result["RemoteTime"] = readDateTime(udpdata[i:i+5]); i += 5
                elif t == 0x37:
                    rc = udpdata[i]; i += 2
                    if rc & 0x80: rc = (rc << 7) + udpdata[i]; i += 1
                    result["ResponseCode"] = str(rc)
                elif t == 0x38:
                    result["ResponseCode"] = "0"
                elif t == 0x55:
                    lat = int.from_bytes(udpdata[i:i+4], "big"); lon = int.from_bytes(udpdata[i+4:i+8], "big")
                    result["Latitude"] = _wlat(lat); result["Longitude"] = _wlon(lon)
                    result["Radius"] = round(readFloat(udpdata[i+8:i+10]), 2)
                    result["Altitude"] = round(readFloat(udpdata[i+10:i+12]), 2)
                    result["Accuracy"] = round(readFloat(udpdata[i+12:i+14]), 2); i += 14
                elif t == 0x56:
                    result["Direction"] = str(udpdata[i] * 2); i += 1
                elif t == 0x66:
                    lat = int.from_bytes(udpdata[i:i+4], "big"); lon = int.from_bytes(udpdata[i+4:i+8], "big")
                    result["Latitude"] = _wlat(lat); result["Longitude"] = _wlon(lon); i += 8
                elif t == 0x69:
                    lat = int.from_bytes(udpdata[i:i+4], "big"); lon = int.from_bytes(udpdata[i+4:i+8], "big")
                    result["Latitude"] = _wlat(lat); result["Longitude"] = _wlon(lon)
                    result["Altitude"] = round(readFloat(udpdata[i+10:i+12]), 2); i += 10
                elif t == 0x6C:
                    result["Speed"] = round(readFloat(udpdata[i:i+2]) * 2.23, 2); i += 2  # mph
                else:
                    break   # tag no reconocido (campos extra al final): parar, conservar lat/lon
        return result
    except Exception as e:
        result["Error"] = "parse exc: %s" % e
        return result


if __name__ == "__main__":
    # vectores reales (de los comentarios de lrrpdec.py)
    vectors = [
        "0D1A22040000000134 1F973280C6 51 4BE8AED73B882B46 0352 6C 0215",
        "0D15 2203 000001 51 4BDE538F3B94F1E6 02436C0000561E",
    ]
    for v in vectors:
        b = bytes.fromhex(v.replace(" ", ""))
        print(v.replace(" ", ""))
        print("   ->", parselrrp(b))
