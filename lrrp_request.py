#!/usr/bin/env python3
"""
lrrp_request.py - Construye peticiones LRRP MOTOTRBO (payload UDP, puerto 4001).
Formato extraido por ingenieria inversa de MTNetTester.exe con Ghidra (2026-06-08):
  FUN_004167b0 (selecciona flags por tipo/modo) -> FUN_00416540 (encoder) ->
  FUN_00416070 (varint base-128 MSB-first) + FUN_004164a0 (reqID TLV 0x22/0x23).
Cotejado con lrrpdec.py::parselrrp (decoder) y TRBO-NET LOC.pm.

Estructura del request:
  [opcode][reqID TLV opcional][tags de campos pedidos...][bloque trigger]
  opcode: 0x05 Immediate | 0x09 TriggeredStart | 0x0F TriggeredStop | 0x14 GetVersion
  reqID : 0x23 <1B>            (si valor <= 256)
          0x22 <len> <val BE>  (len = 2..4 bytes)
  tags  : 0x50 accuracy | 0x51 accuracy+time | 0x52 time | 0x54 alt | 0x57 dir
          0x61 | 0x62 | 0x64   (campos extra)
  trigger periodico: 0x34 0x31 <varint(intervalo_segundos)>
  trigger on-change : 0x33
"""

# opcodes
IMMEDIATE      = 0x05
TRIGGER_START  = 0x09
TRIGGER_STOP   = 0x0F
GET_VERSION    = 0x14

# tags de campos a reportar
TAG_ACCURACY       = 0x50
TAG_ACCURACY_TIME  = 0x51
TAG_TIME           = 0x52
TAG_ALTITUDE       = 0x54
TAG_DIRECTION      = 0x57


def encode_varint(n):
    """base-128, MSB-first, bit 0x80 de continuacion en todos menos el ultimo (FUN_00416070)."""
    if n < 0:
        raise ValueError("varint negativo")
    groups = []
    # 32-bit como en el binario: 5 grupos de 7 bits (0x1c,0x15,0xe,7,0)
    started = False
    for shift in (28, 21, 14, 7):
        g = (n >> shift) & 0x7f
        if g != 0 or started:
            groups.append(0x80 | g)
            started = True
    groups.append(n & 0x7f)            # ultimo byte sin bit de continuacion
    return bytes(groups)


def build_reqid(reqid, four_byte=True):
    """TLV de request-id. Por defecto la forma de captura real `22 04 <val BE32>`
    (= TRBO-NET y el ejemplo de lrrpdec). Ghidra FUN_004164a0 usa `23 <1B>` si <=256;
    pasar four_byte=False para esa variante."""
    if not four_byte and reqid <= 0x100:
        return bytes([0x23, reqid & 0xff])
    return bytes([0x22, 0x04]) + (reqid & 0xffffffff).to_bytes(4, "big")


def build_request(opcode, reqid=None, field_tags=(TAG_ACCURACY_TIME,),
                  interval_s=None, on_change=False, reqid_4byte=True):
    """Monta el payload LRRP (sin IP/UDP): [opcode][len-varint][cuerpo].
    El cuerpo = reqID + tags de campos + bloque trigger. Para periodico:
    opcode=TRIGGER_START + interval_s (segundos)."""
    body = bytearray()
    if reqid is not None:
        body += build_reqid(reqid, four_byte=reqid_4byte)
    for t in field_tags:
        body.append(t)
    if opcode == TRIGGER_START:
        if interval_s is not None:
            body += bytes([0x34, 0x31]) + encode_varint(int(interval_s))  # periodico
        elif on_change:
            body.append(0x33)                                             # on-change
    return bytes([opcode]) + encode_varint(len(body)) + bytes(body)


def triggered_periodic(interval_s, reqid=1, fields=(TAG_ACCURACY_TIME, TAG_ALTITUDE)):
    """Petición LRRP periódica: que la radio reporte GPS cada interval_s segundos."""
    return build_request(TRIGGER_START, reqid=reqid, field_tags=fields, interval_s=interval_s)


def immediate(reqid=1, fields=(TAG_ACCURACY_TIME,)):
    return build_request(IMMEDIATE, reqid=reqid, field_tags=fields)


def stop_trigger(reqid=1):
    return build_request(TRIGGER_STOP, reqid=reqid, field_tags=())


if __name__ == "__main__":
    print("varint(60)   =", encode_varint(60).hex(), "(esperado 3c)")
    print("varint(300)  =", encode_varint(300).hex(), "(esperado 822c)")
    print("varint(127)  =", encode_varint(127).hex(), "(esperado 7f)")
    print("varint(128)  =", encode_varint(128).hex(), "(esperado 8100)")
    print("reqid(1)         =", build_reqid(1).hex(), "(esperado 2204 00000001)")
    print("reqid(0x37271707) =", build_reqid(0x37271707).hex(), "(TRBO-NET: 220437271707)")
    print()
    # ejemplo real (lrrpdec): 09 0E 2204 00000001 51 ... 34 31 78  -> opcode+len+cuerpo
    print("periodico 60s :", triggered_periodic(60).hex())
    print("periodico 120s:", triggered_periodic(120).hex())
    print("immediate     :", immediate().hex())
    print("stop          :", stop_trigger().hex())
    # comprobar que parselrrp (decoder real) lo digiere: i empieza en 2 (salta opcode+len)
    p = triggered_periodic(120)
    print("len byte =", p[1], "= len(cuerpo) =", len(p) - 2, "->", p[1] == len(p) - 2)
