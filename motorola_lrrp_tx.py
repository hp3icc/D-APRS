#!/usr/bin/env python3
"""
motorola_lrrp_tx.py - Envuelve un payload LRRP (lrrp_request.py) en IP/UDP puerto 4001
+ bloques DMR rate-1/2 + data header + preambulos CSBK + framing DMRD, listo para
inyectar por send_master (igual que el ARS ACK pero con puerto 4001 y payload LRRP).
Reusa los primitivos ya validados de motorola_tx.py.
"""
import os
import motorola_tx as tx
import lrrp_request as lr

LRRP_PORT = 4001

def build_lrrp_frames(radio_id, lrrp_payload, src_id=tx.GW_ID, peer_id=tx.GW_ID,
                      n_pre=16, ip_id=0x0002, src_cai=0x0D, dst_cai=0x0C):
    """Devuelve la secuencia de tramas DMRD (preambulos + header + bloques) para enviar
    'lrrp_payload' (bytes LRRP) al radio por UDP 4001. Mismo addressing que el ARS ACK."""
    ip = tx.build_ip_udp(src_id, radio_id, LRRP_PORT, LRRP_PORT, lrrp_payload,
                         ip_id=ip_id, src_cai=src_cai, dst_cai=dst_cai)
    blocks, nb = tx.build_blocks(ip)
    pad = nb*12 - 4 - len(ip)
    hdr = tx.build_data_header(src_id, radio_id, nb, pad)
    sid = os.urandom(4)
    frames = []; seq = 0
    total_after = 1 + nb
    for i in range(n_pre):
        btf = (n_pre - 1 - i) + total_after
        pre = tx.build_preamble(src_id, radio_id, btf)
        frames.append(tx.build_dmrd(seq, src_id, radio_id, peer_id, 3, sid,
                                    payload33=tx.block_to_payload(pre, 3))); seq += 1
    frames.append(tx.build_dmrd(seq, src_id, radio_id, peer_id, 6, sid,
                                payload33=tx.block_to_payload(hdr, 6))); seq += 1
    for blk in blocks:
        frames.append(tx.build_dmrd(seq, src_id, radio_id, peer_id, 7, sid,
                                    payload33=tx.block_to_payload(blk, 7))); seq += 1
    return frames, ip

def build_triggered_periodic_frames(radio_id, interval_s=120, **kw):
    """Atajo: petir GPS periodico cada interval_s segundos al radio."""
    payload = lr.triggered_periodic(interval_s)
    return build_lrrp_frames(radio_id, payload, **kw)


# ----------------------- OFFLINE ROUND-TRIP SELF-TEST -----------------------
if __name__ == '__main__':
    from bitarray import bitarray
    from dmr_utils3 import decode as dmrdecode
    from dmr_codec import decode_full

    RADIO = 7140023
    payload = lr.triggered_periodic(120)
    print("LRRP payload (periodico 120s):", payload.hex())
    frames, ip = build_lrrp_frames(RADIO, payload)
    print("IP/UDP datagram (%dB): %s" % (len(ip), ip.hex()))
    print("  IP ok:", _ok := (ip[0] == 0x45), " dport:", (ip[22]<<8)|ip[23], "(esperado 4001)")
    n_pre = sum(1 for f in frames if (f[15] & 0xf) == 3)
    print("frames: %d (%d preambulos + 1 header + %d datos)" % (len(frames), n_pre, len(frames)-n_pre-1))

    # round-trip: decodificar header+bloques por el camino del gateway y recuperar el IP
    def dec(p33):
        bits = bitarray(dmrdecode.to_bits(p33)); del bits[98:166]
        return decode_full(bits).tobytes()[:12]
    asm = b""
    for f in frames:
        vs = f[15] & 0xf
        if vs == 7:
            asm += dec(f[20:53])
    iplen = (ip[2]<<8)|ip[3]
    rec_ip = asm[:iplen]
    print("recovered IP == built IP:", rec_ip == ip)
    # extraer el payload UDP recuperado y re-parsear como LRRP request
    udp_payload = rec_ip[28:iplen]
    print("recovered LRRP payload:", udp_payload.hex(), "==", payload.hex(), "->", udp_payload == payload)
