#!/usr/bin/env python3
"""
motorola_tx.py - Build the MOTOTRBO ARS-ACK (server->radio) as DMR rate-1/2 data
frames ready to inject into the master via send_master(). OFFLINE-VALIDATED:
the data blocks we build decode back (gateway path) to the exact ACK IP datagram.

Pieces proven elsewhere: dmr_codec.encode_block (BPTC rate-1/2), crc_model.crc32_trbo
(packet CRC, hypothesis), header CRC-16 (CCITT 0x1021 init0 MSB-first xorout 0x3333).
The 68 middle bits (slot type + sync) are copied verbatim from real captured frames
of the same data type (avoids Golay slot-type encoding).
"""
from bitarray import bitarray
from dmr_utils3 import decode as dmrdecode
from dmr_codec import encode_block, decode_full
from crc_model import crc32_trbo, crc32_dmr_bytes

RADIO_ID = 7140023      # the Motorola
GW_ID    = 213999       # data gateway / DATA_DMR_ID

# ---- real captured frames (first 33 bytes = _data[20:53]) per data type ----
TPL = {
 6: "4f650ace65167233e1836c43c58d5d7f77fd757338c0355033256183828286b4",  # header
 7: "0cbfb16b034d00f4a2c0888205ed5d7f77fd757095b0409c42d30026130141987a",  # rate-1/2 data
 3: "52ce058589a413fc6b38481144cd5d7f77fd757acb324bb8e5c97483fb1f761396",  # CSBK preamble
}
def _middle68(tpl_hex):
    bits = bitarray(dmrdecode.to_bits(bytes.fromhex(tpl_hex)))
    return bits[98:166]

# ---- CRC-16 for the data header (CCITT 0x1021, init 0, MSB-first, xorout 0x3333) ----
def crc16_hdr(data):
    c = 0
    for byte in data:
        c ^= byte << 8
        for _ in range(8):
            c = ((c << 1) ^ 0x1021) & 0xffff if (c & 0x8000) else (c << 1) & 0xffff
    return c ^ 0x3333

# ---- IP/UDP datagram builder ----
def _ones16(data):
    if len(data) % 2: data += b"\x00"
    s = 0
    for i in range(0, len(data), 2):
        s += (data[i] << 8) | data[i+1]
    while s >> 16: s = (s & 0xffff) + (s >> 16)
    return (~s) & 0xffff

def build_ip_udp(src_id, dst_id, sport, dport, payload, ip_id, src_cai=0x0D, dst_cai=0x0C):
    src_ip = bytes([src_cai]) + src_id.to_bytes(3, 'big')
    dst_ip = bytes([dst_cai]) + dst_id.to_bytes(3, 'big')
    udp_len = 8 + len(payload)
    # UDP checksum (pseudo-header)
    pseudo = src_ip + dst_ip + b"\x00\x11" + udp_len.to_bytes(2, 'big')
    udp_nock = sport.to_bytes(2,'big') + dport.to_bytes(2,'big') + udp_len.to_bytes(2,'big') + b"\x00\x00" + payload
    uck = _ones16(pseudo + udp_nock)
    udp = sport.to_bytes(2,'big') + dport.to_bytes(2,'big') + udp_len.to_bytes(2,'big') + uck.to_bytes(2,'big') + payload
    total = 20 + len(udp)
    iph = bytes([0x45,0x01]) + total.to_bytes(2,'big') + ip_id.to_bytes(2,'big') + b"\x00\x00" + bytes([0x40,0x11]) + b"\x00\x00" + src_ip + dst_ip
    ick = _ones16(iph)
    iph = iph[:10] + ick.to_bytes(2,'big') + iph[12:]
    return iph + udp

# ---- assemble data payload into 12-byte rate-1/2 blocks ----
def build_blocks(ip_datagram):
    total = len(ip_datagram) + 4
    pad = (12 - total % 12) % 12
    crc_input = ip_datagram + b'\x00' * pad
    crc = crc32_dmr_bytes(crc_input)        # ETSI DMR CRC-32, little-endian, over IP+pad
    raw = crc_input + crc
    blocks = [raw[i:i+12] for i in range(0, len(raw), 12)]
    return blocks, len(blocks)

# ---- 12-byte block -> 33-byte on-air payload (info + middle68) ----
_BS_DATA_SYNC = bitarray('0' * 48)
_BS_DATA_SYNC[:] = bitarray(bin(0xdff57d75df5d)[2:].zfill(48))   # ETSI BS-sourced DATA sync (downlink)

def block_to_payload(block12, vseq, downlink=True):
    info196 = encode_block(block12)            # 196 on-air info bits
    mid = _middle68(TPL[vseq])                 # [slottype10][sync48][slottype10] (MS sync from captured uplink)
    if downlink:
        mid = mid[0:10] + _BS_DATA_SYNC + mid[58:68]   # swap MS->BS sourced data sync
    frame = info196[0:98] + mid + info196[98:196]   # 264 bits
    assert len(frame) == 264
    return frame.tobytes()                      # 33 bytes

# ---- data header (vseq=6) 12 info bytes ----
def build_data_header(src_id, dst_id, n_blocks, pad):
    # mirror radio's unconfirmed data header: byte0=0x02, byte1 SAP/pad, dst, src,
    # byte8 = 0x80|BTF, byte9=0x00, then CRC16
    b = bytearray(12)
    b[0] = 0x02
    b[1] = 0x40 | (pad & 0x0f)        # SAP nibble 4 + pad octet count (best-effort)
    b[2:5] = dst_id.to_bytes(3,'big') # dst = radio
    b[5:8] = src_id.to_bytes(3,'big') # src = gateway
    b[8] = 0x80 | (n_blocks & 0x7f)
    b[9] = 0x00
    crc = crc16_hdr(bytes(b[0:10]))
    b[10] = (crc >> 8) & 0xff; b[11] = crc & 0xff
    return bytes(b)

# ======================= OFFLINE SELF-TEST =======================
if __name__ == '__main__':
    # Build ACK: gateway(213999) -> radio(7140023), UDP 4005, payload 00 02 bf 01
    ack_ip = build_ip_udp(GW_ID, RADIO_ID, 4005, 4005, bytes.fromhex("0002bf01"), ip_id=0x0001)
    print("ACK IP datagram (%d B): %s" % (len(ack_ip), ack_ip.hex()))
    # verify our own IP/UDP checksums
    print("  IP ck valid:", _ones16(ack_ip[:20]) == 0, " UDP len:", (ack_ip[24]<<8)|ack_ip[25])
    blocks, nb = build_blocks(ack_ip)
    pad = nb*12 - 4 - len(ack_ip)
    print("blocks=%d pad=%d crc=%s" % (nb, pad, crc32_trbo(ack_ip).to_bytes(4,'big').hex()))

    # header
    hdr = build_data_header(GW_ID, RADIO_ID, nb, pad)
    print("data header: %s (crc16=%04x)" % (hdr.hex(), (hdr[10]<<8)|hdr[11]))

    # ROUND-TRIP: build on-air payloads, decode them back like the gateway, reassemble
    recovered = b""
    for i, blk in enumerate(blocks):
        pay = block_to_payload(blk, 7)             # 33 bytes
        # gateway decode path: to_bits(pay) -> del[98:166] -> decode_full -> 12 bytes
        bits = bitarray(dmrdecode.to_bits(pay)); del bits[98:166]
        back = decode_full(bits).tobytes()[:12]
        ok = back == blk
        recovered += back
        print("  block %d roundtrip %s" % (i, "OK" if ok else "FAIL %s vs %s"%(back.hex(),blk.hex())))
    rec_ip = recovered[:len(ack_ip)]
    rec_crc = recovered[nb*12-4:nb*12]
    print("recovered IP == built IP:", rec_ip == ack_ip)
    print("recovered CRC:", rec_crc.hex(), "expected:", crc32_trbo(ack_ip).to_bytes(4,'big').hex())
    # decode header roundtrip
    hpay = block_to_payload(hdr, 6)
    bits = bitarray(dmrdecode.to_bits(hpay)); del bits[98:166]
    hback = decode_full(bits).tobytes()[:12]
    print("header roundtrip:", "OK" if hback==hdr else "FAIL", hback.hex())

# ======================= DMRD/HBP framing + sequence =======================
import os, struct

DMRD = b'DMRD'

def build_dmrd(seq, rf_src, dst_id, peer_id, vseq, stream_id, slot=2, ctype_unit=True, ftype=2, payload33=b"\x00"*33):
    bits = (0x80 if slot==2 else 0x00) | (0x40 if ctype_unit else 0x00) | ((ftype & 0x3) << 4) | (vseq & 0x0f)
    frame = (DMRD + bytes([seq & 0xff])
             + rf_src.to_bytes(3,'big') + dst_id.to_bytes(3,'big') + peer_id.to_bytes(4,'big')
             + bytes([bits]) + stream_id + payload33 + b"\x00\x00")   # 55 bytes (mirror radio)
    assert len(frame) == 55, len(frame)
    return frame

def build_ack_sequence(src_id=GW_ID, dst_id=RADIO_ID, peer_id=GW_ID, ip_id=0x0001):
    """Ordered DMRD frames for the ARS ACK: data header (vseq6) + N data blocks (vseq7).
    Preambles (vseq3) intentionally omitted in v1 (master regenerates PHY); add if needed."""
    ack_ip = build_ip_udp(src_id, dst_id, 4005, 4005, bytes.fromhex("0002bf01"), ip_id)
    blocks, nb = build_blocks(ack_ip)
    pad = nb*12 - 4 - len(ack_ip)
    hdr = build_data_header(src_id, dst_id, nb, pad)
    sid = os.urandom(4)
    frames = []
    seq = 0
    frames.append(build_dmrd(seq, src_id, dst_id, peer_id, 6, sid, payload33=block_to_payload(hdr, 6))); seq += 1
    for blk in blocks:
        frames.append(build_dmrd(seq, src_id, dst_id, peer_id, 7, sid, payload33=block_to_payload(blk, 7))); seq += 1
    return frames

def _selftest_frames():
    frames = build_ack_sequence()
    print("\n=== ACK DMRD sequence: %d frames ===" % len(frames))
    for i, f in enumerate(frames):
        seq=f[4]; rf=int.from_bytes(f[5:8],'big'); ds=int.from_bytes(f[8:11],'big')
        pe=int.from_bytes(f[11:15],'big'); b=f[15]
        print("  f%d len=%d seq=%d src=%d dst=%d peer=%d bits=%02x(slot%d %s ftype%d vseq%d) sid=%s"
              % (i, len(f), seq, rf, ds, pe, b, 2 if b&0x80 else 1, 'unit' if b&0x40 else 'grp',
                 (b&0x30)>>4, b&0xf, f[16:20].hex()))
    return frames

if __name__ == '__main__':
    _selftest_frames()

# ===== CSBK preambles (added 2026-06-05 after live capture showed ACK delivered OTA but w/o preambles) =====
def crc16_csbk(data):
    c = 0
    for byte in data:
        c ^= byte << 8
        for _ in range(8):
            c = ((c << 1) ^ 0x1021) & 0xffff if (c & 0x8000) else (c << 1) & 0xffff
    return c ^ 0x5A5A          # CSBK mask (empirical, matches captured radio preambles)

def build_preamble(src_id, dst_id, btf):
    # Preamble CSBK: bd 00 80 <BTF> <dst:3> <src:3> <crc16>; dst=data destination, src=data source
    b = bytearray(12)
    b[0] = 0xbd; b[1] = 0x00; b[2] = 0x80; b[3] = btf & 0xff
    b[4:7] = dst_id.to_bytes(3, 'big')
    b[7:10] = src_id.to_bytes(3, 'big')
    crc = crc16_csbk(bytes(b[0:10]))
    b[10] = (crc >> 8) & 0xff; b[11] = crc & 0xff
    return bytes(b)

def build_ack_sequence(src_id=GW_ID, dst_id=RADIO_ID, peer_id=GW_ID, ip_id=0x0001, n_pre=16):
    """ACK with CSBK preambles + data header + data blocks (mimics the radio's own TX)."""
    ack_ip = build_ip_udp(src_id, dst_id, 4005, 4005, bytes.fromhex("0002bf01"), ip_id)
    blocks, nb = build_blocks(ack_ip)
    pad = nb*12 - 4 - len(ack_ip)
    hdr = build_data_header(src_id, dst_id, nb, pad)
    sid = os.urandom(4)
    total_after = 1 + nb            # header + data blocks following the last preamble
    frames = []; seq = 0
    for i in range(n_pre):
        btf = (n_pre - 1 - i) + total_after
        pre = build_preamble(src_id, dst_id, btf)
        frames.append(build_dmrd(seq, src_id, dst_id, peer_id, 3, sid, payload33=block_to_payload(pre, 3))); seq += 1
    frames.append(build_dmrd(seq, src_id, dst_id, peer_id, 6, sid, payload33=block_to_payload(hdr, 6))); seq += 1
    for blk in blocks:
        frames.append(build_dmrd(seq, src_id, dst_id, peer_id, 7, sid, payload33=block_to_payload(blk, 7))); seq += 1
    return frames
