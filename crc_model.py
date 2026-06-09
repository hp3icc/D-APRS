#!/usr/bin/env python3
"""MOTOTRBO/ETSI DMR data packet CRC-32 (B.3.9). CRACKED & validated 24/24 real
codewords (radios 7140023 + 7140001) + matches OK-DMR/ok-dmrlib reference.
Algorithm: CRC-32 (poly 0x04C11DB7, MSB-first, init 0, no xor) over the PAIRWISE
BYTE-SWAPPED [user data + pad octets]; result stored LITTLE-ENDIAN as the last 4
octets of the last block."""

def _byteswap_pairs(data):
    d = bytearray(data); last = b''
    if len(d) % 2: last = bytes([d[-1]]); d = d[:-1]
    d[0::2], d[1::2] = d[1::2], d[0::2]
    return bytes(d) + last

def crc32_dmr_int(data):
    """CRC-32 int over [user data + pad] (data must already include pad octets)."""
    reg = 0; poly = 0x04C11DB7
    for byte in _byteswap_pairs(data):
        for i in range(7, -1, -1):              # MSB-first
            bit = (byte >> i) & 1; msb = (reg >> 31) & 1
            reg = (reg << 1) & 0xFFFFFFFF
            if msb ^ bit: reg ^= poly
    return reg

def crc32_dmr_bytes(data):
    """The 4 CRC octets as they appear in the packet (little-endian)."""
    return crc32_dmr_int(data).to_bytes(4, 'little')

# back-compat names used elsewhere
def crc32_trbo(data):
    return crc32_dmr_int(data)

if __name__ == '__main__':
    import json
    cw = json.load(open('/tmp/cw_fec.json'))
    ok = sum(crc32_dmr_bytes(bytes.fromhex(c['ip']) + b'\x00\x00') == bytes.fromhex(c['crc']) for c in cw)
    print("self-test vs %d real codewords (IP+pad): %d/%d %s" % (len(cw), ok, len(cw), "PASS" if ok==len(cw) else "FAIL"))
