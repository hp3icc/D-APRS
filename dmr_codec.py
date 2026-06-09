#!/usr/bin/env python3
"""
dmr_codec.py - DMR rate-1/2 BPTC block codec for the Motorola APRS gateway TX path.

VALIDATED OFFLINE (2026-06-05): encode_block() -> the gateway's decode_full()
round-trips byte-identical against the real captured ARS registration of radio
7140023. This is the hardest piece of the Motorola TX (ARS ACK / LRRP request)
and it is proven working.

  on-air 196-bit BPTC matrix  ==  interleave_19696(encode_19696(12_bytes))
  decode_full(on-air matrix)  ==  original 12 bytes

Remaining for full TX (see memory daprs-aprs-gateway):
  - packet CRC-32 variant (UNRESOLVED: not a standard CRC32 over the decoded
    bytes; needs more complete captured (data,crc) pairs to crack)
  - data header (vseq=6) + CSBK preamble (vseq=3) construction
  - DMRD/HBP framing (slot type + sync re-insertion at bits[98:166]) + send_master()
"""
from bitarray import bitarray
from dmr_utils3 import bptc

# --- info-bit layout used by the D-APRS gateway (gps_data.py decode_full) ---
_IDX = [
    [136,121,106,91,76,61,46,31],
    [152,137,122,107,92,77,62,47,32,17,2],
    [123,108,93,78,63,48,33,18,3,184,169],
    [94,79,64,49,34,19,4,185,170,155,140],
    [65,50,35,20,5,186,171,156,141,126,111],
    [36,21,6,187,172,157,142,127,112,97,82],
    [7,188,173,158,143,128,113,98,83],
    [68,53,174,159,144,129,114,99,84,69,54,39],
    [24,145,130,115,100,85,70,55,40,25,10,191],
]

def decode_full(matrix196):
    """196-bit on-air BPTC matrix -> 96 info bits (gateway-compatible)."""
    b = bitarray(endian='big')
    for row in _IDX:
        b.extend([matrix196[i] for i in row])
    return b

def encode_block(twelve_bytes):
    """12 bytes of user data -> 196-bit on-air BPTC matrix (rate-1/2, deinterleaved
    then interleaved exactly as the radio expects)."""
    assert len(twelve_bytes) == 12
    return bptc.interleave_19696(bptc.encode_19696(twelve_bytes))

def decode_block(matrix196):
    """196-bit on-air BPTC matrix -> 12 bytes (inverse of encode_block)."""
    return decode_full(matrix196).tobytes()[:12]

if __name__ == '__main__':
    # Self-test against the captured ARS registration blocks
    BLOCKS = ["4501002a98050000401192a7","0c6cf2b70d0343ef0fa50fa5",
              "00160567000cf02007373134","30303233000000003feba58f"]
    ok = True
    for h in BLOCKS:
        blk = bytes.fromhex(h)
        rt = decode_block(encode_block(blk))
        ok = ok and rt == blk
        print("%s -> %s %s" % (h, rt.hex(), "OK" if rt == blk else "FAIL"))
    print("SELF-TEST:", "PASS" if ok else "FAIL")
