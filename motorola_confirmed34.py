#!/usr/bin/env python3
"""
motorola_confirmed34.py - Downlink confirmed en RATE-3/4 (Trellis), igual que Brandmeister
(capturado 2026-06-08: BM manda ACK app/LRRP en rate-3/4, btf=3 pad=10; nosotros íbamos
rate-1/2). Bloque confirmed rate-3/4 = [DBSN:7][CRC9:9][data:128] (16 oct), último 12 oct
+ CRC32. Trellis34.encode(144)->196 (validado round-trip). Reusa header BPTC (dt6) y
preámbulos/framing de motorola_tx/motorola_confirmed.
"""
import os
from bitarray import bitarray
from dmr_utils3 import decode as dmrdecode
import motorola_tx as tx
import motorola_confirmed as mc
from crc_model import crc32_dmr_bytes
from okdmr.dmrlib.etsi.fec.trellis import Trellis34
from okdmr.dmrlib.etsi.layer2.pdu.rate34_data import Rate34Data, Rate34DataTypes

# plantilla rate-3/4 (burst real vseq=8 del radio) para slot-type + sync
TPL34 = "2b320a2f88c4b20226a22ab0463dff57d75df5da8723e8cf2e8093b8aa32752bd1"
_BS_DATA_SYNC = bitarray(bin(0xdff57d75df5d)[2:].zfill(48))   # BS-sourced DATA sync (downlink)


def _mid68_34(downlink=True):
    bits = bitarray(dmrdecode.to_bits(bytes.fromhex(TPL34)))
    mid = bits[98:166]
    if downlink:
        mid = mid[0:10] + _BS_DATA_SYNC + mid[58:68]
    return mid


def block34_to_payload(payload144, downlink=True):
    enc196 = Trellis34.encode(payload144)            # 196 bits info on-air
    mid = _mid68_34(downlink)
    frame = enc196[0:98] + mid + enc196[98:196]      # 264 bits
    assert len(frame) == 264, len(frame)
    return frame.tobytes()


def build_confirmed_blocks34(ip_datagram):
    nb = 1
    while 16 * nb - 4 < len(ip_datagram):
        nb += 1
    U = 16 * nb - 4
    pad = U - len(ip_datagram)
    user = ip_datagram + b"\x00" * pad
    crc32 = int.from_bytes(crc32_dmr_bytes(user), "big")   # LE en aire (ok-dmrlib int2ba BE)
    payloads = []
    for i in range(nb):
        if i < nb - 1:
            blk = Rate34Data(data=user[i*16:(i+1)*16], packet_type=Rate34DataTypes.Confirmed, dbsn=i)
        else:
            blk = Rate34Data(data=user[i*16:i*16+12], packet_type=Rate34DataTypes.ConfirmedLastBlock,
                             dbsn=i, crc32=crc32)
        blk.crc9 = blk.calculate_crc9()
        payloads.append(blk.as_bits())               # 144 bits
    return payloads, nb, pad


def build_confirmed_sequence34(ip_datagram, src_id, dst_id, peer_id, n_pre=2):
    blocks, nb, pad = build_confirmed_blocks34(ip_datagram)
    hdr = mc.build_confirmed_header(src_id, dst_id, nb, pad)   # cabecera BPTC (dt6)
    sid = os.urandom(4)
    frames = []; seq = 0
    total_after = 1 + nb
    for i in range(n_pre):
        btf = (n_pre - 1 - i) + total_after
        pre = tx.build_preamble(src_id, dst_id, btf)
        frames.append(tx.build_dmrd(seq, src_id, dst_id, peer_id, 3, sid,
                                    payload33=tx.block_to_payload(pre, 3))); seq += 1
    frames.append(tx.build_dmrd(seq, src_id, dst_id, peer_id, 6, sid,
                                payload33=tx.block_to_payload(hdr, 6))); seq += 1
    for p144 in blocks:
        frames.append(tx.build_dmrd(seq, src_id, dst_id, peer_id, 8, sid,
                                    payload33=block34_to_payload(p144))); seq += 1
    return frames


def build_confirmed_ack_sequence34(src_id=tx.GW_ID, dst_id=7140023, peer_id=tx.GW_ID, n_pre=2):
    ip = tx.build_ip_udp(src_id, dst_id, 4005, 4005, bytes.fromhex("0002bf011080"), ip_id=0x0001)
    return build_confirmed_sequence34(ip, src_id, dst_id, peer_id, n_pre)


def build_confirmed_lrrp_sequence34(dst_id, lrrp_payload, src_id=tx.GW_ID, peer_id=tx.GW_ID, n_pre=2):
    ip = tx.build_ip_udp(src_id, dst_id, 4001, 4001, lrrp_payload, ip_id=0x0002)
    return build_confirmed_sequence34(ip, src_id, dst_id, peer_id, n_pre)


if __name__ == '__main__':
    from okdmr.dmrlib.etsi.layer2.burst import Burst
    from okdmr.dmrlib.etsi.layer2.elements.burst_types import BurstTypes
    ip = tx.build_ip_udp(tx.GW_ID, 7140023, 4005, 4005, bytes.fromhex("0002bf011080"), 0x0001)
    print("ACK IP (%dB): %s" % (len(ip), ip.hex()))
    blocks, nb, pad = build_confirmed_blocks34(ip)
    print("rate-3/4: nb=%d pad=%d (BM: nb=3 pad=10)" % (nb, pad))
    frames = build_confirmed_sequence34(ip, tx.GW_ID, 7140023, tx.GW_ID, n_pre=2)
    # round-trip: decodificar los bursts dt8 con ok-dmrlib (independiente) y reensamblar
    user = b""
    for f in frames:
        if (f[15] & 0xf) == 8:
            bits = bitarray(); bits.frombytes(f[20:53])
            b = Burst.from_bits(bits, BurstTypes.DataAndControl)
            last = (len(user) >= 16*(nb-1))
            from okdmr.dmrlib.etsi.layer2.pdu.rate34_data import Rate34Data as R34, Rate34DataTypes as RT
            rb = R34.from_bits_typed(b.extract_data().as_bits(), RT.ConfirmedLastBlock if last else RT.Confirmed)
            user += rb.data
    rec_ip = user[:len(ip)]
    print("data_type del burst dt8:", b.data_type)
    print("IP reensamblado == original:", rec_ip == ip)
