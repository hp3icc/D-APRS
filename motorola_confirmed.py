#!/usr/bin/env python3
"""
motorola_confirmed.py - Variante CONFIRMED data (DPF=3) para ARS ACK y LRRP request.
Descubierto por el codeplug BM (PDF DK5RAS): canal con 'Data Call Confirmed'=ON y
'Location Data Delivery Mode'=Follow Data Call Confirmed -> el radio espera datos
confirmed. Cada bloque rate-1/2 confirmed = [DBSN:7][CRC9:9 LE][data:80] (10 oct),
ultimo = [DBSN:7][CRC9:9][data:48][CRC32:32] (6 oct + crc32). Cabecera DPF confirmed,
A=1. Construido con ok-dmrlib (ETSI). Reusa BPTC/preambulos/framing de motorola_tx.
"""
import os
from bitarray import bitarray
import motorola_tx as tx
from crc_model import crc32_dmr_bytes
from okdmr.dmrlib.etsi.crc.crc32 import CRC32
from okdmr.dmrlib.etsi.layer2.pdu.rate12_data import Rate12Data, Rate12DataTypes
from okdmr.dmrlib.etsi.layer2.pdu.data_header import DataHeader
from okdmr.dmrlib.etsi.layer2.elements.data_packet_formats import DataPacketFormats
from okdmr.dmrlib.etsi.layer2.elements.sap_identifier import SAPIdentifier
from okdmr.dmrlib.etsi.layer2.elements.full_message_flag import FullMessageFlag
from okdmr.dmrlib.etsi.layer2.elements.resynchronize_flag import ResynchronizeFlag
from okdmr.dmrlib.etsi.layer2.elements.fragment_sequence_number import FragmentSequenceNumber


def _crc32_int(user_bytes):
    try:
        return CRC32.calculate(user_bytes)
    except Exception:
        ub = bitarray(); ub.frombytes(user_bytes)
        return CRC32.calculate(ub)


def build_confirmed_blocks(ip_datagram):
    """Devuelve (lista de payloads de 12B/96b, nb, pad). Confirmed rate-1/2:
    nb-1 bloques de 10 oct + 1 ultimo de 6 oct + CRC32."""
    nb = 1
    while 10 * nb - 4 < len(ip_datagram):
        nb += 1
    U = 10 * nb - 4
    pad = U - len(ip_datagram)
    user = ip_datagram + b"\x00" * pad
    # CRC-32 en el ORDEN DE AIRE del radio (little-endian, validado con crc_model);
    # ok-dmrlib lo serializa con to_bytes(4,'big'), asi que le pasamos el int cuyo
    # BE == nuestros bytes LE -> el aire lleva exactamente crc32_dmr_bytes(user).
    crc32 = int.from_bytes(crc32_dmr_bytes(user), "big")
    payloads = []
    for i in range(nb):
        dbsn = i
        if i < nb - 1:
            chunk = user[i*10:(i+1)*10]
            blk = Rate12Data(data=chunk, packet_type=Rate12DataTypes.Confirmed, dbsn=dbsn)
        else:
            chunk = user[i*10:i*10+6]
            blk = Rate12Data(data=chunk, packet_type=Rate12DataTypes.ConfirmedLastBlock,
                             dbsn=dbsn, crc32=crc32)
        blk.crc9 = blk.calculate_crc9()
        payloads.append(blk.as_bits().tobytes())     # 12 bytes (96 bits)
    return payloads, nb, pad


def build_confirmed_header(src_id, dst_id, nb, pad):
    dh = DataHeader(
        dpf=DataPacketFormats.DataPacketConfirmed,
        is_group=False,
        is_response_requested=True,
        pad_octet_count=pad,
        sap_identifier=SAPIdentifier.IP_PacketData,
        llid_destination=dst_id,
        llid_source=src_id,
        full_message_flag=FullMessageFlag.FirstTryToCompletePacket,
        blocks_to_follow=nb,
        resynchronize_flag=ResynchronizeFlag.SyncSeqNumberWithDataHeader,
        send_sequence_number=0,
        fragment_sequence_number=FragmentSequenceNumber(
            FragmentSequenceNumber.SINGLE_CONFIRMED_FRAGMENT_VALUE),
    )
    return dh.as_bytes()      # 12 bytes incl CRC-16


def build_confirmed_sequence(ip_datagram, src_id, dst_id, peer_id, n_pre=2):
    """DMRD frames: preambulos CSBK + header confirmed (vseq6) + bloques confirmed (vseq7)."""
    blocks, nb, pad = build_confirmed_blocks(ip_datagram)
    hdr = build_confirmed_header(src_id, dst_id, nb, pad)
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
    for blk in blocks:
        frames.append(tx.build_dmrd(seq, src_id, dst_id, peer_id, 7, sid,
                                    payload33=tx.block_to_payload(blk, 7))); seq += 1
    return frames


def build_confirmed_ack_sequence(src_id=tx.GW_ID, dst_id=7140023, peer_id=tx.GW_ID, n_pre=2):
    # respuesta ARS clavada a Brandmeister: bf01 + sufijo 1080 (capturado 2026-06-08)
    ip = tx.build_ip_udp(src_id, dst_id, 4005, 4005, bytes.fromhex("0002bf011080"), ip_id=0x0001)
    return build_confirmed_sequence(ip, src_id, dst_id, peer_id, n_pre)


def build_confirmed_lrrp_sequence(dst_id, lrrp_payload, src_id=tx.GW_ID, peer_id=tx.GW_ID, n_pre=2):
    ip = tx.build_ip_udp(src_id, dst_id, 4001, 4001, lrrp_payload, ip_id=0x0002)
    return build_confirmed_sequence(ip, src_id, dst_id, peer_id, n_pre)


# ----------------------- SELF-TEST: round-trip via ok-dmrlib -----------------------
if __name__ == '__main__':
    from dmr_utils3 import decode as dmrdecode
    from dmr_codec import decode_full

    ip = tx.build_ip_udp(tx.GW_ID, 7140023, 4005, 4005, bytes.fromhex("0002bf01"), 0x0001)
    print("ACK IP (%dB): %s" % (len(ip), ip.hex()))
    blocks, nb, pad = build_confirmed_blocks(ip)
    print("confirmed: nb=%d pad=%d, %d block payloads" % (nb, pad, len(blocks)))
    hdr = build_confirmed_header(tx.GW_ID, 7140023, nb, pad)
    print("confirmed header (%dB): %s" % (len(hdr), hdr.hex()))
    # re-parsear header con ok-dmrlib
    hb = bitarray(); hb.frombytes(hdr)
    print("  ->", repr(DataHeader.from_bits(hb))[:150])
    # re-parsear cada bloque confirmed via ok-dmrlib y validar CRC9
    for i, p in enumerate(blocks):
        pb = bitarray(); pb.frombytes(p)
        last = (i == nb-1)
        pt = Rate12DataTypes.ConfirmedLastBlock if last else Rate12DataTypes.Confirmed
        r = Rate12Data.from_bits_typed(pb, pt)
        ok = (r.crc9 == r.calculate_crc9())
        print("  block %d dbsn=%d crc9_ok=%s%s" % (i, r.dbsn, ok, " (LAST,crc32=%08x)" % r.crc32 if last else ""))
    # round-trip BPTC: header+bloques por el camino del gateway
    def dec(p33):
        b = bitarray(dmrdecode.to_bits(p33)); del b[98:166]
        return decode_full(b).tobytes()[:12]
    frames = build_confirmed_sequence(ip, tx.GW_ID, 7140023, tx.GW_ID, n_pre=2)
    rec = []
    for f in frames:
        if (f[15] & 0xf) == 7:
            rec.append(dec(f[20:53]))
    print("bloques recuperados BPTC==originales:", rec == blocks)
