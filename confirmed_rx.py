#!/usr/bin/env python3
"""
confirmed_rx.py - RX de DATOS CONFIRMED del radio (descubierto 2026-06-08: con
'Data Call Confirmed' ON el radio manda ARS/LRRP como confirmed rate-3/4 vseq=8 y
pide ACK de capa de enlace). Decodifica header (BPTC) + bloques rate-1/2 (vseq7) o
rate-3/4 Trellis (vseq8) via ok-dmrlib, reensambla el IP datagram, y construye el
Response PDU (ACK de enlace) para que el radio deje de retransmitir.
"""
from bitarray import bitarray
from dmr_utils3 import decode as dmrdecode
from dmr_codec import decode_full
import motorola_tx as tx
from okdmr.dmrlib.etsi.layer2.burst import Burst
from okdmr.dmrlib.etsi.layer2.elements.burst_types import BurstTypes
from okdmr.dmrlib.etsi.layer2.pdu.rate34_data import Rate34Data, Rate34DataTypes
from okdmr.dmrlib.etsi.layer2.pdu.rate12_data import Rate12Data, Rate12DataTypes
from okdmr.dmrlib.etsi.layer2.pdu.data_header import DataHeader
from okdmr.dmrlib.etsi.layer2.elements.data_packet_formats import DataPacketFormats
from okdmr.dmrlib.etsi.layer2.elements.sap_identifier import SAPIdentifier
from okdmr.dmrlib.etsi.layer2.elements.full_message_flag import FullMessageFlag
import os


def decode_header(payload33):
    b = bitarray(dmrdecode.to_bits(payload33)); del b[98:166]
    hb = decode_full(b).tobytes()[:12]
    hbits = bitarray(); hbits.frombytes(hb)
    try:
        return DataHeader.from_bits(hbits)
    except Exception:
        return None


def decode_rate34_data(payload33, last):
    bits = bitarray(); bits.frombytes(payload33)
    b = Burst.from_bits(bits, BurstTypes.DataAndControl)
    raw = b.extract_data()
    pt = Rate34DataTypes.ConfirmedLastBlock if last else Rate34DataTypes.Confirmed
    rb = Rate34Data.from_bits_typed(raw.as_bits(), pt)
    return rb.data


def decode_rate12_data(payload33, last, confirmed):
    b = bitarray(dmrdecode.to_bits(payload33)); del b[98:166]
    raw12 = decode_full(b).tobytes()[:12]
    if not confirmed:
        return raw12 if not last else raw12  # unconfirmed: 12 oct (last incl crc32)
    bits = bitarray(); bits.frombytes(raw12)
    pt = Rate12DataTypes.ConfirmedLastBlock if last else Rate12DataTypes.Confirmed
    rb = Rate12Data.from_bits_typed(bits, pt)
    return rb.data


class ConfirmedAssembler:
    """Feed (vseq, payload33); devuelve (ip_datagram, header) al completar, o None."""
    def __init__(self):
        self.hdr = None
        self.confirmed = False
        self.btf = 0
        self.nsq = 0
        self.blocks = []
        self.src = self.dst = None

    def feed(self, vseq, payload33):
        if vseq == 6:
            h = decode_header(payload33)
            self.hdr = h; self.blocks = []
            if h is not None:
                self.confirmed = (h.data_packet_format == DataPacketFormats.DataPacketConfirmed)
                self.btf = getattr(h, 'blocks_to_follow', 0) or 0
                self.nsq = getattr(h, 'send_sequence_number', 0) or 0
                self.src = getattr(h, 'llid_source', None)
                self.dst = getattr(h, 'llid_destination', None)
            return None
        if vseq in (7, 8) and self.hdr is not None:
            last = (len(self.blocks) + 1 >= self.btf) if self.btf else False
            try:
                if vseq == 8:
                    data = decode_rate34_data(payload33, last)
                else:
                    data = decode_rate12_data(payload33, last, self.confirmed)
            except Exception:
                return None
            self.blocks.append(data)
            user = b"".join(self.blocks)
            if len(user) >= 4 and user[0] == 0x45:
                iplen = (user[2] << 8) | user[3]
                if len(user) >= iplen:
                    return (user[:iplen], self.hdr)
        return None


def build_response_ack(radio_id, gw_id, nsq=0):
    """Response PDU (ACK de capa de enlace). Clavado a Brandmeister (capturado 2026-06-08):
    full_message_flag=SubsequentTry (byte8=0x00, NO FirstTry) y response_status = N(S) del
    radio (la secuencia del mensaje que se ACKea: 0,1,2,3...). class0/type1 = ACK positivo."""
    dh = DataHeader(dpf=DataPacketFormats.ResponsePacket,
                    sap_identifier=SAPIdentifier.IP_PacketData,
                    llid_destination=radio_id, llid_source=gw_id,
                    full_message_flag=FullMessageFlag.SubsequentTry,
                    blocks_to_follow=0, response_class=0, response_type=1,
                    response_status=(nsq & 0x7))
    return dh.as_bytes()


def build_response_ack_frames(radio_id, gw_id, peer_id, nsq=0, n_pre=1):
    """Tramas DMRD del ACK de enlace: preambulos + 1 header Response (sin bloques).
    n_pre=1 como BM (mas preambulos DESBORDAN la cola RF del modem -> 'overflow')."""
    hdr = build_response_ack(radio_id, gw_id, nsq)
    sid = os.urandom(4)
    frames = []; seq = 0
    for i in range(n_pre):
        btf = (n_pre - 1 - i) + 1
        pre = tx.build_preamble(gw_id, radio_id, btf)
        frames.append(tx.build_dmrd(seq, gw_id, radio_id, peer_id, 3, sid,
                                    payload33=tx.block_to_payload(pre, 3))); seq += 1
    frames.append(tx.build_dmrd(seq, gw_id, radio_id, peer_id, 6, sid,
                                payload33=tx.block_to_payload(hdr, 6))); seq += 1
    return frames


if __name__ == '__main__':
    HDR = "0f550a3f651274cbe7f379a3c58dff57d75df5d33af4304433e569e38282bfb4a5"
    BLKS = [
        "2b320a2f88c4b20226a22ab0463dff57d75df5da8723e8cf2e8093b8aa32752bd1",
        "2f30b612742b2222d1fc2d31063dff57d75df5da86b0b1507e7f97d4202da332df",
        "2f46065222d6fd747b2b22d1463dff57d75df5da841ff6fb622274b4421b72279b",
    ]
    asm = ConfirmedAssembler()
    print("feed header:", asm.feed(6, bytes.fromhex(HDR)))
    print("  confirmed=%s btf=%d src=%s dst=%s" % (asm.confirmed, asm.btf, asm.src, asm.dst))
    res = None
    for h in BLKS:
        res = asm.feed(8, bytes.fromhex(h)) or res
    ip, hdr = res
    print("REASSEMBLED IP (%dB): %s" % (len(ip), ip.hex()))
    print("  src=%s dst=%s udp_dport=%d payload=%s" % (
        ip[12:16].hex(), ip[16:20].hex(), (ip[22] << 8) | ip[23], ip[28:].hex()))
    ack = build_response_ack(7140023, 213999)
    print("Response ACK header: %s" % ack.hex())
    frames = build_response_ack_frames(7140023, 213999, 213999)
    print("ACK frames: %d (%d preambulos + 1 header)" % (len(frames), sum(1 for f in frames if (f[15] & 0xf) == 3)))
