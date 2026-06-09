#!/usr/bin/env python3
"""
ars_responder.py - RX side: reensambla la data DMR del radio y despacha por puerto UDP:
  - 4005 (ARS HELLO op 0x3820) -> manda el ARS ACK; y si LRRP esta ARMADO, manda la
    peticion LRRP TriggeredStart periodica a ese radio.
  - 4001 (LRRP report 0x0D/0x07) -> decodifica con lrrp_rx.parselrrp y lo loguea
    (y opcionalmente lo empuja a APRS via callback).
Conducido desde gps_data.py dmrd_received: on_frame(vseq, stream_id, _data[20:53], send_master).

ARMADO LRRP: crear /tmp/lrrp_arm con el intervalo en segundos (p.ej. `echo 120 >/tmp/lrrp_arm`).
Sin ese fichero NO se transmite ninguna peticion LRRP (solo el ARS ACK de siempre).
"""
import os, time
from bitarray import bitarray
from dmr_utils3 import decode as dmrdecode
from dmr_codec import decode_full
import motorola_tx as tx
import lrrp_rx
import motorola_lrrp_tx as ltx
try:
    import motorola_confirmed as mc
except Exception:
    mc = None
try:
    import motorola_confirmed34 as mc34   # downlink RATE-3/4 (como BM)
except Exception:
    mc34 = None
try:
    import confirmed_rx as crx
except Exception:
    crx = None

ARM_FILE = '/tmp/lrrp_arm'
CONFIRMED_FILE = '/tmp/lrrp_confirmed'   # si existe -> ACK/LRRP como datos CONFIRMED (DPF=3)

def _confirmed():
    # Use CONFIRMED data (DPF=3, rate-3/4) for the ACK/LRRP, which is what real
    # MOTOTRBO radios expect to complete ARS registration. Default ON so a fresh
    # server works end-to-end without any /tmp flag file; override with
    # MOTO_CONFIRMED=0 (unconfirmed) or it is also forced on by the flag file. (C31AG)
    if mc is None:
        return False
    env = os.environ.get('MOTO_CONFIRMED')
    if env is not None:
        return env not in ('0', '', 'false', 'False', 'no', 'off')
    return True

def _decode_block(payload33):
    bits = bitarray(dmrdecode.to_bits(payload33))
    del bits[98:166]
    return decode_full(bits).tobytes()[:12]

class ArsResponder:
    def __init__(self, data_id=tx.GW_ID, logger=None, aprs_cb=None, sms_cb=None):
        self.data_id = data_id
        self.log = logger
        self.aprs_cb = aprs_cb              # callback opcional: aprs_cb(radio_id, lrrp_dict)
        self.sms_cb = sms_cb               # callback opcional: sms_cb(radio_id, text) para TMS->process_sms
        self._asm = {}                      # stream_id -> [bloques 12B]
        self._last_lrrp_fire = {}           # radio_id -> ts

    def _info(self, m):
        if self.log: self.log.info('[ARS] ' + m)
        else: print('[ARS] ' + m)

    # ---------- entrada ----------
    def on_frame(self, vseq, stream_id, payload33, send_func):
        try:
            key = bytes(stream_id)
            if crx is not None:
                # camino unificado: maneja rate-1/2 (vseq7) y rate-3/4 confirmed (vseq8)
                if vseq == 6:
                    self._asm[key] = crx.ConfirmedAssembler()
                a = self._asm.get(key)
                if not isinstance(a, crx.ConfirmedAssembler):
                    return
                res = a.feed(vseq, payload33)
                if res:
                    ip, hdr = res
                    self._asm.pop(key, None)
                    radio_id = a.src
                    resp_req = bool(getattr(hdr, 'is_response_requested', False))
                    # 1) ACK de capa de enlace si el radio mando confirmed + pide respuesta
                    if a.confirmed and resp_req and radio_id:
                        ackf = crx.build_response_ack_frames(radio_id, self.data_id, self.data_id, nsq=a.nsq)
                        self._pace_send(ackf, send_func)
                        self._info('Link-layer ACK -> %d (confirmed data recibida, %d bloques, N(S)=%d)'
                                   % (radio_id, a.btf, a.nsq))
                        # BM espacia la respuesta ARS ~5s tras el ACK de enlace -> retrasar el dispatch
                        try:
                            from twisted.internet import reactor
                            reactor.callLater(4.0, self._dispatch_ip, ip, send_func)
                            return
                        except Exception:
                            pass
                    # 2) dispatch por puerto UDP (ARS app response / LRRP decode)
                    self._dispatch_ip(ip, send_func)
                return
            # fallback (sin ok-dmrlib): solo rate-1/2 unconfirmed
            if vseq == 6:
                self._asm[key] = []
                return
            if vseq == 7 and isinstance(self._asm.get(key), list):
                self._asm[key].append(_decode_block(payload33))
                asm = b"".join(self._asm[key])
                if len(asm) >= 4 and asm[0] == 0x45:
                    iplen = (asm[2] << 8) | asm[3]
                    if len(asm) >= iplen:
                        ip = asm[:iplen]
                        self._asm.pop(key, None)
                        self._dispatch_ip(ip, send_func)
        except Exception as e:
            self._info('on_frame exc: %s' % e)

    def _dispatch_ip(self, ip, send_func):
        if len(ip) < 28 or ip[0] != 0x45 or ip[9] != 0x11:
            return
        dport = (ip[22] << 8) | ip[23]
        radio_id = int.from_bytes(ip[13:16], 'big')      # src IP = 0x0C + radioID
        if dport == 4005:
            rid = self.parse_ars_hello(ip)
            if rid:
                self._info('ARS HELLO from %d -> sending ACK' % rid)
                self.send_ack(rid, send_func)
                self._maybe_fire_lrrp(rid, send_func)
        elif dport == 4001:
            self._handle_lrrp(ip, radio_id, send_func)
        elif dport == 4007:
            self._handle_tms(ip, radio_id, send_func)

    # ---------- TMS (text messages: lets users edit their APRS settings) ----------
    @staticmethod
    def _tms_text_run(udp, big_endian):
        """Longest run of UTF-16 printable-ASCII text (char paired with a 0x00).
        big_endian False -> 'XX 00' (UTF-16LE), True -> '00 XX' (UTF-16BE)."""
        best = ''
        i = 0
        n = len(udp)
        while i < n - 1:
            hi = udp[i] if big_endian else udp[i + 1]
            lo = udp[i + 1] if big_endian else udp[i]
            if hi == 0x00 and 0x20 <= lo <= 0x7E:
                chars = []
                j = i
                while j < n - 1:
                    h = udp[j] if big_endian else udp[j + 1]
                    l = udp[j + 1] if big_endian else udp[j]
                    if h == 0x00 and 0x20 <= l <= 0x7E:
                        chars.append(chr(l)); j += 2
                    else:
                        break
                if len(chars) > len(best):
                    best = ''.join(chars)
                i = j
            else:
                i += 1
        return best

    def _decode_tms(self, udp):
        """Extract the text body of a MOTOTRBO TMS message. The @-commands
        (@SSID/@ICON/@COM...) are ASCII, so we pull the longest UTF-16 ASCII run
        and don't depend on the exact TMS header layout."""
        le = self._tms_text_run(udp, False)
        be = self._tms_text_run(udp, True)
        return (le if len(le) >= len(be) else be).strip()

    def _handle_tms(self, ip, radio_id, send_func):
        iplen = (ip[2] << 8) | ip[3]
        udp = ip[28:iplen]
        text = self._decode_tms(udp)
        if not text:
            return
        self._info('TMS from %d: %r' % (radio_id, text))
        if self.sms_cb:
            try:
                self.sms_cb(radio_id, text)
            except Exception as e:
                self._info('sms_cb exc: %s' % e)

    # ---------- ARS ----------
    def parse_ars_hello(self, ip_datagram):
        if len(ip_datagram) < 28 or ip_datagram[0] != 0x45: return None
        if ip_datagram[9] != 0x11: return None
        dport = (ip_datagram[22]<<8)|ip_datagram[23]
        if dport != 4005: return None
        udp_payload = ip_datagram[28: 20 + ((ip_datagram[2]<<8)|ip_datagram[3]) ]
        if len(udp_payload) < 4: return None
        dlen = (udp_payload[0]<<8)|udp_payload[1]
        if dlen != len(udp_payload)-2: return None
        def ber(d, i):
            n=d[i]; v=n&0x7f
            while n&0x80:
                i+=1; n=d[i]; v=v*128+(n&0x7f)
            return v
        opv = ber(udp_payload, 2)
        if opv in (0x3820, 0x3840):
            self._info("ARS op=%04x (%s)" % (opv, "INITIAL/retry" if opv==0x3820 else "PERIODIC/registered"))
            return int.from_bytes(ip_datagram[13:16],'big')
        return None

    def send_ack(self, radio_id, send_func):
        if _confirmed():
            frames = mc34.build_confirmed_ack_sequence34(src_id=self.data_id, dst_id=radio_id, peer_id=self.data_id)
            mode = 'CONFIRMED-r34'
        else:
            frames = tx.build_ack_sequence(src_id=self.data_id, dst_id=radio_id, peer_id=self.data_id)
            mode = 'unconfirmed'
        self._pace_send(frames, send_func)
        self._info('ACK %s queued (%d frames, 60ms paced) to %d' % (mode, len(frames), radio_id))

    # ---------- LRRP ----------
    def _arm_interval(self):
        """Triggered-LRRP request interval (seconds), or None to not send requests.

        Priority: /tmp/lrrp_arm file -> MOTO_LRRP_INTERVAL env -> default 120.
        Default 120 means a fresh server automatically asks each registering radio
        for periodic GPS (triggered mode), so it works out of the box like the
        reference deployment. Set MOTO_LRRP_INTERVAL=0 to disable. (C31AG)"""
        try:
            with open(ARM_FILE) as f:
                txt = f.read().strip()
            return int(txt) if txt else 120
        except Exception:
            pass
        env = os.environ.get('MOTO_LRRP_INTERVAL', '120')
        try:
            iv = int(env)
        except (TypeError, ValueError):
            return 120
        return iv if iv > 0 else None

    def _maybe_fire_lrrp(self, radio_id, send_func):
        interval = self._arm_interval()
        if interval is None:
            return
        now = time.time()
        if now - self._last_lrrp_fire.get(radio_id, 0) < 55:   # no spamear (1/ARS-cycle)
            return
        self._last_lrrp_fire[radio_id] = now
        if _confirmed():
            # peticion LRRP clavada a Brandmeister (capturado 2026-06-08): TriggeredStart,
            # reqID 7fffffff, tags alt(54)/62/dir(57), periodo 0x3c=60s.
            bm_lrrp = bytes.fromhex("090c22047fffffff54625734313c")
            frames = mc34.build_confirmed_lrrp_sequence34(radio_id, bm_lrrp,
                                                          src_id=self.data_id, peer_id=self.data_id)
            mode = 'CONFIRMED-r34'
        else:
            frames, _ = ltx.build_triggered_periodic_frames(
                radio_id, interval_s=interval, src_id=self.data_id, peer_id=self.data_id)
            mode = 'unconfirmed'
        self._info('LRRP ARMADO %s -> TriggeredStart periodico %ss a %d (%d frames)'
                   % (mode, interval, radio_id, len(frames)))
        self._pace_send(frames, send_func)

    def _handle_lrrp(self, ip, radio_id, send_func):
        iplen = (ip[2]<<8)|ip[3]
        udp = ip[28:iplen]
        res = lrrp_rx.parselrrp(udp)
        if 'Latitude' in res:
            self._info('*** LRRP REPORT de %d: lat=%s lon=%s  %s ***'
                       % (radio_id, res.get('Latitude'), res.get('Longitude'), res))
            if self.aprs_cb:
                try: self.aprs_cb(radio_id, res)
                except Exception as e: self._info('aprs_cb exc: %s' % e)
        else:
            self._info('LRRP de %d (sin fix/otro): %s' % (radio_id, res))

    # ---------- util ----------
    def _pace_send(self, frames, send_func):
        # override de SLOT opcional via /tmp/lrrp_slot (contenido "1" o "2") para
        # compensar un remap de slot aguas abajo (adn-server/OpenBridge -> hotspot).
        try:
            with open('/tmp/lrrp_slot') as _sf:
                _s = _sf.read().strip()
            if _s in ('1', '2'):
                _w2 = (_s == '2'); _nf = []
                for _fr in frames:
                    _b = bytearray(_fr)
                    _b[15] = (_b[15] | 0x80) if _w2 else (_b[15] & 0x7f)
                    _nf.append(bytes(_b))
                frames = _nf
        except Exception:
            pass
        # programacion SECUENCIAL global: cada lote empieza tras el anterior (evita
        # solapar varios streams DMR en el mismo slot, que el radio no puede decodificar).
        try:
            from twisted.internet import reactor
            import time
            now = time.time()
            base = max(getattr(self, '_send_until', 0.0) - now, 0.0)
            for i, f in enumerate(frames):
                reactor.callLater(base + i * 0.09, send_func, f)
            self._send_until = now + base + len(frames) * 0.09 + 0.15
        except Exception:
            for f in frames:
                send_func(f)


# ---------------- self-test offline (ARS ACK contra el registro real) ----------------
if __name__ == '__main__':
    HDR = "4f650ace65167233e1836c43c58d5d7f77fd757338c0355033256183828286b4e80000"
    BLKS = [
     "0cbfb16b034d00f4a2c0888205ed5d7f77fd757095b0409c42d30026130141987a0000",
     "7bdb61f88054905c2b9f31ea45ed5d7f77fd7570978c6d534e646547415db3a7700000",
     "04d12a7241641b4323a5112685ed5d7f77fd7570974977d0241360609d042b802c0000",
     "703da14800bc068008531ab205ed5d7f77fd757097e327b308b01fa421103c201f0000",
    ]
    sent = []
    r = ArsResponder()
    sid = b'\xaa\xbb\xcc\xdd'
    r.on_frame(6, sid, bytes.fromhex(HDR), sent.append)
    for h in BLKS:
        r.on_frame(7, sid, bytes.fromhex(h), sent.append)
    print("ARS: frames que enviaria:", len(sent))
    print("LRRP armado?", r._arm_interval())
