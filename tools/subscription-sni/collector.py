#!/usr/bin/env python3
"""Opt-in Linux collector. Only tagged outbound TCP/443 ClientHello copies enter NFLOG."""
import collections
import ctypes as C
import ctypes.util
import errno
import ipaddress
import os
import re
import select
import signal
import sqlite3
import ssl
import subprocess
import sys
import time
from pathlib import Path

DIR = Path('/var/lib/flyx-sni')
GROUP = 24861
TABLE = 'flyx_sni_optin'
KEEP = 30 * 86400
LIMIT = 256  # different names per subscription and minute
VALID = re.compile(r'(?=.{1,253}\Z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z')

def parse(packet):
    if not packet: return
    version = packet[0] >> 4
    if version == 4 and len(packet) >= 20 and packet[9] == 6 and not (int.from_bytes(packet[6:8], 'big') & 0x3fff):
        i = (packet[0] & 15)*4
        if i < 20: return
    elif version == 6 and len(packet) >= 40 and packet[6] == 6: i = 40
    else: return
    if len(packet) < i+20 or packet[i+2:i+4] != b'\x01\xbb': return
    size = (packet[i+12] >> 4)*4
    if size < 20: return
    p = packet[i+size:]
    if len(p)<44 or p[:2]!=b'\x16\x03' or p[5]!=1: return
    names=[]
    ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    def observe(_, name, __):
        if name: names.append(name.lower())
        return ssl.ALERT_DESCRIPTION_HANDSHAKE_FAILURE
    ctx.sni_callback=observe
    src=ssl.MemoryBIO(); src.write(p); conn=ctx.wrap_bio(src,ssl.MemoryBIO(),server_side=True)
    try: conn.do_handshake()
    except ssl.SSLError: pass
    if names and VALID.fullmatch(names[0]):
        try: ipaddress.ip_address(names[0]); return
        except ValueError: return names[0],p[11:43]

class Store:
    def __init__(self, db, ids):
        self.db=db; self.ids=set(ids); self.seen=collections.OrderedDict(); self.pending=collections.Counter(); self.minute=None; self.admitted={}
        db.executescript('PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS hits(sub INTEGER,t INTEGER,host TEXT,n INTEGER,PRIMARY KEY(sub,t,host)) WITHOUT ROWID; CREATE INDEX IF NOT EXISTS hits_time ON hits(t); PRAGMA max_page_count=131072; CREATE TABLE IF NOT EXISTS health(sub INTEGER PRIMARY KEY,seen INTEGER);')

    def add(self, sub, sample, now):
        if sub not in self.ids or not sample: return
        host,nonce=sample; key=(sub,nonce); minute=int(now)//60*60
        while self.seen and (len(self.seen)>=16384 or next(iter(self.seen.values()))<now-60): self.seen.popitem(last=False)
        if key in self.seen: return
        if self.minute!=minute: self.minute=minute; self.admitted={}
        hosts=self.admitted.setdefault(sub,set())
        if host not in hosts and len(hosts)>=LIMIT: return
        hosts.add(host); self.seen[key]=now; self.pending[sub,minute,host]+=1
    def flush(self):
        now=int(time.time())
        with self.db:
            self.db.executemany('INSERT INTO hits VALUES(?,?,?,?) ON CONFLICT(sub,t,host) DO UPDATE SET n=n+excluded.n',((*k,n) for k,n in self.pending.items()))
            self.db.executemany('INSERT OR REPLACE INTO health VALUES(?,?)',((i,now) for i in self.ids))
            self.db.execute('DELETE FROM hits WHERE t<?',(now-KEEP,))
        self.pending.clear()

def run(ids):
    os.umask(0o077); DIR.mkdir(mode=0o700,exist_ok=True)
    lib=C.CDLL(ctypes.util.find_library('netfilter_log') or 'libnetfilter_log.so.1'); ptr=C.c_void_p
    def fn(name, restype, *args):
        f=getattr(lib,'nflog_'+name); f.argtypes=args; f.restype=restype; return f
    cbtype=C.CFUNCTYPE(C.c_int,ptr,ptr,ptr,ptr)
    h=fn('open',ptr)();
    if not h: raise RuntimeError('NFLOG unavailable')
    group=fn('bind_group',ptr,ptr,C.c_uint16)(h,GROUP)
    if not group: raise RuntimeError('NFLOG group unavailable')
    if fn('set_mode',C.c_int,ptr,C.c_uint8,C.c_uint32)(group,2,8192)<0: raise RuntimeError('NFLOG copy mode failed')
    fd=fn('fd',C.c_int,ptr)(h); mark=fn('get_nfmark',C.c_uint32,ptr); payload=fn('get_payload',C.c_int,ptr,C.POINTER(ptr)); handle=fn('handle_packet',C.c_int,ptr,C.c_char_p,C.c_int)
    db=sqlite3.connect(DIR/'stats.sqlite3'); store=Store(db,ids)
    @cbtype
    def recv(_g,_m,data,_u):
        tag=mark(data); sub=tag & 65535
        if tag >> 16==0x4656 and sub in store.ids:
            p=ptr(); n=payload(data,C.byref(p));
            if 0<n<=8192: store.add(sub,parse(C.string_at(p,n)),time.time())
        return 0
    if fn('callback_register',C.c_int,ptr,cbtype,ptr)(group,recv,None)<0: raise RuntimeError('NFLOG callback failed')
    # Exclusive create: never flush or replace another service's table.
    rules=[f'create table inet {TABLE}',f'add chain inet {TABLE} out {{ type filter hook output priority -140; policy accept; }}']
    for i in ids: rules.append(f'add rule inet {TABLE} out meta mark {0x46560000|i} tcp dport 443 @ih,0,16 0x1603 @ih,40,8 1 limit rate 100/second burst 100 packets log group {GROUP} snaplen 8192')
    running=True
    def stop(*_):
        nonlocal running
        running=False
    for sig in (signal.SIGINT,signal.SIGTERM): signal.signal(sig,stop)
    subprocess.run(['nft','-f','-'],input='\n'.join(rules)+'\n',text=True,check=True)
    try:
        last=time.monotonic()
        while running:
            if select.select([fd],[],[],1)[0]:
                try: buf=os.read(fd,131072)
                except OSError as e:
                    if e.errno==errno.ENOBUFS: continue  # netlink overflow: observation is best effort
                    raise
                handle(h,buf,len(buf))
            if time.monotonic()-last>=2: store.flush(); last=time.monotonic()
    finally:
        subprocess.run(['nft','delete','table','inet',TABLE],check=False);store.flush();db.close()

if __name__=='__main__':
    ids=sorted(set(map(int,sys.argv[1:])))
    if not ids or len(ids)>32 or min(ids)<1 or max(ids)>65535: sys.exit('usage: collector.py SUBSCRIPTION_ID [ID ...] (max 32)')
    run(ids)
