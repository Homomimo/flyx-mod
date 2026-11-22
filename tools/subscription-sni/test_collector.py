import sqlite3
import ssl
import time
import unittest
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2] / "go-gost/x/kernel/sniworker"))
from collector import parse, Store


def hello(name):
    ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);ctx.check_hostname=False
    out=ssl.MemoryBIO(); client=ctx.wrap_bio(ssl.MemoryBIO(),out,server_hostname=name)
    try: client.do_handshake()
    except ssl.SSLWantReadError: pass
    ip=bytearray(20);ip[0]=0x45;ip[9]=6
    tcp=bytearray(20);tcp[2:4]=b'\x01\xbb';tcp[12]=0x50
    return bytes(ip+tcp)+out.read()

class CollectorTest(unittest.TestCase):
    def test_reconcile_health(self):
        db=sqlite3.connect(':memory:'); Store(db,[7,8]).activate()
        db.execute('UPDATE health SET seen=123 WHERE sub=0'); Store(db,[8,12]).activate()
        self.assertEqual(db.execute('select sub,seen from health where sub>0 order by sub').fetchall(),[(7,123),(8,-1),(12,-1)])
        db.close()
    def test_visible_client_hello_only(self):
        raw=hello('docs.example.test');self.assertEqual(parse(raw)[0],'docs.example.test');self.assertIsNone(parse(raw[:64]));self.assertIsNone(parse(hello(None)))
    def test_scope_minute_and_retransmission(self):
        db=sqlite3.connect(':memory:');s=Store(db,[7,8]);t=int(time.time())//60*60; sample=parse(hello('docs.example.test'))
        s.add(7,sample,t);s.add(7,sample,t+1);s.add(8,sample,t);s.add(9,sample,t);s.add(7,parse(hello('docs.example.test')),t+60);s.flush();s.flush()
        self.assertEqual(db.execute('select sub,t,n from hits order by sub,t').fetchall(),[(7,t,1),(7,t+60,1),(8,t,1)]);db.close()
if __name__=='__main__': unittest.main()
