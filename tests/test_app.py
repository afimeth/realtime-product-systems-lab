import asyncio,json,unittest
from app import Hub,RealtimeServer,request,demo
class CoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_ordered_live_events(self):
        h=Hub();initial,q=h.subscribe(0);h.publish('a','one');h.publish('a','two');self.assertEqual([(await q.get())['seq'] for _ in range(2)],[1,2]);self.assertEqual(h.state['a'],'two')
    async def test_reconnect_replays_missing_only(self):
        h=Hub();h.publish('a','1');h.publish('a','2');self.assertEqual([e['seq'] for e in h.sync(1)['events']],[2])
    async def test_expired_cursor_gets_snapshot(self):
        h=Hub(history=2)
        for i in range(4):h.publish('a',str(i))
        self.assertEqual(h.sync(0),{'kind':'snapshot','state':{'a':'3'},'cursor':4})
    async def test_slow_consumer_bounded_and_evicted(self):
        h=Hub(capacity=1);_,q=h.subscribe(0);h.publish('a','1');h.publish('a','2');self.assertEqual(q.qsize(),1);self.assertEqual((await q.get())['kind'],'resync_required');self.assertEqual(h.metrics['slow_disconnects'],1);self.assertNotIn(q,h.clients)
    async def test_fast_consumer_survives_slow(self):
        h=Hub(capacity=1);_,fast=h.subscribe(0);_,slow=h.subscribe(0);h.publish('a','1');await fast.get();h.publish('a','2');self.assertEqual((await fast.get())['seq'],2);self.assertIn(fast,h.clients);self.assertNotIn(slow,h.clients)
    async def test_future_and_invalid_cursor_rejected(self):
        h=Hub()
        for c in (-1,1,True,'1'):
            with self.assertRaises(ValueError):h.subscribe(c)
        self.assertEqual(len(h.clients),0)
    async def test_invalid_event_no_mutation(self):
        h=Hub()
        for k,v in [('', 'x'),('x',5),('x','a'*257)]:
            with self.assertRaises(ValueError):h.publish(k,v)
        self.assertEqual(h.seq,0)
    async def test_snapshot_detached(self):
        h=Hub(history=1);h.publish('x','1');h.publish('x','2');s=h.sync(0);h.publish('x','3');self.assertEqual(s['state']['x'],'2')
    async def test_state_and_subscriber_bounds(self):
        h=Hub()
        for i in range(128):h.publish(str(i),'x')
        with self.assertRaises(ValueError):h.publish('extra','x')
        h.publish('0','update')
        for _ in range(64):h.subscribe(h.seq)
        with self.assertRaises(ValueError):h.subscribe(h.seq)
class WireTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):self.srv=RealtimeServer();self.port=await self.srv.start()
    async def asyncTearDown(self):await self.srv.close()
    async def test_wire_publish_and_metrics(self):
        self.assertEqual((await request(self.port,{'op':'publish','key':'x','value':'1'}))['seq'],1);self.assertEqual((await request(self.port,{'op':'metrics'}))['published'],1)
    async def test_wire_live_then_reconnect(self):
        r,w=await asyncio.open_connection('127.0.0.1',self.port);w.write(b'{"op":"subscribe","cursor":0}\n');await w.drain();self.assertEqual(json.loads(await r.readline())['kind'],'replay')
        await request(self.port,{'op':'publish','key':'x','value':'1'});self.assertEqual(json.loads(await asyncio.wait_for(r.readline(),2))['seq'],1);w.close();await w.wait_closed()
        await request(self.port,{'op':'publish','key':'x','value':'2'});self.assertEqual((await request(self.port,{'op':'subscribe','cursor':1}))['events'][0]['seq'],2)
    async def test_bad_op_returns_error(self):self.assertEqual((await request(self.port,{'op':'bad'}))['kind'],'error')
    async def test_bad_json(self):
        r,w=await asyncio.open_connection('127.0.0.1',self.port);w.write(b'bad\n');await w.drain();self.assertEqual(json.loads(await r.readline())['kind'],'error');w.close();await w.wait_closed()
    async def test_demo_actual_transport(self):self.assertEqual((await demo())['snapshot']['state']['session'],'3')
    async def test_server_shutdown_cleans_subscriptions(self):
        r,w=await asyncio.open_connection('127.0.0.1',self.port);w.write(b'{"op":"subscribe"}\n');await w.drain();await r.readline()
        async with asyncio.timeout(3):await self.srv.close()
        self.assertEqual(len(self.srv.hub.clients),0);self.assertEqual(len(self.srv.writers),0);w.close();await w.wait_closed()
