"""Bounded asynchronous JSON-lines transport with cursor replay + snapshot fallback."""
import argparse,asyncio,json
from collections import deque
class Hub:
    def __init__(self,history=32,capacity=8):
        if history<1 or capacity<1:raise ValueError('POSITIVE_BOUNDS_REQUIRED')
        self.events=deque(maxlen=history);self.capacity=capacity;self.seq=0;self.state={};self.clients=set();self.metrics={'published':0,'slow_disconnects':0,'snapshot_syncs':0}
    def sync(self,cursor):
        if type(cursor) is not int or cursor<0 or cursor>self.seq:raise ValueError('INVALID_CURSOR')
        if cursor==self.seq:return {'kind':'replay','events':[],'cursor':self.seq}
        if self.events and cursor>=self.events[0]['seq']-1:return {'kind':'replay','events':[e for e in self.events if e['seq']>cursor],'cursor':self.seq}
        self.metrics['snapshot_syncs']+=1;return {'kind':'snapshot','state':dict(self.state),'cursor':self.seq}
    def subscribe(self,cursor):
        if len(self.clients)>=64:raise ValueError('SUBSCRIBER_LIMIT')
        initial=self.sync(cursor);q=asyncio.Queue(self.capacity);self.clients.add(q);return initial,q
    def publish(self,key,value):
        if not isinstance(key,str) or not 1<=len(key)<=64 or not isinstance(value,str) or len(value)>256:raise ValueError('INVALID_EVENT')
        if key not in self.state and len(self.state)>=128:raise ValueError('STATE_KEY_LIMIT')
        self.seq+=1;self.state[key]=value;e={'kind':'event','seq':self.seq,'key':key,'value':value};self.events.append(e);self.metrics['published']+=1
        for q in tuple(self.clients):
            if q.full():
                self.clients.remove(q);self.metrics['slow_disconnects']+=1
                while not q.empty():q.get_nowait()
                q.put_nowait({'kind':'resync_required','cursor':self.seq})
            else:q.put_nowait(e)
        return e
    def unsubscribe(self,q):self.clients.discard(q)

class RealtimeServer:
    def __init__(self,hub=None):self.hub=hub or Hub();self.tasks=set();self.server=None
    async def start(self):self.server=await asyncio.start_server(self.handle,'127.0.0.1',0,limit=8192);return self.server.sockets[0].getsockname()[1]
    async def close(self):
        self.server.close()
        for t in tuple(self.tasks):t.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)
        await self.server.wait_closed()
    async def handle(self,reader,writer):
        task=asyncio.current_task();self.tasks.add(task);q=None
        async def send(body):writer.write((json.dumps(body)+'\n').encode());await asyncio.wait_for(writer.drain(),2)
        try:
            body=json.loads(await asyncio.wait_for(reader.readline(),2))
            if not isinstance(body,dict):raise ValueError('INVALID_MESSAGE')
            if body.get('op')=='publish':await send(self.hub.publish(body.get('key'),body.get('value')))
            elif body.get('op')=='metrics':await send(dict(self.hub.metrics,subscribers=len(self.hub.clients)))
            elif body.get('op')=='subscribe':
                initial,q=self.hub.subscribe(body.get('cursor',0));await send(initial)
                disconnect=asyncio.create_task(reader.read(1))
                event=None
                try:
                    while True:
                        event=asyncio.create_task(q.get())
                        done,_=await asyncio.wait([event,disconnect],return_when=asyncio.FIRST_COMPLETED)
                        if disconnect in done:
                            event.cancel();await asyncio.gather(event,return_exceptions=True);break
                        await send(event.result())
                        if event.result()['kind']=='resync_required':break
                finally:
                    disconnect.cancel();await asyncio.gather(disconnect,return_exceptions=True)
                    if event:
                        event.cancel();await asyncio.gather(event,return_exceptions=True)
            else:raise ValueError('UNKNOWN_OP')
        except (ValueError,TypeError,asyncio.TimeoutError) as e:
            try:await send({'kind':'error','reason':str(e)})
            except (ConnectionError,asyncio.TimeoutError):pass
        except ConnectionError:pass
        finally:
            if q:self.hub.unsubscribe(q)
            writer.close()
            try:await writer.wait_closed()
            except ConnectionError:pass
            self.tasks.discard(task)

async def request(port,body):
    r,w=await asyncio.open_connection('127.0.0.1',port);w.write((json.dumps(body)+'\n').encode());await w.drain()
    try:return json.loads(await asyncio.wait_for(r.readline(),3))
    finally:w.close();await w.wait_closed()
async def demo():
    srv=RealtimeServer(Hub(history=2));port=await srv.start()
    try:
        for i in range(4):await request(port,{'op':'publish','key':'session','value':str(i)})
        snapshot=await request(port,{'op':'subscribe','cursor':0});replay=await request(port,{'op':'subscribe','cursor':3});metrics=await request(port,{'op':'metrics'})
        return {'snapshot':snapshot,'replay':replay,'metrics':metrics,'transport':'loopback TCP JSON-lines; not WebSocket/WebRTC'}
    finally:await srv.close()
async def main():
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['demo','serve'],default='demo',nargs='?');a=p.parse_args()
    if a.mode=='demo':print(json.dumps(await demo(),indent=2))
    else:
        srv=RealtimeServer();port=await srv.start();print(json.dumps({'host':'127.0.0.1','port':port}),flush=True)
        try:await srv.server.serve_forever()
        finally:await srv.close()
if __name__=='__main__':asyncio.run(main())
