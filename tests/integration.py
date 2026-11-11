import json, socket, subprocess, sys, time, urllib.request

def free_port():
    s=socket.socket();s.bind(('127.0.0.1',0));p=s.getsockname()[1];s.close();return p

def fetch(url):
    return urllib.request.urlopen(url,timeout=3).read().decode()

ports=[free_port() for _ in range(4)]
procs=[]
try:
    for i,p in enumerate(ports[:2]):
        procs.append(subprocess.Popen([sys.argv[2],str(p),f'Server-{i+1}'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    procs.append(subprocess.Popen([sys.argv[1],str(ports[2]),*[f'127.0.0.1:{p}' for p in ports[:2]],'--admin-port',str(ports[3]),'--health-ms','200','--log','integration.csv'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    for _ in range(50):
        try:
            data=json.loads(fetch(f'http://127.0.0.1:{ports[3]}/metrics'))
            if all(b['healthy'] for b in data['backends']):break
        except Exception:pass
        time.sleep(.1)
    else:raise AssertionError('backends did not become healthy')
    replies=[fetch(f'http://127.0.0.1:{ports[2]}/') for _ in range(6)]
    assert replies==['Hello from Server-1\n','Hello from Server-2\n']*3,replies
    data=json.loads(fetch(f'http://127.0.0.1:{ports[3]}/metrics'))
    assert data['completed']>=6 and data['accepted']>=6,data
    procs[0].terminate();procs[0].wait(timeout=3)
    for _ in range(40):
        data=json.loads(fetch(f'http://127.0.0.1:{ports[3]}/metrics'))
        if not data['backends'][0]['healthy']:break
        time.sleep(.1)
    else:raise AssertionError('failure not detected')
    assert fetch(f'http://127.0.0.1:{ports[2]}/')=='Hello from Server-2\n'
    print('PASS: round robin, metrics, backend failure handling')
finally:
    for p in procs:
        if p.poll() is None:p.terminate()
    for p in procs:
        try:p.wait(timeout=4)
        except subprocess.TimeoutExpired:p.kill()
