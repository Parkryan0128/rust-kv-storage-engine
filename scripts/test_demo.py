#!/usr/bin/env python3
"""Exercise the real HTTP demo, including recovery and bounded requests."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import urllib.error
import urllib.request

binary = Path(sys.argv[1] if len(sys.argv) > 1 else 'target/release/examples/demo').resolve()
if sys.platform == 'linux':
    import resource

server = subprocess.Popen([str(binary), '0'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True)
root = None
try:
    line = server.stdout.readline().strip()
    assert line.startswith('Storage Lab: http://127.0.0.1:'), line
    base = line.removeprefix('Storage Lab: ')
    path_line = server.stdout.readline().strip()
    root = Path(path_line.removeprefix('Temporary sandbox: ').split(' (')[0])

    def request(path, body=None, status=200, headers=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base + path, data=data,
                                     headers=headers if headers is not None else {'Content-Type': 'application/json', 'X-KV-Demo': '1'})
        try:
            response = urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        assert response.status == status, (path, response.status, response.read())
        return json.loads(response.read())

    assert request('/api/state')['sequence'] == 0
    request('/api/put', {'key': 'user:1', 'value': 'Ryan'})
    assert request('/api/get', {'key': 'user:1'})['value'] == 'Ryan'
    state = request('/api/flush', {})['state']
    old = state['tables'][0]['id']
    assert request(f'/api/table/{old}')['records'][0]['value'] == 'Ryan'
    request('/api/delete', {'key': 'user:1'})
    request('/api/reopen', {})
    assert request('/api/get', {'key': 'user:1'})['value'] is None
    assert request(f'/api/table/{old}')['records'][0]['value'] == 'Ryan'
    request('/api/compact', {})
    request(f'/api/table/{old}', status=400)
    assert request('/api/state')['stats']['sst_records'] == 0
    request('/api/put', {'key': '', 'value': 'invalid'}, status=400)
    request('/api/put', {'key': 'large', 'value': 'x' * 1025}, status=400)
    request('/api/put', {'key': 'large', 'value': 'x' * 9000}, status=400)
    request('/api/put', {'key': 'bad', 'value': 'no'}, status=400, headers={})
    request('/api/put', {'key': 'bad', 'value': 'no'}, status=400,
            headers={'X-KV-Demo': '1', 'Origin': 'https://other.example'})
    request('/api/state', status=403, headers={'Host': 'other.example'})
    assert request('/api/get', {'key': 'bad'})['value'] is None
    request('/api/put', {'key': '<script>alert(1)</script>', 'value': '한글 & <b>text</b>'})
    assert request('/api/get', {'key': '<script>alert(1)</script>'})['value'] == '한글 & <b>text</b>'
    request('/api/compare/step', {}, status=400)
    c = request('/api/compare/start', {})
    assert c['batch'] == 0
    for batch in range(1, 25):
        c = request('/api/compare/step', {})
        assert c['batch'] == batch
        assert c['full']['sequence'] == c['tiered']['sequence']
    last = c['history'][-1]
    assert last['tiered'] < last['full']
    request('/api/compare/step', {}, status=400)
    print(f'Comparison: {last}; SST output reduction: {100 * (1-last["tiered"]/last["full"]):.1f}%')
    request('/api/reset', {})
    assert request('/api/state')['sequence'] == 0
    if sys.platform == 'linux' and Path(f'/proc/{server.pid}/status').exists():
        lines = Path(f'/proc/{server.pid}/status').read_text().splitlines()
        print('Memory (server only):', ', '.join(line.strip() for line in lines if line.startswith(('VmPeak:', 'VmHWM:', 'VmRSS:'))))
    print('HTTP demo checks passed: real writes, SST inspection, deletes, reopen, compaction, request boundaries, comparison, reset.')
finally:
    server.terminate()
    try:
        server.wait(timeout=10)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()
    if root and root.is_dir() and root.name.startswith('.tmp'):
        shutil.rmtree(root)

if sys.platform == 'linux':
    print('Child peak RSS (KiB):', resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
