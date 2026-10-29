"""Isolate Bloom arithmetic/layout costs in real Get; candidates are runner-only."""
import difflib
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile

from workloads import capture, warm

output = Path(sys.argv[1]).resolve()
output.mkdir(parents=True, exist_ok=True)
binary_dir = Path('comparison-bin').resolve()
binary_dir.mkdir(exist_ok=True)
source = Path('src/bloom.rs')
production = source.read_text()
legacy = Path('baseline/src/bloom.rs').read_text()
manifest = 'benchmarks/rocksdb-compare/Cargo.toml'
names = ['original', 'production', 'modulo', 'legacy-bloom']
binaries = {n: binary_dir / ('workloads-' + n) for n in names}
start = production.index('    pub fn contains_hash(')
end = production.index('    #[inline]\n    fn bit_position(', start)
body = production[start:end]
old = 'let bit = self.bit_position(h);'
assert body.count(old) == 1
modulo = production[:start] + body.replace(old, 'let bit = (h % u64::from(self.bit_count)) as usize;') + production[end:]
variants = dict(production=production, modulo=modulo, **{'legacy-bloom': legacy})
evidence = {}

# Every binary uses the same current public-API harness and dependencies.
for relative in ['src/bin/workloads.rs', 'src/workloads_backend.rs', 'src/bounds_workload.rs']:
    shutil.copy2(Path('benchmarks/rocksdb-compare') / relative,
                 Path('baseline/benchmarks/rocksdb-compare') / relative)
subprocess.run(['cargo', 'build', '--locked', '--release', '--manifest-path', 'baseline/' + manifest, '--bin', 'workloads'], check=True)
shutil.copy2('baseline/benchmarks/rocksdb-compare/target/release/workloads', binaries['original'])
try:
    for name, text in variants.items():
        source.write_text(text)
        subprocess.run(['rustfmt', '--edition', '2021', '--config', 'skip_children=true', str(source)], check=True)
        text = source.read_text()
        # Arithmetic/golden-format/model tests run before measuring a candidate.
        subprocess.run(['cargo', 'test', '--locked', '--release', '--all-features', '--lib'], check=True)
        subprocess.run(['cargo', 'build', '--locked', '--release', '--manifest-path', manifest, '--bin', 'workloads'], check=True)
        shutil.copy2('benchmarks/rocksdb-compare/target/release/workloads', binaries[name])
        evidence[name] = dict(bloom_sha256=hashlib.sha256(text.encode()).hexdigest(),
                              binary_sha256=hashlib.sha256(binaries[name].read_bytes()).hexdigest())
        (output / (name + '-bloom.rs')).write_text(text)
        (output / (name + '.patch')).write_text(''.join(difflib.unified_diff(production.splitlines(True), text.splitlines(True), fromfile='production/src/bloom.rs', tofile=name + '/src/bloom.rs')))
finally:
    source.write_text(production)
assert source.read_text() == production

profiles = [
    ('hot4k', dict(keys=20000, value=128, block=4096, cache=8388608, churn=False), 'hot'),
    ('hot16k', dict(keys=20000, value=128, block=16384, cache=8388608, churn=False), 'hot'),
    ('miss16k', dict(keys=20000, value=128, block=16384, cache=8388608, churn=False), 'miss'),
    ('uniform200k', dict(keys=200000, value=128, block=16384, cache=8388608, churn=False), 'uniform'),
]
environment = dict(commit=capture('git', 'rev-parse', 'HEAD'), baseline=capture('git', '-C', 'baseline', 'rev-parse', 'HEAD'),
                   platform=platform.platform(), cpu=capture('lscpu'), rust=capture('rustc', '--version'),
                   trials=12, variants=evidence,
                   binaries={n:hashlib.sha256(p.read_bytes()).hexdigest() for n,p in binaries.items()},
                   notes='Runner-only candidates; no production edit. Same baseline fixture bytes, current harness, owned Get, warm OS cache, 12 balanced process orders; first and repeat engine-cache passes. Not a RocksDB comparison.')
assert environment['baseline'] == 'c0299ec14e60a249fb9628f1888c5740c655432b'
rows = []
with tempfile.TemporaryDirectory(prefix='kv-bloom-api-', dir=os.environ.get('KV_BENCH_DIR')) as tmp:
    root = Path(tmp)
    environment['filesystem'] = capture('findmnt', '-T', str(root), '-o', 'SOURCE,FSTYPE,OPTIONS')
    (output / 'environment.json').write_text(json.dumps(environment, indent=2))
    with (output / 'results.jsonl').open('w') as sink:
        def run(name, mode, directory, config, case, trial, order):
            label = f'{case}-{trial}-{name}-{mode}'
            print('BLOOM_API_CASE ' + label, flush=True)
            completed = subprocess.run([str(binaries[name]), mode, str(directory), json.dumps(config)], text=True, capture_output=True, timeout=120)
            (output / (label + '.log')).write_text(completed.stdout + completed.stderr)
            if completed.returncode:
                print(completed.stdout + completed.stderr, flush=True)
            completed.check_returncode()
            assert 'WORKLOAD_VERIFIED' in completed.stdout
            found = [json.loads(s.removeprefix('WORKLOAD_RESULT ')) for s in completed.stdout.splitlines() if s.startswith('WORKLOAD_RESULT ')]
            assert [r['stage'] for r in found] == ([] if mode == 'prepare' else ['open', 'first', 'repeat'])
            for row in found:
                assert row['backend'] == 'engine' and row['config'] == config
                row.update(backend=name, case=case, trial=trial, order=order)
                rows.append(row)
                sink.write(json.dumps(row) + '\n')
                sink.flush()

        for case, profile, workload in profiles:
            fixture = root / (case + '-fixture')
            run('original', 'prepare', fixture, profile, case, 0, 0)
            for trial in range(1, 13):
                config = dict(profile, workload=workload, queries=500000, seed=7919 + trial * 104729)
                offset = (trial - 1) % len(names)
                for order, name in enumerate(names[offset:] + names[:offset], 1):
                    copy = root / f'{case}-{trial}-{name}'
                    shutil.copytree(fixture, copy)
                    warm(copy)
                    run(name, 'read', copy, config, case, trial, order)
                    shutil.rmtree(copy)
assert len(rows) == 576
print('BLOOM_API_COMPLETE records=576', flush=True)
