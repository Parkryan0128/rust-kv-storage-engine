"""Export every public condition from a validated comparison, without selecting winners."""
import csv, json, sys
from pathlib import Path
source, destination = map(Path, sys.argv[1:3])
report = json.loads(source.read_text())
assert report['public_conditions'] == 263 and report['public_rows'] == 5283
fields = ['suite','case','scenario','stage','trials','baseline_seconds_median','engine_seconds_median','rocksdb_seconds_median','paired_engine_vs_baseline','baseline_ci_lower','baseline_ci_upper','paired_engine_vs_rocksdb','rocksdb_ci_lower','rocksdb_ci_upper','engine_peak_rss_kib','rocksdb_peak_rss_kib','engine_sst_bytes','rocksdb_sst_bytes','baseline_commit','engine_commit','measurement_path_policy']
fields += ['paired_engine_vs_identical_control', 'control_ci_lower', 'control_ci_upper']
def metric(backend, choices):
    values = backend['metrics_median']
    return next((values[k] for k in choices if k in values), '')
with destination.open('w', newline='', encoding='utf-8') as output:
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for g in report['summary']:
        row = {k:g[k] for k in ['suite','case','scenario','stage','trials']}
        for name in ['baseline','engine','rocksdb']:
            row[name+'_seconds_median'] = g['backends'][name]['seconds_median']
        for name in ['baseline','rocksdb']:
            ratio = g['vs_'+name]
            row['paired_engine_vs_'+name] = ratio['median']
            row[name+'_ci_lower'], row[name+'_ci_upper'] = ratio['bootstrap_median_95']
        for name in ['engine','rocksdb']:
            row[name+'_peak_rss_kib'] = metric(g['backends'][name], ['memory.peak_rss_kib','after.peak_rss_kib'])
            row[name+'_sst_bytes'] = metric(g['backends'][name], ['stats_after.sst_bytes','stats.sst_bytes'])
        row['baseline_commit'], row['engine_commit'] = report['baseline'], report['head']
        row['measurement_path_policy'] = 'fixed-v1' if report.get('fixed_paths_verified') else 'native-path-historical'
        if 'engine_vs_identical_control' in g:
            control = g['engine_vs_identical_control']
            row['paired_engine_vs_identical_control'] = control['median']
            row['control_ci_lower'], row['control_ci_upper'] = control['bootstrap_median_95']
        writer.writerow(row)
print(json.dumps({'conditions':len(report['summary']),'bytes':destination.stat().st_size,'output':str(destination)}))
