"""Validate and summarize the complete three-way run without pooling unlike workloads."""
import collections, json, statistics, math, random, sys
from pathlib import Path
ROOT = Path(sys.argv[1]).resolve()
HEAD = sys.argv[2]
BASELINE = sys.argv[3]
CHECKPOINT = "--checkpoint" in sys.argv[4:]
FIXED_PATHS = "--fixed-paths" in sys.argv[4:]
ART = ROOT / "full-results"
if not ART.exists():
    candidates = list(ROOT.rglob("paired/results.jsonl"))
    assert len(candidates) == 1, candidates
    ART = candidates[0].parent.parent
EXPECTED = {"reopened":36,"paired":2034,"random":324,"bounds":486,"writes":63,"remaining":810,
            "scan-scaling":216,"compactions":108,"merge-matrix":540,"internal":96,
            "maintenance":24,"compact-request":36}
if CHECKPOINT:
    EXPECTED = {k:v for k,v in EXPECTED.items() if k in {"reopened","paired","random","bounds","writes","remaining"}}
random_count=sum(bool(line.strip()) for line in (ART/"random"/"results.jsonl").read_text().splitlines())
assert random_count in (324,864), random_count
CONTROL = random_count == 864
EXPECTED["random"] = random_count
groups=collections.defaultdict(list)
counts={}
all_rows=[]
for suite, expected in EXPECTED.items():
    path=ART/suite/"results.jsonl"
    rows=[json.loads(s) for s in path.read_text().splitlines() if s.strip()]
    assert len(rows)==expected,(suite,len(rows),expected)
    counts[suite]=len(rows)
    for r in rows:
        r=dict(r,suite=suite)
        if suite=="random":
            r["backend"]=r["engine"]
            r["case"]=f'{r["keys"]}keys-{r["value_bytes"]}b-{r["tables"]}sst-{r["block_bytes"]}block'
        if suite not in {"internal","maintenance","compact-request"}:
            groups[(suite,r["case"],r.get("scenario",""),r["stage"])].append(r)
        all_rows.append(r)
for file,suite in [("focused/durable.jsonl","focused"),("focused/internal.jsonl","focused-internal"),
                   ("random/profiles.jsonl","read-profile")]:
    if CHECKPOINT and suite != "read-profile":
        continue
    rows=[dict(json.loads(s),suite=suite) for s in (ART/file).read_text().splitlines() if s.strip()]
    counts[suite]=len(rows)
    assert len(rows)=={"focused":72,"focused-internal":180,"read-profile":18}[suite]
    all_rows.extend(rows)
    if suite=="focused":
        for r in rows:groups[(suite,r["case"],"",r["stage"])].append(r)
for file,suite,expected in [("audit/results.json","audit",54),("audit/phases.json","write-phases",36)]:
    if CHECKPOINT:
        continue
    rows=[dict(r,suite=suite) for r in json.loads((ART/file).read_text())]
    counts[suite]=len(rows)
    assert len(rows)==expected
    all_rows.extend(rows)
    if suite=="audit":
        for r in rows:groups[(suite,r["case"],"",r["stage"])].append(r)
def flat(v,prefix=""):
    result={}
    for k,x in v.items():
        path=(prefix+"." if prefix else "")+k
        if isinstance(x,dict):result.update(flat(x,path))
        elif isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x):result[path]=x
    return result
def duration(r):
    m=r.get("metrics",r)
    if "seconds" in m:return m["seconds"]
    if "commit_seconds" in m:return m["commit_seconds"]
    if "us" in m:return m["us"]/1e6
    raise ValueError((r["suite"],r.get("stage"),m.keys()))
def ratio_summary(ratios):
    rng=random.Random(20261029)
    samples=sorted(statistics.median(rng.choices(ratios,k=len(ratios))) for _ in range(4000))
    return dict(median=statistics.median(ratios),min=min(ratios),max=max(ratios),paired_ratios=ratios,below_one=sum(r<1 for r in ratios),bootstrap_median_95=[samples[100],samples[3899]])

summary=[]
for (suite,case,scenario,stage),rows in sorted(groups.items()):
    if stage == "recover":
        assert all(r["config"].get("recover_to_memtable") is True for r in rows)
        assert all(r["metrics"]["stats"]["sst_files"] == 0 for r in rows), (suite,case)
    backends={b:sorted([r for r in rows if r["backend"]==b],key=lambda r:r["trial"]) for b in (["baseline","engine","engine-control","rocksdb"] if suite=="random" and CONTROL else ["baseline","engine","rocksdb"])}
    trials={b:[r["trial"] for r in rs] for b,rs in backends.items()}
    assert all(t==trials["engine"] for t in trials.values()),(suite,case,trials)
    assert set(r["backend"] for r in rows)==set(backends), (suite,case)
    assert len(set(trials["engine"]))==len(trials["engine"])
    assert len(trials["engine"])== (12 if suite=="random" and CONTROL else 12 if suite=="writes" and case=="batch32-128b" else 12 if suite=="reopened" else 3 if suite=="writes" else 6)
    g=dict(suite=suite,case=case,scenario=scenario,stage=stage,trials=len(trials["engine"]),backends={})
    for b,rs in backends.items():
        ts=[duration(r) for r in rs]
        assert all(math.isfinite(t) and t > 0 for t in ts), (suite,case,stage,b,ts)
        metrics=[flat(r.get("metrics",r)) for r in rs]
        common=set.intersection(*(set(m) for m in metrics))
        med={p:statistics.median(m[p] for m in metrics) for p in sorted(common)}
        g["backends"][b]=dict(seconds_median=statistics.median(ts),seconds_min=min(ts),seconds_max=max(ts),metrics_median=med)
    for other in ["baseline","rocksdb"]:
        ratios=[duration(o)/duration(e) for o,e in zip(backends[other],backends["engine"])]
        g["vs_"+other]=ratio_summary(ratios)
    if "engine-control" in backends:
        g["engine_vs_identical_control"]=ratio_summary([duration(c)/duration(e) for c,e in zip(backends["engine-control"],backends["engine"])])
        g["control_vs_baseline"]=ratio_summary([duration(b)/duration(c) for b,c in zip(backends["baseline"],backends["engine-control"])])
    g["regression_candidate"] = g["vs_baseline"]["median"] < 0.97 and g["vs_baseline"]["bootstrap_median_95"][1] < 1
    summary.append(g)
environments={}
for path in ART.rglob("environment.json"):
    env=json.loads(path.read_text())
    assert env["commit"]==HEAD,(path,env["commit"])
    if FIXED_PATHS:
        assert env.get("measurement_path_policy") == "fixed-v1", (path, env.get("measurement_path_policy"))
    assert env.get("baseline")==BASELINE, (path, env.get("baseline"))
    if path.parent.name=="random" and CONTROL:
        assert env["binaries"]["engine"]["sha256"]==env["binaries"]["engine-control"]["sha256"]
        assert env["settings"]["trials"]==12
    environments[path.relative_to(ART).as_posix()]=env
required_environments = {"bounds", "paired", "random", "random-smoke", "remaining", "reopened", "smoke", "writes"}
if not CHECKPOINT:
    required_environments |= {"compactions", "focused", "merge-matrix", "scan-scaling"}
assert set(environments) == {name + "/environment.json" for name in required_environments}, set(environments)
# lscpu includes a live frequency-scaling percentage. Retain the raw value in
# each environment, but compare all other identity fields exactly.
cpus={"\n".join(line for line in x["cpu"].splitlines()
                 if not line.startswith("CPU(s) scaling MHz:"))
      for x in environments.values() if "cpu" in x}
assert len(cpus)==1,"Mismatched host CPU"
assert len(summary)==(208 if CHECKPOINT else 263), len(summary)
report={"checkpoint":CHECKPOINT,"fixed_paths_verified":FIXED_PATHS,"head":HEAD,"baseline":BASELINE,"counts":counts,"public_rows":sum(counts.get(s,0) for s in ["reopened","paired","random","bounds","writes","remaining","scan-scaling","compactions","merge-matrix","focused","audit"]),
        "total_rows":sum(counts.values()),"public_conditions":len(summary),"summary":summary,
        "environments":environments}
(ROOT/"comparison-summary.json").write_text(json.dumps(report,indent=2))
(ROOT/"all-results.json").write_text(json.dumps(all_rows,indent=2))
lines=["Read/write/recovery checkpoint" if CHECKPOINT else "Complete benchmark comparison",
       f"Engine production and harness/run: {HEAD}; baseline: {BASELINE}",
       "All public comparisons run on one host. Ratios are median of trial-matched speed ratios; >1 means current is faster.",
       "Warm OS cache, no compression, owned Get results. Not cold-disk or prolonged production load.",
       "Public paired suite measures validation separately; large random and diagnostic modes include validation.",
       "Shared baseline SST fixtures for general reads; native fixtures for large random and first-range tests.",
       f"Most cases: 6 trials; dedicated writes: 3 trials, batch32: 12 trials; reopened first-scan: 12 trials x 32 traces; random: {12 if CONTROL else 6} trials, A/A control={CONTROL}.",
       "Measurement paths: " + ("fixed-v1 verified" if FIXED_PATHS else "historical; fixed paths not required"),
       "Internal profiles have no equivalent RocksDB comparison.",
       "Count summary: "+json.dumps(counts),"", 
       "suite | case | scenario | stage | baseline ms | current ms | RocksDB ms | current/baseline speed | current/RocksDB speed"]
for g in summary:
    m=[g["backends"][b]["seconds_median"]*1000 for b in ["baseline","engine","rocksdb"]]
    lines.append(" | ".join([g["suite"],g["case"],g["scenario"],g["stage"],*(f"{v:.6f}" for v in m),f'{g["vs_baseline"]["median"]:.4f}x',f'{g["vs_rocksdb"]["median"]:.4f}x']))
(ROOT/"all-comparisons.txt").write_text("\n".join(lines)+"\n")
print(json.dumps({k:report[k] for k in ["counts","public_rows","total_rows","public_conditions"]},indent=2))

print(json.dumps({"regression_candidates":[{"suite":g["suite"],"case":g["case"],"scenario":g["scenario"],"stage":g["stage"],"ratio":g["vs_baseline"]["median"],"interval":g["vs_baseline"]["bootstrap_median_95"]} for g in summary if g["regression_candidate"]]},indent=2))
