use rust_kv_storage_engine::{CompactionStyle, Engine, MemtableInfo, Options, RecordPreview};
use serde_json::{json, Value};
use std::{collections::VecDeque, io::Read, net::SocketAddr, time::Instant};
use tiny_http::{Header, Method, Request, Response, Server};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn options(style: CompactionStyle) -> Options {
    Options {
        memtable_size_limit: 64 * 1024,
        block_cache_capacity: 4 * 1024 * 1024,
        block_size: 4096,
        max_key_size: 128,
        max_value_size: 1024,
        compaction_style: style,
        ..Options::default()
    }
}
fn record(r: RecordPreview) -> Value {
    json!({"key":String::from_utf8_lossy(&r.key),"value":r.value.as_ref().map(|v|String::from_utf8_lossy(v)),
        "key_bytes":r.key_len,"value_bytes":r.value_len,"sequence":r.sequence,
        "truncated":r.key_len>128||r.value_len>128})
}
fn memory(m: MemtableInfo) -> Value {
    json!({"wal_id":m.wal_id,"bytes":m.bytes,"records":m.records,"preview":m.preview.into_iter().map(record).collect::<Vec<_>>()})
}
fn snapshot(e: &Engine) -> Result<Value> {
    let i = e.inspect()?;
    let s = e.stats();
    Ok(
        json!({"sequence":i.sequence,"active":memory(i.active),"frozen":i.frozen.into_iter().map(memory).collect::<Vec<_>>(),
        "tables":i.tables.into_iter().map(|t|json!({"id":t.id,"bytes":t.bytes,"records":t.records,"max_sequence":t.max_sequence,"tier":t.bytes.max(1).ilog2()})).collect::<Vec<_>>(),
        "stats":{"sst_bytes":s.sst_bytes,"sst_files":s.sst_files,"sst_records":s.sst_records,
        "flush_bytes":s.flush_bytes,"compaction_input_bytes":s.compaction_input_bytes,
        "compaction_output_bytes":s.compaction_output_bytes,"compactions":s.compactions,
        "cache_hits":s.cache_hits,"block_reads":s.block_reads,"bloom_negatives":s.bloom_negatives}}),
    )
}
struct Comparison {
    full: Engine,
    tiered: Engine,
    _dir: tempfile::TempDir,
    batch: u64,
    logical: u64,
    history: Vec<Value>,
}
impl Comparison {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let full =
            Engine::open_with_options(dir.path().join("full"), options(CompactionStyle::Full))?;
        let tiered = Engine::open_with_options(
            dir.path().join("tiered"),
            options(CompactionStyle::SizeTiered),
        )?;
        for e in [&full, &tiered] {
            for k in 0..1024 {
                e.put(format!("key:{k:04}").as_bytes(), &[b'x'; 128])?;
            }
            e.compact()?;
        }
        let mut c = Self {
            full,
            tiered,
            _dir: dir,
            batch: 0,
            logical: 1024 * 136,
            history: vec![],
        };
        c.history.push(c.point());
        Ok(c)
    }
    fn point(&self) -> Value {
        let f = self.full.stats();
        let t = self.tiered.stats();
        json!({"batch":self.batch,"full":f.flush_bytes+f.compaction_output_bytes,"tiered":t.flush_bytes+t.compaction_output_bytes})
    }
    fn step(&mut self) -> Result<()> {
        if self.batch >= 24 {
            return Err("comparison finished; start a new comparison".into());
        }
        for e in [&self.full, &self.tiered] {
            for k in 0..64 {
                let key = format!("key:{:04}", (self.batch * 64 + k) % 128);
                e.put(key.as_bytes(), &[b'a' + self.batch as u8; 128])?;
            }
            e.flush()?;
        }
        self.batch += 1;
        self.logical += 64 * 136;
        // Verify the live engines agree after each maintenance barrier.
        for k in 0..128 {
            let key = format!("key:{k:04}");
            if self.full.get(key.as_bytes())? != self.tiered.get(key.as_bytes())? {
                return Err("comparison verification failed".into());
            }
        }
        self.history.push(self.point());
        Ok(())
    }
    fn view(&self) -> Result<Value> {
        Ok(
            json!({"batch":self.batch,"total_batches":24,"logical_bytes":self.logical,
            "full":snapshot(&self.full)?,"tiered":snapshot(&self.tiered)?,"history":self.history}),
        )
    }
}
struct App {
    db: Option<Engine>,
    _dir: tempfile::TempDir,
    comparison: Option<Comparison>,
    events: VecDeque<Value>,
    event_id: u64,
    writes: u64,
}
impl App {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let db = Engine::open_with_options(dir.path(), options(CompactionStyle::SizeTiered))?;
        Ok(Self {
            db: Some(db),
            _dir: dir,
            comparison: None,
            events: VecDeque::new(),
            event_id: 0,
            writes: 0,
        })
    }
    fn db(&self) -> Result<&Engine> {
        self.db
            .as_ref()
            .ok_or_else(|| "database is closed; try reopen".into())
    }
    fn event(&mut self, action: &str, detail: String, micros: u128) {
        self.event_id += 1;
        self.events.push_front(
            json!({"id":self.event_id,"action":action,"detail":detail,"micros":micros}),
        );
        self.events.truncate(40);
    }
    fn view(&self) -> Result<Value> {
        let mut v = snapshot(self.db()?)?;
        v["events"] = json!(self.events);
        v["memtable_limit"] = json!(65536);
        v["writes_remaining"] = json!(10000 - self.writes);
        v["wal_bytes"] = json!(std::fs::read_dir(self._dir.path().join("wal"))?
            .try_fold(0u64, |n, e| -> std::io::Result<u64> {
                Ok(n + e?.metadata()?.len())
            })?);
        Ok(v)
    }
    fn action(&mut self, name: &str, body: Value) -> Result<Value> {
        let start = Instant::now();
        let text = |field: &str| -> Result<&str> {
            body[field]
                .as_str()
                .ok_or_else(|| format!("missing {field}").into())
        };
        let detail = match name {
            "put" | "delete" => {
                if self.writes >= 10000 {
                    return Err("session write limit reached; start a new sandbox".into());
                }
                let key = text("key")?;
                if key.is_empty() {
                    return Err("enter a key".into());
                }
                if name == "put" {
                    self.db()?.put(key.as_bytes(), text("value")?.as_bytes())?;
                } else {
                    self.db()?.delete(key.as_bytes())?;
                }
                self.writes += 1;
                format!("{key} · WAL synced, then memtable updated")
            }
            "get" => {
                let key = text("key")?;
                let value = self.db()?.get(key.as_bytes())?;
                self.event(
                    "GET",
                    format!(
                        "{key} · {}",
                        if value.is_some() {
                            "found"
                        } else {
                            "not found"
                        }
                    ),
                    start.elapsed().as_micros(),
                );
                return Ok(
                    json!({"value":value.as_ref().map(|v|String::from_utf8_lossy(v)),"state":self.view()?}),
                );
            }
            "flush" => {
                self.db()?.flush()?;
                "Memtable persisted; eligible size-tier merges drained".into()
            }
            "compact" => {
                self.db()?.compact()?;
                "All live SSTs merged; obsolete versions and tombstones reclaimed".into()
            }
            "reopen" => {
                self.db.take();
                self.db = Some(Engine::open_with_options(
                    self._dir.path(),
                    options(CompactionStyle::SizeTiered),
                )?);
                "Closed all handles and reopened the same directory; counters restarted".into()
            }
            "batch" => {
                if self.writes + 64 > 10000 {
                    return Err("session write limit reached".into());
                }
                for k in 0..64 {
                    self.db()?.put(
                        format!("sample:{k:03}").as_bytes(),
                        format!("version-{}-{}", self.writes / 64, "x".repeat(96)).as_bytes(),
                    )?;
                }
                self.writes += 64;
                "64 synced writes to sample:000–063; flush to create an SST".into()
            }
            "reset" => {
                self.db.take();
                self.comparison = None;
                std::fs::remove_dir_all(self._dir.path())?;
                std::fs::create_dir_all(self._dir.path())?;
                self.db = Some(Engine::open_with_options(
                    self._dir.path(),
                    options(CompactionStyle::SizeTiered),
                )?);
                self.events.clear();
                self.event_id = 0;
                self.writes = 0;
                "Temporary sandbox cleared".into()
            }
            _ => return Err("unknown action".into()),
        };
        self.event(&name.to_uppercase(), detail, start.elapsed().as_micros());
        Ok(json!({"state":self.view()?}))
    }
}
fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).unwrap()
}
fn respond(req: Request, status: u16, mime: &str, body: String) {
    let response=Response::from_string(body).with_status_code(status)
        .with_header(header("Content-Type",mime)).with_header(header("Cache-Control","no-store"))
        .with_header(header("X-Content-Type-Options","nosniff"))
        .with_header(header("Content-Security-Policy","default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'"));
    let _ = req.respond(response);
}
fn handle(app: &mut App, mut req: Request, authority: &str) {
    let method = req.method().clone();
    let url = req.url().to_string();
    let host = req
        .headers()
        .iter()
        .find(|h| h.field.equiv("Host"))
        .map(|h| h.value.as_str());
    if host != Some(authority) && host != Some(&authority.replace("127.0.0.1", "localhost")) {
        respond(
            req,
            403,
            "application/json",
            json!({"error":"invalid host"}).to_string(),
        );
        return;
    }
    if method == Method::Get {
        let asset = match url.as_str() {
            "/" => Some(("text/html; charset=utf-8", include_str!("index.html"))),
            "/app.css" => Some(("text/css", include_str!("app.css"))),
            "/app.js" => Some(("text/javascript", include_str!("app.js"))),
            _ => None,
        };
        if let Some((mime, data)) = asset {
            respond(req, 200, mime, data.into());
            return;
        }
    }
    let result = (|| -> Result<Value> {
        if method == Method::Get && url == "/api/state" {
            return app.view();
        }
        if method == Method::Get && url.starts_with("/api/table/") {
            let id = url.trim_start_matches("/api/table/").parse()?;
            return Ok(
                json!({"id":id,"records":app.db()?.inspect_table(id)?.into_iter().map(record).collect::<Vec<_>>()}),
            );
        }
        if method != Method::Post {
            return Err("unknown route".into());
        }
        if req
            .headers()
            .iter()
            .find(|h| h.field.equiv("X-KV-Demo"))
            .map(|h| h.value.as_str())
            != Some("1")
        {
            return Err("missing demo request header".into());
        }
        if let Some(origin) = req.headers().iter().find(|h| h.field.equiv("Origin")) {
            let expected = format!("http://{authority}");
            if origin.value.as_str() != expected
                && origin.value.as_str() != expected.replace("127.0.0.1", "localhost")
            {
                return Err("invalid origin".into());
            }
        }
        if req.body_length().is_none_or(|n| n > 8192) {
            return Err("request body must be at most 8 KiB".into());
        }
        let mut data = String::new();
        req.as_reader().take(8193).read_to_string(&mut data)?;
        if data.len() > 8192 {
            return Err("request too large".into());
        }
        let body: Value = serde_json::from_str(&data)?;
        match url.as_str() {
            "/api/compare/start" => {
                app.comparison = None;
                app.comparison = Some(Comparison::new()?);
                app.comparison.as_ref().unwrap().view()
            }
            "/api/compare/step" => {
                let c = app.comparison.as_mut().ok_or("start a comparison first")?;
                c.step()?;
                c.view()
            }
            _ => app.action(url.strip_prefix("/api/").ok_or("unknown route")?, body),
        }
    })();
    match result {
        Ok(value) => respond(req, 200, "application/json", value.to_string()),
        Err(e) => respond(
            req,
            400,
            "application/json",
            json!({"error":e.to_string()}).to_string(),
        ),
    }
}
pub fn run() -> Result<()> {
    let port = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "8080".into())
        .parse::<u16>()?;
    let server = Server::http(SocketAddr::from(([127, 0, 0, 1], port)))?;
    let authority = server.server_addr().to_string();
    let mut app = App::new()?;
    println!("Storage Lab: http://{authority}");
    println!(
        "Temporary sandbox: {} (removed on normal exit)",
        app._dir.path().display()
    );
    for request in server.incoming_requests() {
        handle(&mut app, request, &authority);
    }
    Ok(())
}
