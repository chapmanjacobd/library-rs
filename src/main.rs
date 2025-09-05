use clap::Parser;
use magic::cookie::{Flags};
use magic::Cookie;
use rusqlite::{params_from_iter, Connection, Result, ToSql};
use std::fs;
use std::path::{Path};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Instant, UNIX_EPOCH};
use walkdir::WalkDir;

const STAT_THREADS: usize = 32;
const MAGIC_BATCH_SIZE: usize = 128;
const SQLITE_BATCH_SIZE: usize = 512;

/// static link magic for Windows
#[cfg(target_os = "windows")]
#[link(name = "magic", kind = "static")]
extern "C" {}

#[derive(Parser, Debug)]
struct Args {
    #[clap(required = true)]
    db: String,
    #[clap(required = true, num_args = 1..)]
    paths: Vec<String>,
}

#[derive(Debug)]
struct FileInfo {
    path: String,
    size: u64,
    time_created: u64,
    time_modified: u64,
    file_type: Option<String>,
}

fn create_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY,
            time_created INTEGER DEFAULT (strftime('%s', 'now')),
            time_modified INTEGER,
            size INTEGER,
            path TEXT NOT NULL UNIQUE,
            type TEXT
        )",
        [],
    )?;
    Ok(())
}

fn insert_batch(tx: &rusqlite::Transaction<'_>, batch: &[FileInfo]) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let values: Vec<String> = (0..batch.len())
        .map(|_| "(?, ?, ?, ?, ?)".to_string())
        .collect();
    let sql = format!(
        "INSERT OR REPLACE INTO media (path, size, time_created, time_modified, type) VALUES {}",
        values.join(", ")
    );

    let mut params: Vec<&dyn ToSql> = Vec::with_capacity(batch.len() * 5);
    for file in batch {
        params.push(&file.path);
        params.push(&file.size);
        params.push(&file.time_created);
        params.push(&file.time_modified);
        params.push(&file.file_type);
    }
    tx.execute(&sql, params_from_iter(params))?;
    Ok(())
}

/// processes batches sequentially
fn magic_worker(rx: mpsc::Receiver<Vec<FileInfo>>, tx: mpsc::Sender<Vec<FileInfo>>) {
    let cookie = Cookie::open(Flags::MIME | Flags::PRESERVE_ATIME)
        .expect("failed to open libmagic")
        .load(&Default::default())
        .expect("failed to load magic db");

    let start = Instant::now();
    let mut total_files = 0;

    for mut batch in rx {
        for info in &mut batch {
            info.file_type = cookie.file(&info.path).ok().or_else(|| {
                Path::new(&info.path)
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| format!("extension/{}", ext))
            });
        }
        total_files += batch.len();
        tx.send(batch)
            .expect("sending batch to SQLite inserter failed");
    }
    println!(
        "Magic processing finished {} files in {:.2?}",
        total_files,
        start.elapsed()
    );
}

fn stat_worker(
    entry_rx: Arc<Mutex<mpsc::Receiver<walkdir::DirEntry>>>,
    batch_tx: mpsc::Sender<Vec<FileInfo>>,
) {
    let mut batch = Vec::with_capacity(MAGIC_BATCH_SIZE);
    while let Ok(entry) = entry_rx.lock().unwrap().recv() {
        if let Ok(metadata) = fs::metadata(entry.path()) {
            if metadata.is_file() {
                let info = FileInfo {
                    path: fs::canonicalize(entry.path())
                        .unwrap_or_else(|_| entry.path().to_path_buf())
                        .to_string_lossy()
                        .to_string(),
                    size: metadata.len(),
                    time_created: metadata
                        .created()
                        .ok()
                        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .map_or(0, |d| d.as_secs()),
                    time_modified: metadata
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .map_or(0, |d| d.as_secs()),
                    file_type: None,
                };
                batch.push(info);

                if batch.len() >= MAGIC_BATCH_SIZE {
                    batch_tx.send(batch).unwrap();
                    batch = Vec::with_capacity(MAGIC_BATCH_SIZE);
                }
            }
        }
    }
    if !batch.is_empty() {
        batch_tx.send(batch).unwrap();
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut conn = Connection::open(&args.db)?;
    create_table(&conn)?;
    conn.execute("PRAGMA synchronous = OFF", [])?;
    let _wal: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;

    let start_total = Instant::now();

    // Channels
    let (entry_tx, entry_rx) = mpsc::channel::<walkdir::DirEntry>();
    let (magic_batch_tx, magic_batch_rx) = mpsc::channel::<Vec<FileInfo>>();
    let (sqlite_tx, sqlite_rx) = mpsc::channel::<Vec<FileInfo>>();

    // Spawn magic worker
    thread::spawn(move || magic_worker(magic_batch_rx, sqlite_tx));

    // Spawn stat workers
    let entry_rx = Arc::new(Mutex::new(entry_rx));
    let mut handles = Vec::new();
    let start_stat = Instant::now();
    for _ in 0..STAT_THREADS {
        let rx_clone = Arc::clone(&entry_rx);
        let batch_tx_clone = magic_batch_tx.clone();
        handles.push(thread::spawn(move || stat_worker(rx_clone, batch_tx_clone)));
    }

    // Walk directories in main thread
    let start_walk = Instant::now();
    let mut walk_count = 0;
    for path in &args.paths {
        for entry in WalkDir::new(path).into_iter().filter_map(Result::ok) {
            walk_count += 1;
            entry_tx.send(entry).unwrap();
        }
    }
    drop(entry_tx); // Close channel for stat workers
    drop(magic_batch_tx); // Close batch channel after stat workers finish
    println!(
        "Directory walk done: {} entries in {:.2?}",
        walk_count,
        start_walk.elapsed()
    );

    for h in handles {
        h.join().unwrap();
    }
    println!("Stat workers finished in {:.2?}", start_stat.elapsed());

    // SQLite insertion
    let start_sqlite = Instant::now();
    let tx = conn.transaction()?;
    let mut sqlite_batch = Vec::with_capacity(SQLITE_BATCH_SIZE);
    let mut total_inserted = 0;
    for batch in sqlite_rx {
        for info in batch {
            sqlite_batch.push(info);
            if sqlite_batch.len() >= SQLITE_BATCH_SIZE {
                insert_batch(&tx, &sqlite_batch)?;
                total_inserted += sqlite_batch.len();
                sqlite_batch.clear();
            }
        }
    }
    if !sqlite_batch.is_empty() {
        insert_batch(&tx, &sqlite_batch)?;
        total_inserted += sqlite_batch.len();
    }
    tx.commit()?;
    println!(
        "SQLite insertion finished {} files in {:.2?}",
        total_inserted,
        start_sqlite.elapsed()
    );

    println!("Total elapsed time: {:.2?}", start_total.elapsed());
    Ok(())
}
