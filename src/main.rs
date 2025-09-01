use clap::Parser;
use magic::cookie::{Flags, Load};
use magic::Cookie;
use rayon::prelude::*;
use rusqlite::{params_from_iter, Connection, Result, ToSql};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

/// static link magic for Windows
#[cfg(target_os = "windows")]
#[link(name = "magic", kind = "static")]
extern "C" {}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// SQLite database path
    #[clap(required = true)]
    db: String,

    /// One or more paths to scan
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

/// Gather metadata only (parallel-safe)
fn gather_metadata(entry: walkdir::DirEntry) -> Option<FileInfo> {
    let metadata = fs::metadata(entry.path()).ok()?;

    if !entry.file_type().is_file() {
        return None;
    }
    let path = entry.path();
    let time_created = metadata
        .created()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs());
    let time_modified = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs());
    let canonical_path = fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string();

    Some(FileInfo {
        path: canonical_path,
        size: metadata.len(),
        time_created,
        time_modified,
        file_type: None, // filled later
    })
}

/// Sequential libmagic worker
fn run_magic_worker(
    rx: mpsc::Receiver<FileInfo>,
    tx: mpsc::Sender<FileInfo>,
) {
    let cookie = Cookie::open(Flags::MIME | Flags::PRESERVE_ATIME)
        .expect("failed to open libmagic cookie")
        .load(&Default::default())
        .expect("failed to load default magic db");

    for mut info in rx {
        info.file_type = cookie.file(&info.path).ok().or_else(|| {
            Path::new(&info.path)
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| format!("extension/{}", ext))
        });
        tx.send(info).expect("sending to main thread failed");
    }
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

fn insert_all(tx: &rusqlite::Transaction<'_>, files: Vec<FileInfo>) -> Result<()> {
    const MAX_PARAMS: usize = 32766;
    const PARAMS_PER_ROW: usize = 5;
    const MAX_ROWS: usize = MAX_PARAMS / PARAMS_PER_ROW;
    for chunk in files.chunks(MAX_ROWS) {
        insert_batch(tx, chunk)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let db_path = Path::new(&args.db);
    let mut conn = Connection::open(db_path)?;
    create_table(&conn)?;
    conn.execute("PRAGMA synchronous = OFF", [])?;
    let _wal_mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;

    // Gather entries
    let entries: Vec<_> = args
        .paths
        .iter()
        .flat_map(|p| WalkDir::new(p).into_iter().filter_map(Result::ok))
        .collect();

    // Parallel metadata collection
    let metadata_files: Vec<FileInfo> = entries
        .into_par_iter()
        .filter_map(gather_metadata)
        .collect();

    // Set up channels: main -> magic worker -> main
    let (meta_tx, magic_rx) = mpsc::channel();
    let (magic_tx, main_rx) = mpsc::channel();

    // Launch magic worker
    thread::spawn(move || run_magic_worker(magic_rx, magic_tx));

    // Send metadata to magic worker
    for info in metadata_files {
        meta_tx.send(info).expect("sending to magic worker failed");
    }
    drop(meta_tx); // close channel so worker exits

    // Collect results from magic worker
    let mut final_files = Vec::new();
    for info in main_rx {
        final_files.push(info);
    }

    // Insert into SQLite
    let tx = conn.transaction()?;
    insert_all(&tx, final_files)?;
    tx.commit()?;

    Ok(())
}

