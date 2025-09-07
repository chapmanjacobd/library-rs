use clap::Parser;
use indicatif::{HumanCount, MultiProgress, ProgressBar, ProgressStyle};
use rusqlite::{params, Connection};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc as tokio_mpsc, Mutex};
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use tree_magic_mini as tree_magic;
use walkdir::WalkDir;

const MIME_WORKERS: usize = 4;
const CHANNEL_CAPACITY: usize = 512;
const SQLITE_BATCH_SIZE: usize = 32;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser, Debug)]
struct Args {
    #[clap(required = true)]
    db: String,

    #[clap(required = true, num_args = 1..)]
    paths: Vec<String>,

    #[clap(long)]
    no_progress: bool,
}

#[derive(Debug, Clone)]
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
    let mut stmt = tx.prepare(
        "INSERT OR REPLACE INTO media (path, size, time_created, time_modified, type) VALUES (?, ?, ?, ?, ?)",
    )?;
    for file in batch {
        stmt.execute(params![
            file.path,
            file.size,
            file.time_created,
            file.time_modified,
            file.file_type,
        ])?;
    }
    Ok(())
}

async fn walk_and_stat(
    paths: Vec<String>,
    tx: tokio_mpsc::Sender<FileInfo>,
    pb: ProgressBar,
) -> Result<()> {
    for root in paths {
        match fs::canonicalize(&root) {
            Ok(canon_root) => {
                for entry in WalkDir::new(canon_root).into_iter().filter_map(|e| e.ok()) {
                    if let Ok(metadata) = fs::metadata(entry.path()) {
                        if metadata.is_file() {
                            let info = FileInfo {
                                path: entry.path().to_string_lossy().to_string(),
                                size: metadata.len(),
                                time_created: metadata
                                    .created()
                                    .ok()
                                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                    .map_or(0, |t| t.as_secs()),
                                time_modified: metadata
                                    .modified()
                                    .ok()
                                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                    .map_or(0, |t| t.as_secs()),
                                file_type: None,
                            };
                            pb.inc(1);
                            if tx.send(info).await.is_err() {
                                error!("Receiver for MIME worker is closed, stopping walk.");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to canonicalize path {:?}: {}", root, e);
                continue;
            }
        }
    }
    pb.finish_with_message("Scan complete");
    Ok(())
}

async fn mime_worker(
    rx: Arc<Mutex<tokio_mpsc::Receiver<FileInfo>>>,
    tx: tokio_mpsc::Sender<FileInfo>,
    pb: ProgressBar,
) -> Result<()> {
    loop {
        let maybe_info = { rx.lock().await.recv().await };
        match maybe_info {
            Some(mut info) => {
                info.file_type =
                    tree_magic::from_filepath(Path::new(&info.path)).map(|s| s.to_string());
                pb.inc(1);
                if tx.send(info).await.is_err() {
                    error!("Receiver for SQLite worker is closed, stopping MIME processing.");
                    break;
                }
            }
            None => break,
        }
    }
    pb.finish_with_message("MIME complete");
    Ok(())
}

async fn sqlite_worker(
    db_path: String,
    mut rx: tokio_mpsc::Receiver<FileInfo>,
    pb: ProgressBar,
) -> Result<()> {
    let mut conn = Connection::open(&db_path)?;
    conn.execute("PRAGMA synchronous = OFF", [])?;
    let _wal: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    create_table(&conn)?;

    let mut batch = Vec::with_capacity(SQLITE_BATCH_SIZE);
    while let Some(file_info) = rx.recv().await {
        batch.push(file_info);
        if batch.len() >= SQLITE_BATCH_SIZE {
            let tx = conn.transaction()?;
            if insert_batch(&tx, &batch).is_ok() {
                tx.commit()?;
                pb.inc(batch.len() as u64);
            }
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let tx = conn.transaction()?;
        if insert_batch(&tx, &batch).is_ok() {
            tx.commit()?;
            pb.inc(batch.len() as u64);
        }
    }
    pb.finish_with_message("SQLite complete");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .finish()
        .init();

    info!("Starting file scanner with db: {}", args.db);

    // Pre-pass to count total files with optional progress
    let total_files: u64 = if args.no_progress {
        let mut count = 0;
        for root in &args.paths {
            if let Ok(canon_root) = fs::canonicalize(root) {
                for entry in WalkDir::new(canon_root).into_iter().filter_map(|e| e.ok()) {
                    if let Ok(metadata) = fs::metadata(entry.path()) {
                        if metadata.is_file() {
                            count += 1;
                        }
                    }
                }
            }
        }
        info!("Total files to process: {}", count);
        count
    } else {
        let mp_count = MultiProgress::new();
        let count_pb = mp_count.add(ProgressBar::new(u64::MAX)); // indeterminate large number
        count_pb.set_style(ProgressStyle::with_template("{msg}")?);
        count_pb.set_message("Counting files...");

        let paths = args.paths.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let mut count = 0u64;
            for root in &paths {
                if let Ok(canon_root) = fs::canonicalize(root) {
                    for entry in WalkDir::new(canon_root).into_iter().filter_map(|e| e.ok()) {
                        if let Ok(metadata) = fs::metadata(entry.path()) {
                            if metadata.is_file() {
                                count += 1;
                                if count % 50 == 0 {
                                    count_pb.set_message(format!(
                                        "Counting {} files...",
                                        HumanCount(count)
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            count_pb.finish_with_message(format!("Counted {} files", HumanCount(count)));
            count
        });

        handle.await?
    };

    // Setup main progress bars
    let style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}",
    )?
    .progress_chars("#>-");

    let (walk_pb, mime_pb, sqlite_pb);
    let _ = if args.no_progress {
        walk_pb = ProgressBar::hidden();
        mime_pb = ProgressBar::hidden();
        sqlite_pb = ProgressBar::hidden();
        None
    } else {
        let mp = MultiProgress::new();
        walk_pb = mp.add(ProgressBar::new(total_files).with_style(style.clone()));
        walk_pb.set_message("Scanning files...");

        mime_pb = mp.add(ProgressBar::new(total_files).with_style(style.clone()));
        mime_pb.set_message("Detecting MIME types...");

        sqlite_pb = mp.add(ProgressBar::new(total_files).with_style(style.clone()));
        sqlite_pb.set_message("Inserting into SQLite...");

        Some(mp)
    };

    let (walk_tx, mime_rx) = tokio_mpsc::channel(CHANNEL_CAPACITY);
    let (mime_tx, sqlite_rx) = tokio_mpsc::channel(CHANNEL_CAPACITY);
    let mime_rx = Arc::new(Mutex::new(mime_rx));

    let walk_handle = tokio::spawn(walk_and_stat(args.paths.clone(), walk_tx, walk_pb.clone()));

    let mime_handles: Vec<_> = (0..MIME_WORKERS)
        .map(|_| {
            let rx = Arc::clone(&mime_rx);
            let tx = mime_tx.clone();
            let pb = mime_pb.clone();
            tokio::spawn(mime_worker(rx, tx, pb))
        })
        .collect();

    let sqlite_handle = tokio::spawn(sqlite_worker(args.db.clone(), sqlite_rx, sqlite_pb.clone()));

    drop(mime_tx);

    // Await tasks
    let _ = walk_handle.await;
    for h in mime_handles {
        let _ = h.await;
    }
    let _ = sqlite_handle.await;

    Ok(())
}
