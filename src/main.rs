use clap::Parser;
use crossterm::{cursor, style::Print, terminal, ExecutableCommand, QueueableCommand};
use magic::cookie::Flags;
use magic::Cookie;
use rusqlite::{params, Connection, Result};
use std::fs;
use std::io::{stderr, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::{sync::mpsc as tokio_mpsc, task::spawn_blocking};
use walkdir::WalkDir;

const MAGIC_BATCH_SIZE: usize = 128;
const SQLITE_BATCH_SIZE: usize = 32;
const CHANNEL_CAPACITY: usize = 512;

#[derive(Parser, Debug)]
struct Args {
    #[clap(required = true)]
    db: String,

    #[clap(required = true, num_args = 1..)]
    paths: Vec<String>,
}

#[derive(Debug, Clone)]
struct FileInfo {
    path: String,
    size: u64,
    time_created: u64,
    time_modified: u64,
    file_type: Option<String>,
}

static FILES_SCANNED: AtomicUsize = AtomicUsize::new(0);
static MIME_PROCESSED: AtomicUsize = AtomicUsize::new(0);
static SQLITE_INSERTED: AtomicUsize = AtomicUsize::new(0);

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
        "INSERT OR REPLACE INTO media (path, size, time_created, time_modified, type) VALUES (?, ?, ?, ?, ?)"
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

async fn mime_worker_libmagic_async(
    mut rx: tokio_mpsc::Receiver<Vec<FileInfo>>,
    tx: tokio_mpsc::Sender<Vec<FileInfo>>,
) {
    // https://github.com/robo9k/rust-magic-sys/issues/28
    // TODO: maybe use glommio
    while let Some(batch) = rx.recv().await {
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let result_batch = spawn_blocking(move || {
                let cookie = Cookie::open(Flags::MIME | Flags::PRESERVE_ATIME)
                    .expect("failed to open libmagic")
                    .load(&Default::default())
                    .expect("failed to load magic db");

                let mut processed_batch = Vec::new();
                for mut info in batch {
                    info.file_type = cookie.file(&info.path).ok().or_else(|| {
                        Path::new(&info.path)
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .map(|ext| format!("extension/{}", ext))
                    });
                    MIME_PROCESSED.fetch_add(1, Ordering::Relaxed);
                    processed_batch.push(info);
                }
                processed_batch
            })
            .await
            .unwrap();
            let _ = tx_clone.send(result_batch).await;
        });
    }
}

async fn walk_and_stat(paths: Vec<String>, tx: tokio_mpsc::Sender<Vec<FileInfo>>) {
    let mut batch = Vec::with_capacity(MAGIC_BATCH_SIZE);
    for path in &paths {
        for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
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
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map_or(0, |t| t.as_secs()),
                        time_modified: metadata
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map_or(0, |t| t.as_secs()),
                        file_type: None,
                    };
                    batch.push(info);
                    FILES_SCANNED.fetch_add(1, Ordering::Relaxed);
                    if batch.len() >= MAGIC_BATCH_SIZE {
                        if tx.send(batch.clone()).await.is_err() {
                            break;
                        }
                        batch.clear();
                    }
                }
            }
        }
    }
    if !batch.is_empty() {
        let _ = tx.send(batch).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut conn = Connection::open(&args.db)?;
    create_table(&conn)?;
    conn.execute("PRAGMA synchronous = OFF", [])?;
    let _wal: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;

    let (progress_tx, mut progress_rx) = tokio_mpsc::channel::<()>(1);

    let progress_handle = tokio::spawn(async move {
        let mut stderr = stderr();
        let _ = stderr.queue(cursor::SavePosition).unwrap();
        let _ = stderr.execute(cursor::Hide);

        while progress_rx.recv().await.is_some() {
            let scanned = FILES_SCANNED.load(Ordering::Relaxed);
            let mime = MIME_PROCESSED.load(Ordering::Relaxed);
            let sqlite = SQLITE_INSERTED.load(Ordering::Relaxed);

            let status = format!(
                "Files scanned: {}  MIME processed: {}  SQLite inserted: {}",
                scanned, mime, sqlite
            );

            let _ = stderr.queue(cursor::RestorePosition);
            let _ = stderr.queue(terminal::Clear(terminal::ClearType::CurrentLine));
            let _ = stderr.queue(Print(status));
            let _ = stderr.flush();
        }

        let _ = stderr.execute(cursor::Show);
    });

    let (mime_batch_tx, mime_batch_rx) = tokio_mpsc::channel::<Vec<FileInfo>>(CHANNEL_CAPACITY);
    let (sqlite_tx, mut sqlite_rx) = tokio_mpsc::channel::<Vec<FileInfo>>(CHANNEL_CAPACITY);

    let stat_handle = tokio::spawn({
        let mime_batch_tx = mime_batch_tx.clone();
        async move {
            walk_and_stat(args.paths, mime_batch_tx).await;
        }
    });

    let sqlite_tx_for_mime = sqlite_tx.clone();

    let mime_handle =
        tokio::spawn(
            async move { mime_worker_libmagic_async(mime_batch_rx, sqlite_tx_for_mime).await },
        );

    let progress_tx_for_sqlite = progress_tx.clone();
    let sqlite_handle = tokio::spawn(async move {
        let mut sqlite_batch = Vec::with_capacity(SQLITE_BATCH_SIZE);
        let mut total_inserted = 0;

        while let Some(batch) = sqlite_rx.recv().await {
            sqlite_batch.extend(batch);
            if sqlite_batch.len() >= SQLITE_BATCH_SIZE {
                if let Ok(tx) = conn.transaction() {
                    if insert_batch(&tx, &sqlite_batch).is_ok() {
                        tx.commit().ok();
                        SQLITE_INSERTED.fetch_add(sqlite_batch.len(), Ordering::Relaxed);
                        total_inserted += sqlite_batch.len();
                    }
                }
                sqlite_batch.clear();
            }
            progress_tx_for_sqlite.send(()).await.ok();
        }

        if !sqlite_batch.is_empty() {
            if let Ok(tx) = conn.transaction() {
                if insert_batch(&tx, &sqlite_batch).is_ok() {
                    tx.commit().ok();
                    SQLITE_INSERTED.fetch_add(sqlite_batch.len(), Ordering::Relaxed);
                    total_inserted += sqlite_batch.len();
                }
            }
        }

        total_inserted
    });

    let progress_tx_for_update = progress_tx.clone();
    let progress_update_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            if progress_tx_for_update.send(()).await.is_err() {
                break;
            }
            interval.tick().await;
        }
    });

    let stat_result = stat_handle.await;

    drop(mime_batch_tx);
    let mime_result = mime_handle.await;

    drop(sqlite_tx);
    let sqlite_result = sqlite_handle.await;

    progress_update_handle.abort();
    drop(progress_tx);
    let _ = progress_handle.await;

    if let Err(e) = stat_result {
        eprintln!("Error in stat task: {:?}", e);
    }
    if let Err(e) = mime_result {
        eprintln!("Error in MIME task: {:?}", e);
    }
    if let Err(e) = sqlite_result {
        eprintln!("Error in SQLite task: {:?}", e);
    }

    Ok(())
}
