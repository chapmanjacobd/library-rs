use rusqlite::{Connection, Result};
use std::io::Read;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

#[test]
fn test_program_completes_and_db_has_rows_v2() -> Result<()> {
    let db_path = "test.db";
    let _ = std::fs::remove_file(db_path);

    let mut child = Command::new("./target/release/library-rs")
        .arg(db_path)
        .arg(".")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start program");

    let timeout = Duration::from_secs(20);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if let Some(status) = child.try_wait().expect("Failed to poll child process") {
            assert!(status.success(), "Program did not exit successfully");

            let mut out = String::new();
            let mut err = String::new();
            child.stdout.take().unwrap().read_to_string(&mut out).ok();
            child.stderr.take().unwrap().read_to_string(&mut err).ok();

            println!("stdout: {}", out);
            eprintln!("stderr: {}", err);

            break;
        }
        thread::sleep(Duration::from_millis(500));
    }

    if child.try_wait().unwrap().is_none() {
        child.kill().expect("Failed to kill process on timeout");
        panic!("Program exceeded 20 second timeout");
    }

    let conn = Connection::open(db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM media", [], |row| row.get(0))?;
    assert!(count > 0, "Database has no rows");

    Ok(())
}
