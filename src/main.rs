use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use rand::Rng;
use rayon::prelude::*;
use reqwest::blocking::Client;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_DB: &str = "app-data/store.db";
const DEFAULT_CHUNK_SIZE: usize = 7_000_000;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Split files into chunks, store in SQLite, and reconstruct."
)]
struct Cli {
    #[arg(long, default_value = DEFAULT_DB)]
    db: PathBuf,

    #[arg(long, short)]
    webhook: String,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create tables if they don't exist
    Init,
    /// Ingest a file into the database as 2MB chunks
    Ingest {
        /// Path to the file to ingest
        #[arg(long, short)]
        path: PathBuf,
        /// Optional override for chunk size in bytes
        #[arg(long)]
        chunk_size: Option<usize>,
    },
    /// List stored files
    List,
    /// List directories
    ListDirs,
    ListFileInDir {
        /// ID of the directory
        #[arg(long)]
        dir_id: i64,
    },
    /// Export (reconstruct) a stored file by ID
    Export {
        /// ID from the `files` table
        #[arg(long)]
        file_id: i64,
        /// Output path to write the reconstructed file
        #[arg(long, short)]
        out: PathBuf,
    },
    /// Verify checksums of chunks for a file
    Verify {
        /// ID from the `files` table
        #[arg(long)]
        file_id: i64,
    },
    /// Move file to a directory
    MoveFile {
        /// ID from the `files` table
        #[arg(long)]
        file_id: i64,
        /// ID of the directory
        #[arg(long)]
        dir_id: i64,
    },
    /// Create new directory
    CreateDir {
        /// Directory name
        #[arg(long)]
        name: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = Path::new("app-data");
    if !data_dir.exists() {
        fs::create_dir_all(data_dir)?;
    }
    let mut conn =
        Connection::open(&cli.db).with_context(|| format!("opening db: {}", cli.db.display()))?;

    match cli.cmd {
        Commands::Init => {
            init_schema(&mut conn)?;
            println!("Database initialized at {}", cli.db.display());
        }
        Commands::Ingest { path, chunk_size } => {
            init_schema(&mut conn)?;
            let webhook = cli.webhook.as_str();
            let file_id = ingest_file(
                &mut conn,
                &path,
                chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE),
                &webhook,
            )?;
            println!("Ingested '{}' with file_id={}", path.display(), file_id);
        }
        Commands::List => {
            list_files(&mut conn)?;
        }
        Commands::Export { file_id, out } => {
            let proxy_base = std::env::var("PROXY_BASE").expect("PROXY_BASE must be set.");
            export_file(&mut conn, file_id, &proxy_base, Some(out))?;
        }
        Commands::Verify { file_id } => {
            verify_file(&mut conn, file_id)?;
        }
        Commands::CreateDir { name } => {
            let id = create_directory(&mut conn, &name.as_str(), None)?;
            println!("Created directory '{}' with id {}", name, id);
        }
        Commands::ListDirs => {
            for (id, name) in list_directories(&conn, None)? {
                println!("{} - {}", id, name);
            }
        }
        Commands::MoveFile { file_id, dir_id } => {
            move_file_to_directory(&mut conn, file_id, Some(dir_id))?;
        }
        Commands::ListFileInDir { dir_id } => {
            list_files_in_directory(&conn, Some(dir_id))?;
        }
    }

    Ok(())
}

fn list_files(conn: &mut Connection) -> Result<()> {
    let mut stmt = conn
        .prepare("SELECT id, filename, filesize, chunk_size, created_at FROM files ORDER BY id")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;

    for row in rows {
        let (id, filename, size, chunk_size, created_at) = row?;
        println!(
            "id={:<3} size={:<10} chunk_size={:<7} created_at={} file={} ",
            id, size, chunk_size, created_at, filename
        );
    }
    Ok(())
}

fn export_file(
    conn: &mut Connection,
    file_id: i64,
    proxy_base: &str,
    out: Option<PathBuf>,
) -> Result<()> {
    // fetch original filename
    let mut stmt = conn.prepare("SELECT filename FROM files WHERE id = ?1")?;
    let filename: String = stmt.query_row(params![file_id], |row| row.get(0))?;

    // prepare output writer
    let mut out_writer: Box<dyn Write> = if let Some(path) = out {
        if path.to_string_lossy() == "-" {
            Box::new(std::io::stdout())
        } else {
            Box::new(File::create(path)?)
        }
    } else {
        Box::new(File::create(filename)?)
    };

    // query chunks
    let mut stmt =
        conn.prepare("SELECT idx, url FROM file_chunks WHERE file_id = ?1 ORDER BY idx ASC")?;
    let rows = stmt.query_map(params![file_id], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;

    let client = Client::new();
    for row in rows {
        let (idx, url) = row?;
        // wrap original discord cdn url with proxy
        let proxied_url = format!("{proxy_base}/?{url}");

        eprintln!("Downloading chunk {idx} via {proxied_url}");
        let mut resp = client.get(&proxied_url).send()?;
        std::io::copy(&mut resp, &mut out_writer)?;
    }

    Ok(())
}

fn ingest_file(
    conn: &mut Connection,
    path: &Path,
    chunk_size: usize,
    webhook: &str,
) -> Result<i64> {
    let mut f = File::open(path)?;
    let filesize = f.metadata()?.len() as i64;
    let filename = path.file_name().unwrap().to_string_lossy().to_string();

    conn.execute(
        "INSERT INTO files (filename, filesize, chunk_size, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            filename,
            filesize,
            chunk_size as i64,
            Utc::now().to_rfc3339()
        ],
    )?;
    let file_id = conn.last_insert_rowid();

    // Prepare storage directory
    let dir = PathBuf::from("storage").join(file_id.to_string());
    fs::create_dir_all(&dir)?;

    // Read all chunks into memory first
    let mut chunks: Vec<(usize, Vec<u8>)> = Vec::new();
    let mut buffer = vec![0u8; chunk_size];
    let mut idx = 0;
    loop {
        let n = f.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        chunks.push((idx, buffer[..n].to_vec()));
        idx += 1;
    }

    let client = Client::new();
    let results: Arc<Mutex<Vec<(usize, (String, String))>>> = Arc::new(Mutex::new(Vec::new()));

    // Limit parallelism to avoid burst (e.g. 3 concurrent uploads)
    chunks.chunks(3).for_each(|chunk_group| {
        chunk_group.par_iter().for_each(|(idx, data)| {
            let chunk_path = dir.join(format!("{}.chunk", idx));
            let mut chunk_file = File::create(&chunk_path).unwrap();
            chunk_file.write_all(&data).unwrap();

            match upload_chunk_with_retry(&client, webhook, &chunk_path, *idx) {
                Ok(res) => {
                    results.lock().unwrap().push(res);
                }
                Err(e) => {
                    eprintln!("[Chunk {}] Failed permanently: {}", idx, e);
                }
            }

            // Add a random delay after each upload to spread requests
            let delay = rand::rng().random_range(2..=6);
            thread::sleep(Duration::from_secs(delay));
        });
    });

    // Insert results sequentially
    let mut results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
    results.sort_by_key(|(idx, _)| *idx);
    for (idx, data) in results {
        conn.execute(
            "INSERT INTO file_chunks (file_id, idx, message_id, url) VALUES (?1, ?2, ?3, ?4)",
            params![file_id, idx as i64, data.0, data.1],
        )?;
    }

    Ok(file_id)
}

fn upload_chunk_with_retry(
    client: &Client,
    webhook: &str,
    chunk_path: &Path,
    idx: usize,
) -> Result<(usize, (String, String))> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let form = reqwest::blocking::multipart::Form::new().file("file", chunk_path)?;
        let resp = client.post(webhook).multipart(form).send();

        match resp {
            Ok(r) => {
                if r.status().as_u16() == 429 {
                    // Rate limited, sleep and retry
                    let delay = rand::rng().random_range(5..=15);
                    eprintln!("[Chunk {}] Rate limited. Sleeping {}s", idx, delay);
                    thread::sleep(Duration::from_secs(delay));
                    continue;
                }
                let json: serde_json::Value = r.json()?;
                let message_id = json["id"].as_str().unwrap().to_string();
                let url = json["attachments"][0]["url"].as_str().unwrap().to_string();
                return Ok((idx, (message_id, url)));
            }
            Err(e) => {
                if attempts < 5 {
                    let delay = 2u64.pow(attempts);
                    eprintln!(
                        "[Chunk {}] Upload failed: {}. Retrying in {}s",
                        idx, e, delay
                    );
                    thread::sleep(Duration::from_secs(delay));
                    continue;
                } else {
                    return Err(e.into());
                }
            }
        }
    }
}

fn init_schema(conn: &mut Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            filesize INTEGER NOT NULL,
            chunk_size INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_chunks (
            file_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            url TEXT NOT NULL,
            message_id TEXT NOT NULL,
            PRIMARY KEY(file_id, idx)
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS directories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            parent_id INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY(parent_id) REFERENCES directories(id)
        )",
        [],
    )?;
    Ok(())
}

fn create_directory(conn: &mut Connection, name: &str, parent_id: Option<i64>) -> Result<i64> {
    conn.execute(
        "INSERT INTO directories (name, parent_id, created_at) VALUES (?1, ?2, datetime('now'))",
        params![name, parent_id],
    )?;
    Ok(conn.last_insert_rowid())
}

fn list_files_in_directory(conn: &Connection, dir_id: Option<i64>) -> Result<Vec<(i64, String)>> {
    let mut stmt = match dir_id {
        Some(_) => conn.prepare("SELECT id, filename FROM files WHERE id = ?1"),
        None => conn.prepare("SELECT id, filename FROM files WHERE id IS NULL"),
    }?;

    let rows: Vec<(i64, String)> = match dir_id {
        Some(did) => stmt
            .query_map(params![did], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?,
        None => stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?,
    };

    Ok(rows)
}

fn move_file_to_directory(conn: &mut Connection, file_id: i64, dir_id: Option<i64>) -> Result<()> {
    conn.execute(
        "UPDATE files SET directory_id = ?1 WHERE id = ?2",
        params![dir_id, file_id],
    )?;
    Ok(())
}

fn list_directories(conn: &Connection, parent_id: Option<i64>) -> Result<Vec<(i64, String)>> {
    let mut stmt = match parent_id {
        Some(_) => conn.prepare("SELECT id, name FROM directories WHERE parent_id = ?1"),
        None => conn.prepare("SELECT id, name FROM directories WHERE parent_id IS NULL"),
    }?;

    let rows: Vec<(i64, String)> = match parent_id {
        Some(pid) => stmt
            .query_map(params![pid], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?,
        None => stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?,
    };

    Ok(rows)
}

fn verify_file(conn: &mut Connection, file_id: i64) -> Result<()> {
    // return true if all chunk hashes match recomputed hashes
    let mut ok_all = true;
    let mut stmt =
        conn.prepare("SELECT idx, data, sha256 FROM chunks WHERE file_id = ?1 ORDER BY idx ASC")?;
    let mut rows = stmt.query(params![file_id])?;
    while let Some(row) = rows.next()? {
        let idx: i64 = row.get(0)?;
        let data: Vec<u8> = row.get(1)?;
        let stored: String = row.get(2)?;
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let calc = hex::encode(hasher.finalize());
        if calc != stored {
            println!("Chunk {}: MISMATCH (stored={}, calc={})", idx, stored, calc);
            ok_all = false;
        }
    }
    if ok_all {
        println!("All chunks verified for file_id={}", file_id);
    }
    Ok(())
}
