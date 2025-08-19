# File Chunker with SQLite & Discord Storage

This project is a Rust application that splits large files into chunks, stores their metadata in SQLite, and uploads the chunks to Discord using webhooks. It also supports exporting files, streaming them, and organizing files into directories.

---

## Features

* Split files into chunks (default 7 MB each)
* Upload chunks to Discord via webhook
* Store file and chunk metadata in SQLite
* Export or stream files directly from stored chunks
* Directory support for organizing files
* Automatic `app-data` folder creation for database

---

## Requirements

* Rust (latest stable)
* SQLite3
* Discord webhook URL
* Discord CDN proxy. click here for [example](https://github.com/vader-pepe/discord-cdn-proxy)

---

## Installation

Clone the repository:

```bash
git clone https://github.com/vader-pepe/octo-potato.git
cd octo-potato
```

Build the project:

```bash
cargo build --release
```

---

## Usage

### Initialize Database

```bash
./target/release/octo-potato init
```

This will create the `app-data` folder (if not exists) and initialize the schema.

### Ingest File

```bash
./target/release/octo-potato ingest --path /path/to/file --chunk-size 2_000_000
```

Splits the file into chunks, uploads them to Discord, and stores metadata.

### List Files

```bash
./target/release/octo-potato list
```

Lists all files stored in the database.

### Export File

```bash
./target/release/octo-potato export --file-id 1 --out output.mp4
```

Reassembles and saves the file locally.

Use `--out -` to stream to stdout (e.g., pipe to VLC):

```bash
./target/release/octo-potato export --db app-data/files.db --file-id 1 --out - | vlc -
```

### Directories

* Create a directory:

```bash
./target/release/octo-potato create-dir --db app-data/files.db --name music
```

* Move a file into a directory:

```bash
./target/release/octo-potato move-file --db app-data/files.db --file-id 1 --dir-id 2
```

* List files in a directory:

```bash
./target/release/octo-potato list-file-in-dir --db app-data/files.db --dir-id 2
```

---

## License

MIT License

