# File Deduplicator

A fast CLI tool that deduplicates files by content using SHA-256 hashing with concurrent processing.

## Features

- ✅ Content-based deduplication (not name-based)
- ✅ Concurrent processing with configurable workers
- ✅ Whitelist or blacklist file extensions
- ✅ Preserves file permissions
- ✅ Handles filename collisions automatically

## Installation

```bash
go build -o sorter
```

## Usage

### List Available Extensions

Scan a directory and get all file extensions in whitelist format:

```bash
./sorter extensions -input /path/to/source
```

Output example: `jpg,png,pdf,txt,doc`

### Basic Usage

```bash
./sorter -input /path/to/source -output /path/to/destination
```

### With Whitelist (only copy specific file types)

```bash
./sorter -input /path/to/source -output /path/to/destination -whitelist jpg,png,pdf
```

### With Blacklist (exclude specific file types)

```bash
./sorter -input /path/to/source -output /path/to/destination -blacklist tmp,log,cache
```

### Adjust Worker Count

```bash
./sorter -input /path/to/source -output /path/to/destination -workers 16
```

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `-input` | Yes | Source directory path |
| `-output` | Yes | Destination directory path |
| `-whitelist` | No | Comma-separated extensions to include (e.g., `jpg,png,pdf`) |
| `-blacklist` | No | Comma-separated extensions to exclude (e.g., `tmp,log`) |
| `-workers` | No | Number of concurrent workers (default: 8) |

**Note:** You cannot specify both `-whitelist` and `-blacklist` at the same time.

## Examples

Discover available file types:
```bash
./sorter extensions -input /mnt/external
# Output: avi,doc,docx,jpg,mov,mp3,mp4,pdf,png,txt
```

Copy only images:
```bash
./sorter -input /mnt/external -output ~/unique_files -whitelist jpg,jpeg,png,gif,bmp
```

Copy everything except system files:
```bash
./sorter -input /mnt/external -output ~/unique_files -blacklist tmp,log,cache,sys
```

Process with maximum speed:
```bash
./sorter -input /mnt/external -output ~/unique_files -workers 32
```
