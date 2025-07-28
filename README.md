# Usage:
```bash
report.exe -s 2025-05-01 -f 2025-05-31 -r report-2025-05.xls -t tali-2025-05.xls -p playlists -e hershim
```
where:
* s - start date (from 00:00)
* f - finish date (to 23.59)
* r - report file name
* t - tali file name
* p - path to playlists directory
* e - path to hershim directory

# Compile
```bash
GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -trimpath -o report.exe report.go
```

## This Go program is a broadcast report generator that processes TV/media playlist files to create
Excel reports. Here's my analysis:

### Main Purpose

Generates two Excel reports from media playlist files:
- Report: Detailed broadcast schedule with prime time analysis, color coding
- Tali: Simplified broadcast listing

### Key Components

#### Data Structures

- Event: Represents a broadcast event with metadata (name, title, length, subtitles, etc.)
- Playlist: Manages parsing multiple playlist files and database operations

#### Core Features

1. Time Analysis: Categorizes broadcasts by time slots:
   - Prime time (19:00-23:00) - blue highlighting
   - Illegal time (01:00-06:00) - grey highlighting
   - Saturday broadcasts - yellow highlighting
   - Short content (<10min) - red highlighting
2. Subtitle Detection: Identifies Hebrew subtitle files ("hershim") by matching filenames
3. Excel Output: Generates XML-formatted Excel files with:
   - Color-coded rows based on broadcast type
   - Hebrew column headers
   - Duration calculations in minutes/seconds
4. Database Integration: SQLite database for unique ID generation per media file

#### Input Processing

- Parses Windows-1255 encoded playlist files (.ply)
- Extracts metadata: LISTID, METADATA Name/Title, TAPEID, PERFORMER timestamps
- Handles live streams and wait events
- Processes file paths and timing information

### Command Line Options

- Date range filtering (-s start, -f finish)
- Custom output filenames (-r report, -t tali)
- Directory paths for playlists (-p) and subtitles (-e)
- Verbose logging (-v, -V)

This is a legitimate media broadcasting analysis tool, likely used by a Hebrew-language
TV station to track broadcast compliance and generate regulatory reports.
