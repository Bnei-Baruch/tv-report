package main

// To compile:
// GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -trimpath -o report.exe report.go
// GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -trimpath -o /media/sf_D_DRIVE/projects/report/report.exe report.go

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
	_ "modernc.org/sqlite"
)

var (
	superVerbose = false
	verbose      = false
)

const (
	primeTimeStart = "19:00:00"
	primeTimeEnd   = "23:00:00"
	illegalStart   = "01:00:00"
	illegalEnd     = "06:00:00"
	morningStart   = "02:45:00"
	morningEnd     = "06:00:00"
)

type Event struct {
	listid               string
	name                 string
	title                string
	length               float64
	live                 bool
	hershim              bool
	simanim              bool
	longFilename         string
	saturday             bool
	illegal              bool
	tooShort             bool
	morningLessonHershim bool
	tapeid               string
	hash                 string
	report               io.Writer
	tali                 io.Writer
}

type CompiledRegexes struct {
	programOrLesson *regexp.Regexp
	isGreen         *regexp.Regexp
	listid          *regexp.Regexp
	metadataName    *regexp.Regexp
	tapeid          *regexp.Regexp
	metadataTitle   *regexp.Regexp
	performer       *regexp.Regexp
	eventWaitto     *regexp.Regexp
	eventWait       *regexp.Regexp
	liveStreamLive  *regexp.Regexp
	hershim         *regexp.Regexp
	fileDate        *regexp.Regexp
	isLesson        *regexp.Regexp
	isLessonMlt     *regexp.Regexp
	isHershim       *regexp.Regexp
}

type Playlist struct {
	start         time.Time
	finish        time.Time
	playlistsDir  string
	hershimMap    map[string]bool
	db            *sql.DB
	currentFileID int
	selectStmt    *sql.Stmt
	insertStmt    *sql.Stmt
	batchInserts  []batchInsert
	batchMutex    sync.Mutex

	// Connection pools
	reportWriter io.Writer
	taliWriter   io.Writer

	// Memory pools
	eventPool  sync.Pool
	bufferPool sync.Pool

	// Compiled regexes
	regexes *CompiledRegexes
}

type batchInsert struct {
	filename string
	id       int
}

type OptimizedEvent struct {
	listid               string
	name                 string
	title                string
	length               float64
	live                 bool
	hershim              bool
	simanim              bool
	longFilename         string
	saturday             bool
	illegal              bool
	tooShort             bool
	morningLessonHershim bool
	tapeid               string
	hash                 string
}

type playlistEntry struct {
	path string
	date time.Time
}

type playlistJob struct {
	path string
	date time.Time
}

type playlistResult struct {
	events []OptimizedEvent
	times  []time.Time
	err    error
}

type eventWithTime struct {
	event OptimizedEvent
	time  time.Time
}

// initRegexes compiles all regular expressions once
func initRegexes() *CompiledRegexes {
	return &CompiledRegexes{
		programOrLesson: regexp.MustCompile(`_program_|_lesson_`),
		isGreen:         regexp.MustCompile(`(logo|promo|patiach|sagir|luz)`),
		listid:          regexp.MustCompile(`^#LISTID (\d+)`),
		metadataName:    regexp.MustCompile(`^#METADATA Name\s+(.*)`),
		tapeid:          regexp.MustCompile(`^#TAPEID +(.*)`),
		metadataTitle:   regexp.MustCompile(`^#METADATA Title\s+(.*)`),
		performer:       regexp.MustCompile(`^#PERFORMER\s+(.*)`),
		eventWaitto:     regexp.MustCompile(`^#EVENT WAITTO\s+(.*)`),
		eventWait:       regexp.MustCompile(`^#EVENT WAIT\s+(.*)`),
		liveStreamLive:  regexp.MustCompile(`^#LIVE_STREAM LIVE.+;(.*)`),
		hershim:         regexp.MustCompile(`_hrsh$|_hrsh-[\da-f]+$`),
		fileDate:        regexp.MustCompile(`(\d{4}-\d{2}-\d{2})`),
		isLesson:        regexp.MustCompile(`mlt_o_rav_.*(\d{4}-\d{2}-\d{2})_(lesson|lesson_full)`),
		isLessonMlt:     regexp.MustCompile(`mlt_o_rav_(\d{4}-\d{2}-\d{2})_lesson_full|mlt_o_rav_.*_(\d{4}-\d{2}-\d{2})_lesson|mlt_o_rav_(\d{4}-\d{2}-\d{2})_lesson`),
		isHershim:       regexp.MustCompile(`_hrsh$|_hrsh-[\da-f]+$|-[\da-f]+$`),
	}
}

// NewPlaylist creates a new optimized playlist processor
func NewPlaylist(start, finish time.Time, playlistsDir, hershimDir, reportName, taliName string) (*Playlist, error) {
	// Get hershim map with streaming processing
	hershimMap, err := getHershimMapOptimized(hershimDir)
	if err != nil {
		return nil, err
	}

	// Open output files
	reportFile, err := os.Create(reportName)
	if err != nil {
		return nil, err
	}

	taliFile, err := os.Create(taliName)
	if err != nil {
		reportFile.Close()
		return nil, err
	}

	// Open database with optimization
	db, err := sql.Open("sqlite", "report.db?cache=shared&mode=rwc")
	if err != nil {
		reportFile.Close()
		taliFile.Close()
		return nil, err
	}

	// Optimize database settings
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = 10000;
		PRAGMA temp_store = memory;
	`)
	if err != nil {
		reportFile.Close()
		taliFile.Close()
		db.Close()
		return nil, err
	}

	// Create table
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS ids(file TEXT PRIMARY KEY, id INTEGER)")
	if err != nil {
		reportFile.Close()
		taliFile.Close()
		db.Close()
		return nil, err
	}

	var maxID sql.NullInt64
	err = db.QueryRow("SELECT MAX(id) FROM ids").Scan(&maxID)
	if err != nil {
		reportFile.Close()
		taliFile.Close()
		db.Close()
		return nil, err
	}

	currentFileID := 0
	if maxID.Valid {
		currentFileID = int(maxID.Int64)
	}

	// Prepare statements
	selectStmt, err := db.Prepare("SELECT id FROM ids WHERE file = ?")
	if err != nil {
		reportFile.Close()
		taliFile.Close()
		db.Close()
		return nil, err
	}

	insertStmt, err := db.Prepare("INSERT INTO ids(file, id) VALUES(?, ?)")
	if err != nil {
		reportFile.Close()
		taliFile.Close()
		db.Close()
		selectStmt.Close()
		return nil, err
	}

	playlist := &Playlist{
		start:         start,
		finish:        finish,
		playlistsDir:  playlistsDir,
		hershimMap:    hershimMap,
		db:            db,
		currentFileID: currentFileID,
		selectStmt:    selectStmt,
		insertStmt:    insertStmt,
		reportWriter:  reportFile,
		taliWriter:    taliFile,
		regexes:       initRegexes(),
	}

	// Initialize pools
	playlist.eventPool = sync.Pool{
		New: func() interface{} {
			return &OptimizedEvent{}
		},
	}

	playlist.bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4096)
		},
	}

	return playlist, nil
}

// Close closes all resources
func (p *Playlist) Close() {
	if p.selectStmt != nil {
		p.selectStmt.Close()
	}
	if p.insertStmt != nil {
		p.insertStmt.Close()
	}
	if p.db != nil {
		p.db.Close()
	}
}

// Parse processes playlists with all optimizations
func (p *Playlist) Parse() error {
	if !superVerbose {
		printHeaderReport(p.reportWriter)
		printHeaderTali(p.taliWriter)
	}

	// Get playlist files
	playlists, err := getPlaylistFilesOptimized(p.playlistsDir, p.start, p.finish)
	if err != nil {
		return err
	}

	// Process files in parallel
	numWorkers := runtime.NumCPU()
	jobs := make(chan playlistJob, len(playlists))
	results := make(chan playlistResult, len(playlists))

	// Start workers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go p.worker(ctx, jobs, results, &wg)
	}

	// Send jobs
	go func() {
		defer close(jobs)
		for _, playlist := range playlists {
			select {
			case jobs <- playlistJob{path: playlist.path, date: playlist.date}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Close workers
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and process results
	var combinedEvents []eventWithTime

	for result := range results {
		if result.err != nil {
			cancel()
			return result.err
		}
		// Combine events with their times
		for i, event := range result.events {
			if i < len(result.times) {
				combinedEvents = append(combinedEvents, eventWithTime{
					event: event,
					time:  result.times[i],
				})
			}
		}
	}

	// Sort events by time
	sort.Slice(combinedEvents, func(i, j int) bool {
		return combinedEvents[i].time.Before(combinedEvents[j].time)
	})

	// Extract sorted events for database processing
	var allEvents []OptimizedEvent
	for _, combined := range combinedEvents {
		allEvents = append(allEvents, combined.event)
	}

	// Batch process database operations
	if err = p.batchProcessEvents(allEvents); err != nil {
		return err
	}

	// Output results in chronological order
	for i, combined := range combinedEvents {
		p.printOptimizedEvent(&allEvents[i], combined.time)
	}

	if !superVerbose {
		printFooter(p.reportWriter)
		printFooter(p.taliWriter)
	}

	return nil
}

// worker processes playlist files in parallel
func (p *Playlist) worker(ctx context.Context, jobs <-chan playlistJob, results chan<- playlistResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				return
			}
			events, times, err := p.parseFileOptimized(job.path, job.date)
			select {
			case results <- playlistResult{events: events, times: times, err: err}:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// parseFileOptimized processes a single playlist file with streaming
func (p *Playlist) parseFileOptimized(playlistPath string, date time.Time) ([]OptimizedEvent, []time.Time, error) {
	if verbose {
		log.Printf("Processing playlist: %s", playlistPath)
	}

	file, err := os.Open(playlistPath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	// Use buffered reader with pooled buffer
	buffer := p.bufferPool.Get().([]byte)
	defer p.bufferPool.Put(buffer)

	decoder := charmap.Windows1255.NewDecoder()
	reader := transform.NewReader(file, decoder)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(buffer, 8192)

	var events []OptimizedEvent
	var times []time.Time
	var playlisttc time.Time

	// Reuse event object
	event := p.eventPool.Get().(*OptimizedEvent)
	defer p.eventPool.Put(event)

	// Reset event
	*event = OptimizedEvent{}

	lineNo := 0
	performer := time.Time{}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if superVerbose {
			lineNo++
			log.Printf("Line #%d: %s", lineNo, line)
		}

		switch {
		case strings.HasPrefix(line, "#PLAYLISTTC"):
			if err := p.parsePlaylistTC(line, date, &playlisttc); err != nil {
				if verbose {
					log.Printf("Error parsing PLAYLISTTC: %v", err)
				}
			}

		case strings.HasPrefix(line, "#LISTID "):
			p.parseListID(line, event)

		case strings.HasPrefix(line, "#METADATA Name"):
			p.parseMetadataName(line, event)

		case strings.HasPrefix(line, "#TAPEID"):
			p.parseTapeID(line, event)

		case strings.HasPrefix(line, "#METADATA Title"):
			p.parseMetadataTitle(line, event)

		case strings.HasPrefix(line, "#PERFORMER"):
			if err := p.parsePerformer(line, playlisttc, &performer); err != nil {
				if verbose {
					log.Printf("Error parsing PERFORMER: %v", err)
				}
			}

		case strings.HasPrefix(line, "#EVENT WAITTO"):
			if newEvents, newTimes, newTC := p.parseEventWaitto(line, playlisttc, event); newTC != playlisttc {
				events = append(events, newEvents...)
				times = append(times, newTimes...)
				playlisttc = newTC
			}

		case strings.HasPrefix(line, "#EVENT WAIT") || strings.HasPrefix(line, "#LIVE_STREAM LIVE"):
			if newEvents, newTimes, newTC := p.parseEventWait(line, playlisttc, event); newTC != playlisttc {
				events = append(events, newEvents...)
				times = append(times, newTimes...)
				playlisttc = newTC
			}

		case strings.HasPrefix(line, `"`):
			if newEvents, newTimes, newTC := p.parseMediaFile(line, playlisttc, event); newTC != playlisttc {
				events = append(events, newEvents...)
				times = append(times, newTimes...)
				playlisttc = newTC
			}
		}
	}

	return events, times, scanner.Err()
}

// Optimized parsing methods
func (p *Playlist) parsePlaylistTC(line string, date time.Time, playlisttc *time.Time) error {
	parts := strings.Fields(line)
	if len(parts) > 0 {
		timeStr := parts[len(parts)-1]
		if strings.Count(timeStr, ":") == 3 {
			timeStr = timeStr[:8]
		}
		var err error
		*playlisttc, err = time.Parse("2006-01-02 15:04:05", fmt.Sprintf("%s %s", date.Format("2006-01-02"), timeStr))
		return err
	}
	return nil
}

func (p *Playlist) parseListID(line string, event *OptimizedEvent) {
	if matches := p.regexes.listid.FindStringSubmatch(line); len(matches) > 1 {
		event.listid = matches[1]
		event.name = ""
		event.title = ""
		event.tapeid = ""
	}
}

func (p *Playlist) parseMetadataName(line string, event *OptimizedEvent) {
	if matches := p.regexes.metadataName.FindStringSubmatch(line); len(matches) > 1 {
		event.name = matches[1]
	}
}

func (p *Playlist) parseTapeID(line string, event *OptimizedEvent) {
	if matches := p.regexes.tapeid.FindStringSubmatch(line); len(matches) > 1 {
		event.tapeid = matches[1]
	}
}

func (p *Playlist) parseMetadataTitle(line string, event *OptimizedEvent) {
	if matches := p.regexes.metadataTitle.FindStringSubmatch(line); len(matches) > 1 {
		event.title = matches[1]
	}
}

func (p *Playlist) parsePerformer(line string, playlisttc time.Time, performer *time.Time) error {
	if matches := p.regexes.performer.FindStringSubmatch(line); len(matches) > 1 {
		timeStr := fmt.Sprintf("%s %s", playlisttc.Format("2006-01-02"), matches[1])
		var err error
		*performer, err = time.Parse("2006-01-02 15:04:05", timeStr)
		if err != nil {
			*performer = time.Time{}
		}
		return err
	}
	return nil
}

func (p *Playlist) parseEventWaitto(line string, playlisttc time.Time, event *OptimizedEvent) ([]OptimizedEvent, []time.Time, time.Time) {
	matches := p.regexes.eventWaitto.FindStringSubmatch(line)
	if len(matches) > 1 {
		duration, err := strconv.ParseFloat(matches[1], 64)
		if err != nil {
			return nil, nil, playlisttc
		}
		endTime := time.Date(playlisttc.Year(), playlisttc.Month(), playlisttc.Day(), 0, 0, 0, 0, playlisttc.Location()).Add(time.Duration(duration) * time.Second)

		if !inDateRange(endTime, p.start, p.finish) {
			return nil, nil, endTime
		}

		newEvent := *event
		if inTimeRange(playlisttc, morningStart, morningEnd) {
			newEvent.name = "שיעור בוקר"
		} else {
			newEvent.name = "שידור חי"
		}
		newEvent.title = "שידור חי"
		newEvent.live = true
		newEvent.hershim = false
		newEvent.simanim = false
		newEvent.longFilename = ""
		newEvent.length = endTime.Sub(playlisttc).Seconds()

		return []OptimizedEvent{newEvent}, []time.Time{playlisttc}, endTime
	}
	return nil, nil, playlisttc
}

func (p *Playlist) parseEventWait(line string, playlisttc time.Time, event *OptimizedEvent) ([]OptimizedEvent, []time.Time, time.Time) {
	var duration float64
	var err error

	if strings.HasPrefix(line, "#EVENT WAIT") {
		if matches := p.regexes.eventWait.FindStringSubmatch(line); len(matches) > 1 {
			duration, err = strconv.ParseFloat(matches[1], 64)
		}
	} else {
		if matches := p.regexes.liveStreamLive.FindStringSubmatch(line); len(matches) > 1 {
			duration, err = strconv.ParseFloat(matches[1], 64)
		}
	}

	if err != nil {
		return nil, nil, playlisttc
	}

	endTime := playlisttc.Add(time.Duration(duration) * time.Second)

	newEvent := *event
	if inTimeRange(playlisttc, morningStart, morningEnd) {
		newEvent.name = "שיעור בוקר"
	} else {
		newEvent.name = "שידור חי"
	}
	newEvent.title = "שידור חי"
	newEvent.live = true
	newEvent.hershim = false
	newEvent.simanim = false
	newEvent.longFilename = ""
	newEvent.length = endTime.Sub(playlisttc).Seconds()

	return []OptimizedEvent{newEvent}, []time.Time{playlisttc}, endTime
}

func (p *Playlist) parseMediaFile(line string, playlisttc time.Time, event *OptimizedEvent) ([]OptimizedEvent, []time.Time, time.Time) {
	parts := strings.Split(line, ";")
	if len(parts) < 5 {
		return nil, nil, playlisttc
	}

	// Extract filename
	filePart := strings.Trim(parts[0], `"`)
	longFilename := filepath.Base(strings.ReplaceAll(filePart, `\`, "/"))
	longFilename = strings.TrimSuffix(longFilename, ".mpg")

	// Extract timing
	playTimeTrimmed, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return nil, nil, playlisttc
	}
	playTimeTotal, err := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
	if err != nil {
		return nil, nil, playlisttc
	}
	playTime := playTimeTotal - playTimeTrimmed

	short := strings.TrimSpace(parts[4])

	// Skip if out of date range
	if !inDateRange(playlisttc, p.start, p.finish) {
		if verbose {
			log.Printf("%s SKIP %s [time] %s", event.listid, playlisttc.Format("2006-01-02 15:04:05"), short)
		}
		return nil, nil, playlisttc.Add(time.Duration(playTime) * time.Second)
	}

	newEvent := *event

	// Check for simanim
	newEvent.simanim = p.regexes.hershim.MatchString(longFilename)
	if verbose && newEvent.simanim {
		log.Printf("%s SIMANIM %s %s %s", newEvent.listid, playlisttc.Format("2006-01-02 15:04:05"), short, longFilename)
	}

	// Find hershim
	p.findHershimOptimized(longFilename, &newEvent)
	if verbose && newEvent.hershim {
		log.Printf("%s HERSHIM %s %s %s", newEvent.listid, playlisttc.Format("2006-01-02 15:04:05"), short, longFilename)
	}

	// Extract file date
	var fileDate time.Time
	if matches := p.regexes.fileDate.FindStringSubmatch(longFilename); len(matches) > 1 {
		fileDate, _ = time.Parse("2006-01-02", matches[1])
	}

	// Check if live
	dayBefore := playlisttc.AddDate(0, 0, -1)
	dayAfter := playlisttc.AddDate(0, 0, 1)
	newEvent.live = inDateRange(fileDate, dayBefore, dayAfter)

	if newEvent.name == "" {
		newEvent.name = short
	}
	newEvent.length = playTime
	newEvent.longFilename = longFilename

	return []OptimizedEvent{newEvent}, []time.Time{playlisttc}, playlisttc.Add(time.Duration(playTime) * time.Second)
}

// findHershimOptimized is an optimized version of hershim detection
func (p *Playlist) findHershimOptimized(filename string, event *OptimizedEvent) {
	var hershim bool

	if event.tapeid != "" {
		// Use prefix matching for better performance
		for name := range p.hershimMap {
			if strings.Contains(name, event.tapeid) {
				hershim = true
				break
			}
		}
		event.morningLessonHershim = p.regexes.isLesson.MatchString(filename)
	}

	if !hershim {
		matches := p.regexes.isLessonMlt.FindStringSubmatch(filename)
		if len(matches) > 0 {
			var date string
			for i := 1; i < len(matches); i++ {
				if matches[i] != "" {
					date = matches[i]
					break
				}
			}
			if date != "" {
				for name := range p.hershimMap {
					if strings.Contains(name, "shiur-boker-helek") && strings.Contains(name, date) {
						hershim = true
						break
					}
				}
				event.morningLessonHershim = hershim
			}
		} else {
			hershimName := p.regexes.isHershim.ReplaceAllString(filename, "")
			for name := range p.hershimMap {
				if strings.HasSuffix(name, hershimName) {
					hershim = true
					break
				}
			}
			event.morningLessonHershim = false
		}
	}

	event.hershim = hershim
}

// batchProcessEvents processes database operations in batches
func (p *Playlist) batchProcessEvents(events []OptimizedEvent) error {
	const batchSize = 100

	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	selectStmt := tx.Stmt(p.selectStmt)
	insertStmt := tx.Stmt(p.insertStmt)

	for i, event := range events {
		if event.longFilename == "" {
			continue
		}

		var hash int
		err = selectStmt.QueryRow(event.longFilename).Scan(&hash)
		if err == sql.ErrNoRows {
			p.currentFileID++
			_, err = insertStmt.Exec(event.longFilename, p.currentFileID)
			if err != nil {
				return err
			}
			hash = p.currentFileID
		} else if err != nil {
			return err
		}

		events[i].hash = formatWithPrefix(hash, "KAB", 5)

		// Commit in batches
		if (i+1)%batchSize == 0 {
			if err := tx.Commit(); err != nil {
				return err
			}
			tx, err = p.db.Begin()
			if err != nil {
				return err
			}
			selectStmt = tx.Stmt(p.selectStmt)
			insertStmt = tx.Stmt(p.insertStmt)
		}
	}

	return tx.Commit()
}

// printOptimizedEvent prints an event using the original Event methods
func (p *Playlist) printOptimizedEvent(event *OptimizedEvent, t time.Time) {
	// Convert OptimizedEvent to Event for compatibility
	e := &Event{
		listid:               event.listid,
		name:                 event.name,
		title:                event.title,
		length:               event.length,
		live:                 event.live,
		hershim:              event.hershim,
		simanim:              event.simanim,
		longFilename:         event.longFilename,
		saturday:             event.saturday,
		illegal:              event.illegal,
		tooShort:             event.tooShort,
		morningLessonHershim: event.morningLessonHershim,
		tapeid:               event.tapeid,
		hash:                 event.hash,
		report:               p.reportWriter,
		tali:                 p.taliWriter,
	}

	// Print tali output once per original event (no splitting)
	e.printTaliSimple(t)

	// Print report output with splitting logic
	e.printRowReport(t)
}

// isWindowsAbsolutePath checks if path is Windows absolute path
func isWindowsAbsolutePath(path string) bool {
	// Check for drive letter format: C:\ or C:/ or just C:
	if len(path) >= 2 && path[1] == ':' &&
		((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) {
		return true
	}
	// Check for UNC path: \\server\share
	if len(path) >= 2 && path[0] == '\\' && path[1] == '\\' {
		return true
	}
	return false
}

// getHershimMapOptimized creates hershim map with streaming processing
func getHershimMapOptimized(hershimDir string) (map[string]bool, error) {
	hershimMap := make(map[string]bool)

	// Handle Windows paths properly
	targetPath := hershimDir
	if runtime.GOOS == "windows" && isWindowsAbsolutePath(hershimDir) {
		// Use the path as-is for Windows absolute paths
		targetPath = filepath.Clean(hershimDir)
	} else if !filepath.IsAbs(hershimDir) {
		var err error
		targetPath, err = filepath.Abs(hershimDir)
		if err != nil {
			return nil, err
		}
	}

	// Check if directory exists
	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		return nil, err
	}

	err := filepath.Walk(targetPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Continue processing even if some files are inaccessible
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(path, ".sub") {
			basename := strings.TrimSuffix(filepath.Base(path), ".sub")
			hershimMap[basename] = true
		}
		return nil
	})

	return hershimMap, err
}

// getPlaylistFilesOptimized gets playlist files with better filtering
func getPlaylistFilesOptimized(playlistsDir string, start, finish time.Time) ([]playlistEntry, error) {
	var playlists []playlistEntry
	startFile := start.AddDate(0, 0, -1)

	// Handle Windows paths properly
	targetPath := playlistsDir
	if runtime.GOOS == "windows" && isWindowsAbsolutePath(playlistsDir) {
		// Use the path as-is for Windows absolute paths
		targetPath = filepath.Clean(playlistsDir)
	} else if !filepath.IsAbs(playlistsDir) {
		var err error
		targetPath, err = filepath.Abs(playlistsDir)
		if err != nil {
			return nil, err
		}
	}

	// Check if directory exists
	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		return nil, err
	}

	err := filepath.Walk(targetPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Continue processing
		}

		if info.IsDir() || !strings.HasSuffix(path, ".ply") {
			return nil
		}

		nameParts := strings.TrimSuffix(filepath.Base(path), ".ply")
		nameParts = strings.ReplaceAll(nameParts, "_", "-")

		var nameDate time.Time
		var parseErr error

		// Try full timestamp format first
		if len(nameParts) >= 19 {
			nameDate, parseErr = time.Parse("2006-01-02-15-04-05", nameParts)
		}

		// Try just date format
		if parseErr != nil && len(nameParts) >= 10 {
			datePart := nameParts[:10]
			nameDate, parseErr = time.Parse("2006-01-02", datePart)
		}

		if parseErr == nil {
			if (nameDate.Equal(startFile) || nameDate.After(startFile)) &&
				(nameDate.Equal(finish) || nameDate.Before(finish)) {
				playlists = append(playlists, playlistEntry{path: path, date: nameDate})
			}
		}

		return nil
	})

	return playlists, err
}

// printRowReport handles report output with splitting logic (separated from tali output)
func (e *Event) printRowReport(t time.Time) {
	e.tooShort = e.length < 10 && !regexp.MustCompile(`_program_|_lesson_`).MatchString(e.longFilename)
	e.saturday = t.Weekday() == time.Saturday

	lengthSec := t.Add(time.Duration(e.length) * time.Second)

	if superVerbose {
		fmt.Fprintf(os.Stderr, "%s %s %s %f\n", t.Format("2006-01-02 15:04:05"), e.name, e.title, e.length)
	}

	// Report output with prime time splitting logic
	if inTimeRange(t, primeTimeStart, primeTimeEnd) {
		if inTimeRange(lengthSec, primeTimeStart, primeTimeEnd) {
			if superVerbose {
				fmt.Fprintf(os.Stderr, "starts inside, ends inside PRIME_TIME\n")
			}
			e.printOneLineReport(t, e.length, true, false, e.length)
		} else {
			if superVerbose {
				fmt.Fprintf(os.Stderr, "starts inside, ends after PRIME_TIME\n")
			}
			finish := parseTime(t, "23:00:00")
			insideLength := finish.Sub(t).Seconds()
			e.printOneLineReport(t, insideLength, true, false, e.length)
			e.printOneLineReport(finish, e.length-insideLength, false, false, e.length)
		}
	} else {
		// Check if event crosses prime time boundary (starts before, ends at or after 19:00:00)
		primeStart := parseTime(t, primeTimeStart)
		// Allow for small timing differences - if event ends within 10 seconds of boundary, treat as crossing
		boundaryTolerance := 10 * time.Second
		if lengthSec.After(primeStart) || lengthSec.Equal(primeStart) || primeStart.Sub(lengthSec) <= boundaryTolerance {
			if superVerbose {
				fmt.Fprintf(os.Stderr, "starts before, ends at or after PRIME_TIME start\n")
			}
			start := parseTime(t, "19:00:00")
			beforeLength := start.Sub(t).Seconds()
			afterLength := e.length - beforeLength

			// Ensure we don't have negative durations
			if beforeLength < 0 {
				beforeLength = 0
			}
			if afterLength < 0 {
				afterLength = 0
			}

			e.printOneLineReport(t, beforeLength, false, false, e.length)
			e.printOneLineReport(start, afterLength, true, false, e.length)
		} else {
			if inTimeRange(t, illegalStart, illegalEnd) {
				if inTimeRange(lengthSec, illegalStart, illegalEnd) {
					if superVerbose {
						fmt.Fprintf(os.Stderr, "starts inside, ends inside ILLEGAL_TIME\n")
					}
					e.printOneLineReport(t, e.length, false, true, e.length)
				} else {
					if superVerbose {
						fmt.Fprintf(os.Stderr, "starts inside, ends after ILLEGAL_TIME\n")
					}
					finish := parseTime(t, "06:00:00")
					insideLength := finish.Sub(t).Seconds()
					e.printOneLineReport(t, insideLength, false, true, e.length)
					e.printOneLineReport(finish, e.length-insideLength, false, false, e.length)
				}
			} else {
				if inTimeRange(lengthSec, illegalStart, illegalEnd) {
					if superVerbose {
						fmt.Fprintf(os.Stderr, "starts before, ends inside ILLEGAL_TIME\n")
					}
					var start time.Time
					if !sameDay(t, lengthSec) {
						if superVerbose {
							fmt.Fprintf(os.Stderr, " and crosses midnight!!!\n")
						}
						start = parseTime(t.AddDate(0, 0, 1), "01:00:00")
					} else {
						start = parseTime(t, "01:00:00")
					}
					beforeLength := start.Sub(t).Seconds()
					e.printOneLineReport(t, beforeLength, false, false, e.length)
					e.printOneLineReport(start, e.length-beforeLength, false, true, e.length)
				} else {
					if superVerbose {
						fmt.Fprintf(os.Stderr, "starts outside, ends outside PRIME_TIME & ILLEGAL_TIME\n")
					}
					e.printOneLineReport(t, e.length, false, false, e.length)
				}
			}
		}
	}
}

func (e *Event) printTaliSimple(t time.Time) {
	// Set event properties needed for consistent data
	e.tooShort = e.length < 10 && !regexp.MustCompile(`_program_|_lesson_`).MatchString(e.longFilename)
	e.saturday = t.Weekday() == time.Saturday

	nameVal := e.name
	titleVal := e.title
	if nameVal == "" || nameVal == "Not defined" {
		nameVal = e.title
	}
	if titleVal == "" || titleVal == "Not defined" {
		titleVal = e.name
	}

	fmt.Fprintf(e.tali, `      <Row>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell ss:StyleID="s66"><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
      </Row>
`, t.Format("2006-01-02"), t.Format("15:04:05"), nameVal, titleVal,
		formatDuration(e.length), e.longFilename, e.hash)
}

func (e *Event) printOneLineReport(t time.Time, partLength float64, prime, illegal bool, length float64) {
	var id string
	switch {
	case illegal:
		id = "s65" // grey
	case regexp.MustCompile(`(logo|promo|patiach|sagir|luz)`).MatchString(e.longFilename):
		id = "s67" // green
	case e.tooShort:
		id = "s64" // red
	case e.saturday:
		id = "s62" // yellow
	case prime:
		id = "s63" // blue
	default:
		id = "s61" // none
	}

	nameVal := e.name
	titleVal := e.title
	if nameVal == "" || nameVal == "Not defined" {
		nameVal = e.title
	}
	if titleVal == "" || titleVal == "Not defined" {
		titleVal = e.name
	}

	primeText := "לא"
	if prime {
		primeText = "כן"
	}
	liveText := "לא"
	if e.live {
		liveText = "כן"
	}
	hershimText := "לא"
	if e.hershim {
		hershimText = "כן"
	}
	morningText := "לא"
	if e.morningLessonHershim {
		morningText = "כן"
	}
	simanimText := "לא"
	if e.simanim {
		simanimText = "כן"
	}

	fmt.Fprintf(e.report, `      <Row>
        <Cell ss:StyleID="%s"><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell ss:StyleID="s66"><Data ss:Type="Number">%02.0f.00</Data></Cell>
        <Cell ss:StyleID="s66"><Data ss:Type="Number">%02.0f.00</Data></Cell>
        <Cell ss:StyleID="s66"><Data ss:Type="String">%s</Data></Cell>
        <Cell ss:StyleID="s66"><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
        <Cell><Data ss:Type="String">%s</Data></Cell>
      </Row>
`, id, e.listid, t.Format("2006-01-02"), t.Format("15:04:05"), nameVal, titleVal,
		partLength/60, length/60, formatDuration(partLength), formatDuration(length),
		primeText, liveText, hershimText, morningText, simanimText, e.longFilename, e.hash)
}

// Header and footer functions
func printHeaderReport(w io.Writer) {
	fmt.Fprint(w, `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
  xmlns:o="urn:schemas-microsoft-com:office:office"
  xmlns:x="urn:schemas-microsoft-com:office:excel"
  xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
  xmlns:html="http://www.w3.org/TR/REC-html40">
  <Styles>
    <Style ss:ID="Default" ss:Name="Normal">
     <Alignment ss:Vertical="Bottom"/>
     <Borders/>
     <Font ss:FontName="Calibri" x:Family="Swiss" ss:Size="11" ss:Color="#000000"/>
     <Interior/>
     <NumberFormat/>
     <Protection/>
    </Style>
    <Style ss:ID="s60">
     <Font ss:FontName="Calibri" x:Family="Swiss" ss:Size="11" ss:Color="#000000" ss:Bold="1"/>
    </Style>
    <Style ss:ID="s61">
    </Style>
    <Style ss:ID="s62">
     <Interior ss:Color="#FFFF00" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s63">
     <Interior ss:Color="#0000FF" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s64">
     <Interior ss:Color="#FF0000" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s65">
     <Interior ss:Color="#666666" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s66">
     <NumberFormat ss:Format="Fixed"/>
    </Style>
    <Style ss:ID="s67">
     <Interior ss:Color="#00FF00" ss:Pattern="Solid"/>
    </Style>
  </Styles>
  <Worksheet ss:Name="Sheet1">
    <Table>
      <Row ss:StyleID="s60">
        <Cell><Data ss:Type="String">ID</Data></Cell>
        <Cell><Data ss:Type="String">תאריך שידור</Data></Cell>
        <Cell><Data ss:Type="String">שעת שידור</Data></Cell>
        <Cell><Data ss:Type="String">שם סדרה</Data></Cell>
        <Cell><Data ss:Type="String">שם פרק</Data></Cell>
        <Cell><Data ss:Type="String">אורך פרק (דק')</Data></Cell>
        <Cell><Data ss:Type="String">אורך מלא (דק')</Data></Cell>
        <Cell><Data ss:Type="String">אורך פרק</Data></Cell>
        <Cell><Data ss:Type="String">אורך מלא</Data></Cell>
        <Cell><Data ss:Type="String">פריים טיים</Data></Cell>
        <Cell><Data ss:Type="String">האם שידור חי או לא</Data></Cell>
        <Cell><Data ss:Type="String">האם היו כתוביות - כן/לא</Data></Cell>
        <Cell><Data ss:Type="String">כתוביות שיעור בוקר - כן/לא</Data></Cell>
        <Cell><Data ss:Type="String">האם תכנית עם שפת הסימנים - כן/לא</Data></Cell>
        <Cell><Data ss:Type="String">שם קובץ</Data></Cell>
        <Cell><Data ss:Type="String">UNIQUE KAB ID</Data></Cell>
      </Row>
`)
}

func printHeaderTali(w io.Writer) {
	fmt.Fprint(w, `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
  xmlns:o="urn:schemas-microsoft-com:office:office"
  xmlns:x="urn:schemas-microsoft-com:office:excel"
  xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
  xmlns:html="http://www.w3.org/TR/REC-html40">
  <Styles>
    <Style ss:ID="Default" ss:Name="Normal">
     <Alignment ss:Vertical="Bottom"/>
     <Borders/>
     <Font ss:FontName="Calibri" x:Family="Swiss" ss:Size="11" ss:Color="#000000"/>
     <Interior/>
     <NumberFormat/>
     <Protection/>
    </Style>
    <Style ss:ID="s60">
     <Font ss:FontName="Calibri" x:Family="Swiss" ss:Size="11" ss:Color="#000000" ss:Bold="1"/>
    </Style>
    <Style ss:ID="s61">
    </Style>
    <Style ss:ID="s62">
     <Interior ss:Color="#FFFF00" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s63">
     <Interior ss:Color="#0000FF" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s64">
     <Interior ss:Color="#FF0000" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s65">
     <Interior ss:Color="#666666" ss:Pattern="Solid"/>
    </Style>
    <Style ss:ID="s66">
     <NumberFormat ss:Format="Fixed"/>
    </Style>
    <Style ss:ID="s67">
     <Interior ss:Color="#00FF00" ss:Pattern="Solid"/>
    </Style>
  </Styles>
  <Worksheet ss:Name="Sheet1">
    <Table>
      <Row ss:StyleID="s60">
        <Cell><Data ss:Type="String">תאריך שידור</Data></Cell>
        <Cell><Data ss:Type="String">שעת שידור</Data></Cell>
        <Cell><Data ss:Type="String">שם סדרה</Data></Cell>
        <Cell><Data ss:Type="String">שם פרק</Data></Cell>
        <Cell><Data ss:Type="String">אורך פרק</Data></Cell>
        <Cell><Data ss:Type="String">שם קובץ</Data></Cell>
        <Cell><Data ss:Type="String">UNIQUE KAB ID</Data></Cell>
      </Row>
`)
}

func printFooter(w io.Writer) {
	fmt.Fprint(w, `    </Table>
  </Worksheet>
</Workbook>
`)
}

// Helper functions
func inTimeRange(t time.Time, startTime, endTime string) bool {
	timeStr := t.Format("15:04:05")
	// Handle exact boundary cases more precisely
	return timeStr >= startTime && timeStr <= endTime
}

func inDateRange(t, start, finish time.Time) bool {
	date := t.Truncate(24 * time.Hour)
	startDate := start.Truncate(24 * time.Hour)
	finishDate := finish.Truncate(24 * time.Hour)
	return (date.Equal(startDate) || date.After(startDate)) && (date.Equal(finishDate) || date.Before(finishDate))
}

func parseTime(base time.Time, timeStr string) time.Time {
	t, _ := time.Parse("15:04:05", timeStr)
	return time.Date(base.Year(), base.Month(), base.Day(), t.Hour(), t.Minute(), t.Second(), 0, base.Location())
}

func sameDay(t1, t2 time.Time) bool {
	return t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day()
}

func formatDuration(seconds float64) string {
	duration := time.Duration(seconds) * time.Second
	return fmt.Sprintf("%02d:%02d:%02d", int(duration.Hours()), int(duration.Minutes())%60, int(duration.Seconds())%60)
}

func formatWithPrefix(number int, prefix string, digits int) string {
	format := fmt.Sprintf("%%s%%0%dd", digits)
	return fmt.Sprintf(format, prefix, number)
}

func main() {
	var startStr = flag.String("s", "", "start date")
	var finishStr = flag.String("f", "", "finish date")
	var reportName = flag.String("r", "report.xls", "report file name")
	var taliName = flag.String("t", "tali.xls", "tali file name")
	var playlistsDir = flag.String("p", "playlists", "playlists directory")
	var hershimDir = flag.String("e", "hershim", "hershim directory")
	var verboseFlag = flag.Bool("v", false, "verbose output")
	var superVerboseFlag = flag.Bool("V", false, "super verbose output")

	flag.Parse()

	// Debug: print all flag values
	log.Printf("Flag values parsed:")
	log.Printf("  -s (start): %s", *startStr)
	log.Printf("  -f (finish): %s", *finishStr)
	log.Printf("  -r (report): %s", *reportName)
	log.Printf("  -t (tali): %s", *taliName)
	log.Printf("  -p (playlists): %s", *playlistsDir)
	log.Printf("  -e (hershim): %s", *hershimDir)
	log.Printf("  -v (verbose): %t", *verboseFlag)
	log.Printf("  -V (super verbose): %t", *superVerboseFlag)

	verbose = *verboseFlag
	superVerbose = *superVerboseFlag
	if superVerbose {
		verbose = true
	}

	now := time.Now()
	start := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.Local)
	finish := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)

	if *startStr != "" {
		var err error
		start, err = time.Parse("2006-01-02", *startStr)
		if err != nil {
			log.Fatalf("Invalid start date: %v", err)
		}
	}

	if *finishStr != "" {
		var err error
		finish, err = time.Parse("2006-01-02", *finishStr)
		if err != nil {
			log.Fatalf("Invalid finish date: %v", err)
		}
		// Adjust finish to end of day (23:59:59)
		finish = time.Date(finish.Year(), finish.Month(), finish.Day(), 23, 59, 59, 0, finish.Location())
	}

	playlist, err := NewPlaylist(start, finish, *playlistsDir, *hershimDir, *reportName, *taliName)
	if err != nil {
		log.Fatalf("Error creating playlist: %v", err)
	}
	defer playlist.Close()

	if err := playlist.Parse(); err != nil {
		log.Fatalf("Error parsing playlist: %v", err)
	}
}
