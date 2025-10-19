package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/text/unicode/norm"
)

type Config struct {
	InputDir   string
	OutputDir  string
	Whitelist  []string
	Blacklist  []string
	SkipPaths  []string
	NumWorkers int
}

type FileJob struct {
	Path string
	Info os.FileInfo
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "extensions" {
		// Subcommand: list extensions
		extCmd := flag.NewFlagSet("extensions", flag.ExitOnError)
		inputDir := extCmd.String("input", "", "Input directory path (required)")
		workers := extCmd.Int("workers", 8, "Number of concurrent workers")
		skipPaths := extCmd.String("skip", "", "Comma-separated relative paths to skip (e.g., .git,node_modules,temp)")
		extCmd.Parse(os.Args[2:])

		if *inputDir == "" {
			fmt.Fprintln(os.Stderr, "Error: -input directory is required")
			extCmd.Usage()
			os.Exit(1)
		}

		skipList := parseSkipPaths(*skipPaths)
		if err := listExtensions(*inputDir, *workers, skipList); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Default command: deduplicate files
	inputDir := flag.String("input", "", "Input directory path (required)")
	outputDir := flag.String("output", "", "Output directory path (required)")
	whitelist := flag.String("whitelist", "", "Comma-separated file extensions to include (e.g., jpg,png,pdf)")
	blacklist := flag.String("blacklist", "", "Comma-separated file extensions to exclude (e.g., tmp,log)")
	skipPaths := flag.String("skip", "", "Comma-separated relative paths to skip (e.g., .git,node_modules,temp)")
	workers := flag.Int("workers", 8, "Number of concurrent workers")
	flag.Parse()

	// Validate required arguments
	if *inputDir == "" || *outputDir == "" {
		fmt.Fprintln(os.Stderr, "Error: Both -input and -output directories are required")
		flag.Usage()
		os.Exit(1)
	}

	// Check if both whitelist and blacklist are specified
	if *whitelist != "" && *blacklist != "" {
		fmt.Fprintln(os.Stderr, "Error: Cannot specify both -whitelist and -blacklist")
		os.Exit(1)
	}

	config := Config{
		InputDir:   *inputDir,
		OutputDir:  *outputDir,
		Whitelist:  parseExtensions(*whitelist),
		Blacklist:  parseExtensions(*blacklist),
		SkipPaths:  parseSkipPaths(*skipPaths),
		NumWorkers: *workers,
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	if err := deduplicateFiles(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseExtensions(input string) []string {
	if input == "" {
		return nil
	}
	exts := strings.Split(input, ",")
	for i, ext := range exts {
		exts[i] = strings.TrimSpace(strings.TrimPrefix(ext, "."))
	}
	return exts
}

func parseSkipPaths(input string) []string {
	if input == "" {
		return nil
	}
	paths := strings.Split(input, ",")
	for i, path := range paths {
		paths[i] = strings.TrimSpace(path)
	}
	return paths
}

func shouldSkipPath(path, inputDir string, skipPaths []string) bool {
	if len(skipPaths) == 0 {
		return false
	}

	// Get relative path from input directory
	relPath, err := filepath.Rel(inputDir, path)
	if err != nil {
		return false
	}

	// Check if this path or any parent path should be skipped
	for _, skip := range skipPaths {
		// Check if the relative path starts with the skip path
		if relPath == skip || strings.HasPrefix(relPath, skip+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func deduplicateFiles(config Config) error {
	// Map to track seen file hashes (only for files we process from input)
	seenHashes := &sync.Map{}
	var totalFiles, uniqueFiles, skippedFiles, alreadyExisted int64
	var processedFiles int64

	// Create job channel and worker pool
	jobs := make(chan FileJob, 1000)
	var wg sync.WaitGroup

	// Progress ticker for main processing
	stopMainProgress := make(chan bool)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				processed := atomic.LoadInt64(&processedFiles)
				copied := atomic.LoadInt64(&uniqueFiles)
				total := atomic.LoadInt64(&totalFiles)
				fmt.Fprintf(os.Stderr, "\rProcessing: %d/%d files | Copied: %d unique", processed, total, copied)
			case <-stopMainProgress:
				return
			}
		}
	}()

	// Start workers
	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if !shouldProcess(job.Path, config) {
					atomic.AddInt64(&skippedFiles, 1)
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				// Calculate destination path
				relPath, err := filepath.Rel(config.InputDir, job.Path)
				if err != nil {
					fmt.Fprintf(os.Stderr, "\nError calculating relative path for %s: %v\n", job.Path, err)
					atomic.AddInt64(&processedFiles, 1)
					continue
				}
				dstPath := filepath.Join(config.OutputDir, relPath)

				// Create parent directories
				dstDir := filepath.Dir(dstPath)
				if err := os.MkdirAll(dstDir, 0755); err != nil {
					fmt.Fprintf(os.Stderr, "\nError creating directory %s: %v\n", dstDir, err)
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				var hash string

				// Check if destination file already exists
				if _, err := os.Stat(dstPath); err == nil {
					// File exists - hash both to check if identical
					srcHash, err := hashFile(job.Path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "\nError hashing source %s: %v\n", job.Path, err)
						atomic.AddInt64(&processedFiles, 1)
						continue
					}

					dstHash, err := hashFile(dstPath)
					if err != nil {
						fmt.Fprintf(os.Stderr, "\nError hashing destination %s: %v\n", dstPath, err)
						atomic.AddInt64(&processedFiles, 1)
						continue
					}

					if srcHash == dstHash {
						// Exact duplicate already exists at destination
						atomic.AddInt64(&alreadyExisted, 1)
						atomic.AddInt64(&processedFiles, 1)
						continue
					}

					// Different files at same path - skip (keep existing)
					atomic.AddInt64(&alreadyExisted, 1)
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				// Destination doesn't exist - copy while hashing in one pass
				hash, err = hashAndCopyFile(job.Path, dstPath, job.Info)
				if err != nil {
					fmt.Fprintf(os.Stderr, "\nError copying %s: %v\n", job.Path, err)
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				// Check if we've seen this hash before
				if _, exists := seenHashes.LoadOrStore(hash, job.Path); exists {
					// Duplicate content - delete the file we just copied
					os.Remove(dstPath)
					atomic.AddInt64(&alreadyExisted, 1)
				} else {
					// Unique file - keep it
					atomic.AddInt64(&uniqueFiles, 1)
				}

				atomic.AddInt64(&processedFiles, 1)
			}
		}()
	}

	// Walk the input directory and send jobs
	err := filepath.Walk(config.InputDir, func(path string, info os.FileInfo, err error) error {
		// If there's an error, try both normalizations
		if err != nil {
			// Try NFC normalization
			nfcPath := norm.NFC.String(path)
			if nfcInfo, nfcErr := os.Lstat(nfcPath); nfcErr == nil {
				path = nfcPath
				info = nfcInfo
				err = nil
			} else {
				// Try NFD normalization
				nfdPath := norm.NFD.String(path)
				if nfdInfo, nfdErr := os.Lstat(nfdPath); nfdErr == nil {
					path = nfdPath
					info = nfdInfo
					err = nil
				} else {
					// Still can't access it, skip this entry
					return nil
				}
			}
		}

		// Check if this path should be skipped
		if shouldSkipPath(path, config.InputDir, config.SkipPaths) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if !info.IsDir() {
			atomic.AddInt64(&totalFiles, 1)
			jobs <- FileJob{Path: path, Info: info}
		}
		return nil
	})

	close(jobs)
	wg.Wait()
	stopMainProgress <- true

	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "\rProcessing: %d/%d files | Copied: %d unique\n", processedFiles, totalFiles, uniqueFiles)
	fmt.Printf("\nProcessing complete:\n")
	fmt.Printf("  Total files scanned: %d\n", totalFiles)
	fmt.Printf("  Unique files copied: %d\n", uniqueFiles)
	fmt.Printf("  Already existed in output: %d\n", alreadyExisted)
	fmt.Printf("  Skipped (by filter): %d\n", skippedFiles)
	fmt.Printf("  Duplicates in source: %d\n", totalFiles-uniqueFiles-skippedFiles-alreadyExisted)

	return nil
}

func shouldProcess(path string, config Config) bool {
	ext := strings.TrimPrefix(filepath.Ext(path), ".")
	ext = strings.ToLower(ext)

	// If whitelist is specified, only include files with matching extensions
	if len(config.Whitelist) > 0 {
		for _, allowed := range config.Whitelist {
			if ext == strings.ToLower(allowed) {
				return true
			}
		}
		return false
	}

	// If blacklist is specified, exclude files with matching extensions
	if len(config.Blacklist) > 0 {
		for _, blocked := range config.Blacklist {
			if ext == strings.ToLower(blocked) {
				return false
			}
		}
	}

	return true
}

const bufferSize = 1024 * 1024 // 1MB buffer for I/O operations

func hashFile(path string) (string, error) {
	// Check if file still exists before opening
	if _, err := os.Lstat(path); err != nil {
		return "", err
	}

	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := xxhash.New()
	buf := make([]byte, bufferSize)
	if _, err := io.CopyBuffer(hash, file, buf); err != nil {
		return "", err
	}

	return strconv.FormatUint(hash.Sum64(), 16), nil
}

// hashAndCopyFile reads a file once, hashing while copying to destination
// Returns the hash and any error encountered
func hashAndCopyFile(srcPath, dstPath string, info os.FileInfo) (string, error) {
	// Check if source file still exists
	if _, err := os.Lstat(srcPath); err != nil {
		return "", err
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return "", err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return "", err
	}
	defer dst.Close()

	// Hash while copying in a single pass
	hash := xxhash.New()
	buf := make([]byte, bufferSize)

	// TeeReader writes to hash while we copy to dst
	teeReader := io.TeeReader(src, hash)
	if _, err := io.CopyBuffer(dst, teeReader, buf); err != nil {
		return "", err
	}

	// Preserve file permissions
	if err := os.Chmod(dstPath, info.Mode()); err != nil {
		return "", err
	}

	return strconv.FormatUint(hash.Sum64(), 16), nil
}

func listExtensions(inputDir string, numWorkers int, skipPaths []string) error {
	extMap := &sync.Map{}
	jobs := make(chan string, 1000)
	var wg sync.WaitGroup
	var filesProcessed int64

	// Progress ticker
	stopProgress := make(chan bool)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Fprintf(os.Stderr, "\rScanning extensions: %d files processed", atomic.LoadInt64(&filesProcessed))
			case <-stopProgress:
				return
			}
		}
	}()

	// Start workers to extract extensions
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				ext := strings.TrimPrefix(filepath.Ext(path), ".")
				if ext != "" {
					extMap.Store(strings.ToLower(ext), true)
				}
				atomic.AddInt64(&filesProcessed, 1)
			}
		}()
	}

	// Walk directory and queue file paths
	filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		// If there's an error, try both normalizations
		if err != nil {
			// Try NFC normalization
			nfcPath := norm.NFC.String(path)
			if nfcInfo, nfcErr := os.Lstat(nfcPath); nfcErr == nil {
				path = nfcPath
				info = nfcInfo
				err = nil
			} else {
				// Try NFD normalization
				nfdPath := norm.NFD.String(path)
				if nfdInfo, nfdErr := os.Lstat(nfdPath); nfdErr == nil {
					path = nfdPath
					info = nfdInfo
					err = nil
				} else {
					// Still can't access it, skip this entry
					return nil
				}
			}
		}

		// Check if this path should be skipped
		if shouldSkipPath(path, inputDir, skipPaths) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if !info.IsDir() {
			jobs <- path
		}
		return nil
	})

	close(jobs)
	wg.Wait()
	stopProgress <- true

	fmt.Fprintf(os.Stderr, "\rScanning extensions: %d files processed\n", filesProcessed)

	// Collect and sort extensions
	exts := make([]string, 0)
	extMap.Range(func(key, value interface{}) bool {
		exts = append(exts, key.(string))
		return true
	})
	sort.Strings(exts)

	// Print in whitelist format
	fmt.Println(strings.Join(exts, ","))

	return nil
}
