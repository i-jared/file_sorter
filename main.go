package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/text/unicode/norm"
)

type Config struct {
	InputDir   string
	OutputDir  string
	Whitelist  []string
	Blacklist  []string
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
		extCmd.Parse(os.Args[2:])

		if *inputDir == "" {
			fmt.Fprintln(os.Stderr, "Error: -input directory is required")
			extCmd.Usage()
			os.Exit(1)
		}

		if err := listExtensions(*inputDir, *workers); err != nil {
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

func deduplicateFiles(config Config) error {
	// Map to track seen file hashes
	seenHashes := &sync.Map{}
	var mu sync.Mutex
	var totalFiles, uniqueFiles, skippedFiles, alreadyExisted int64
	var processedFiles int64

	// First, scan the output directory to build hash map of existing files (in parallel)
	fmt.Println("Scanning output directory for existing files...")
	var existingCount int64
	var existingProcessed int64
	if _, err := os.Stat(config.OutputDir); err == nil {
		existingJobs := make(chan string, 1000)
		var existingWg sync.WaitGroup

		// Progress ticker for existing files
		stopProgress := make(chan bool)
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					fmt.Fprintf(os.Stderr, "\rScanning existing files: %d processed", atomic.LoadInt64(&existingProcessed))
				case <-stopProgress:
					return
				}
			}
		}()

		// Start workers for scanning existing files
		for i := 0; i < config.NumWorkers; i++ {
			existingWg.Add(1)
			go func() {
				defer existingWg.Done()
				for path := range existingJobs {
					if hash, err := hashFile(path); err == nil {
						seenHashes.Store(hash, path)
						mu.Lock()
						existingCount++
						mu.Unlock()
					}
					atomic.AddInt64(&existingProcessed, 1)
				}
			}()
		}

		// Walk and queue existing files
		filepath.Walk(config.OutputDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			existingJobs <- path
			return nil
		})

		close(existingJobs)
		existingWg.Wait()
		stopProgress <- true
	}
	fmt.Fprintf(os.Stderr, "\rScanning existing files: %d processed\n", existingProcessed)
	fmt.Printf("Found %d existing files in output directory\n", existingCount)

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
				if shouldProcess(job.Path, config) {
					hash, err := hashFile(job.Path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "\nError hashing %s: %v\n", job.Path, err)
						atomic.AddInt64(&processedFiles, 1)
						continue
					}

					// Check if we've seen this hash before (including existing files)
					if _, exists := seenHashes.LoadOrStore(hash, job.Path); !exists {
						// First time seeing this file content
						if err := copyFilePreservePath(job.Path, config.InputDir, config.OutputDir, job.Info); err != nil {
							fmt.Fprintf(os.Stderr, "\nError copying %s: %v\n", job.Path, err)
						} else {
							atomic.AddInt64(&uniqueFiles, 1)
						}
					} else {
						atomic.AddInt64(&alreadyExisted, 1)
					}
				} else {
					atomic.AddInt64(&skippedFiles, 1)
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
					fmt.Fprintf(os.Stderr, "Warning: skipping %s: %v\n", path, err)
					return nil
				}
			}
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

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func copyFile(srcPath, dstDir string, info os.FileInfo) error {
	// Check if source file still exists
	if _, err := os.Lstat(srcPath); err != nil {
		return err
	}

	// Generate unique filename if collision occurs
	baseName := filepath.Base(srcPath)
	dstPath := filepath.Join(dstDir, baseName)

	// Handle filename collisions by appending a number
	counter := 1
	for {
		if _, err := os.Stat(dstPath); os.IsNotExist(err) {
			break
		}
		ext := filepath.Ext(baseName)
		nameWithoutExt := strings.TrimSuffix(baseName, ext)
		dstPath = filepath.Join(dstDir, fmt.Sprintf("%s_%d%s", nameWithoutExt, counter, ext))
		counter++
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}

	// Preserve file permissions
	return os.Chmod(dstPath, info.Mode())
}

func copyFilePreservePath(srcPath, inputDir, outputDir string, info os.FileInfo) error {
	// Check if source file still exists
	if _, err := os.Lstat(srcPath); err != nil {
		return err
	}

	// Calculate relative path from input directory
	relPath, err := filepath.Rel(inputDir, srcPath)
	if err != nil {
		return err
	}

	// Construct destination path preserving directory structure
	dstPath := filepath.Join(outputDir, relPath)

	// Create all parent directories
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	// Check if destination file already exists
	if _, err := os.Stat(dstPath); err == nil {
		// File exists, skip it (already handled by hash checking)
		return nil
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}

	// Preserve file permissions
	return os.Chmod(dstPath, info.Mode())
}

func listExtensions(inputDir string, numWorkers int) error {
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
