package caches

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"time"
)

type FileSystemCache struct {
	root string
}

type Entry struct {
	Content  string    `json:"content,omitempty"`
	LastUsed time.Time `json:"last_used"`
}

type Entries map[string]Entry

const ENTRIES_FILE = "entries.json"

// GetFileSystemCachePath gets the path to the directory to use for storing the
// cache. It defaults to ~/.ctcache/cache and can be overridden by setting
// CLANG_TIDY_CACHE_DIR environment variable.
func GetFileSystemCachePath() string {
	if envPath := os.Getenv("CLANG_TIDY_CACHE_DIR"); len(envPath) > 0 {
		return envPath
	}
	usr, _ := user.Current()
	return path.Join(usr.HomeDir, ".ctcache", "cache")
}

func NewFsCache() *FileSystemCache {
	return &FileSystemCache{
		root: GetFileSystemCachePath(),
	}
}

// Read the cache entries from JSON. For errors, we log and return an empty
// `Entries` map so that execution can continue.
func readJson(filepath string) Entries {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return Entries{} // file doesn't exist yet, equivalent to empty file
	}

	jsonData, err := ioutil.ReadFile(filepath)
	if err != nil {
		fmt.Printf("Error reading cache JSON: %v\n", err)
		return Entries{}
	}

	entries := Entries{}
	err = json.Unmarshal(jsonData, &entries)
	if err != nil {
		fmt.Printf("Error decoding cache JSON: %v\n", err)
	}
	return entries
}

// Check if we have a cache hit in JSON
func checkJsonEntry(c *FileSystemCache, digest []byte) []byte {
	entries := readJson(path.Join(c.root, ENTRIES_FILE))
	entry, exists := entries[hex.EncodeToString(digest)]
	if !exists {
		return nil
	}

	result := []byte(entry.Content)
	c.SaveEntry(digest, result) // to update the last used time
	return result
}

// Check if we have a cache hit in the filesystem
func checkFsEntry(c *FileSystemCache, digest []byte) ([]byte, error) {
	_, entryPath := defineEntryPath(c.root, digest)
	_, err := os.Stat(entryPath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		} else {
			return nil, err
		}
	}

	source, err := os.Open(entryPath)
	if err != nil {
		return nil, err
	}
	defer source.Close()

	return ioutil.ReadAll(source)
}

// `Prune()` consolidates entries into the JSON file so we want to check that first.
// A hit in the filesystem is a fallback and it means that `Prune()` has not run yet.
func (c *FileSystemCache) FindEntry(digest []byte) ([]byte, error) {
	if content := checkJsonEntry(c, digest); content != nil {
		return content, nil
	}
	return checkFsEntry(c, digest)
}

func (c *FileSystemCache) SaveEntry(digest []byte, content []byte) error {
	entryRoot, entryPath := defineEntryPath(c.root, digest)

	err := os.MkdirAll(entryRoot, 0755)
	if err != nil {
		return err
	}

	destination, err := os.Create(entryPath)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = destination.Write(content)
	if err != nil {
		return err
	}

	return nil
}

func defineEntryPath(root string, digest []byte) (string, string) {
	encodedDigest := hex.EncodeToString(digest)
	entryRoot := path.Join(root, encodedDigest[0:2], encodedDigest[2:4])
	entryPath := path.Join(entryRoot, encodedDigest[4:])
	return entryRoot, entryPath
}

// Remove cache entries that have not been used in the last `numWeeks` and
// consolidate the remainder in a single JSON file. The consolidation helps
// speed up later pruning since we only need to look up the single file.
func Prune(numWeeks int) error {
	root := GetFileSystemCachePath()
	err := os.MkdirAll(root, 0755)
	if err != nil {
		return err
	}

	// Populate `Entries` from the many files in the filesystem
	entries := readJson(path.Join(root, ENTRIES_FILE))
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() == ENTRIES_FILE {
			return nil
		}
		content, err := ioutil.ReadFile(path)
		if err != nil {
			fmt.Println("Error reading file:", err)
			return nil
		}

		// The digest is split over 2 parent dir name and the file name, e.g. `ab/cd/efg...`
		parent1 := filepath.Base(filepath.Dir(filepath.Dir(path)))
		parent2 := filepath.Base(filepath.Dir(path))
		digest := parent1 + parent2 + info.Name()
		entries[digest] = Entry{Content: string(content), LastUsed: info.ModTime()}

		// We no longer need the file since the content is going into JSON.
		err = os.Remove(path)
		if err != nil {
			fmt.Println("Error deleting file:", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove all the directories as well now that they are empty
	paths, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, pathInfo := range paths {
		if !pathInfo.IsDir() {
			continue
		}

		if err := os.RemoveAll(filepath.Join(root, pathInfo.Name())); err != nil {
			fmt.Println("Error deleting path:", err)
			return err
		}
	}

	// Keep only the most recent entries
	now := time.Now()
	duration := time.Duration(numWeeks*7*24) * time.Hour
	prunedEntries := Entries{}
	for key, value := range entries {
		if now.Sub(value.LastUsed) <= duration {
			prunedEntries[key] = value
		}
	}

	fmt.Println("Found", len(entries), "cache entries in", root)
	diff := len(entries) - len(prunedEntries)
	if diff == 0 {
		fmt.Println("No outdated entries")
	} else {
		fmt.Println("Removed", diff, "outdated cache entries")
	}

	// Write to JSON
	jsonData, err := json.MarshalIndent(prunedEntries, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path.Join(root, ENTRIES_FILE), jsonData, 0644)
}
