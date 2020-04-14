package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger"
)

func main() {
	scenarios := []struct {
		Name string
		Opts func(string) badger.Options
	}{
		{
			Name: "Aggressive",
			Opts: func(path string) badger.Options {
				opts := badger.DefaultOptions(path)
				opts.NumVersionsToKeep = 0
				opts.CompactL0OnClose = true
				opts.ValueLogFileSize = 1024 * 1024 * 20
				return opts
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(len(scenarios))
	for _, s := range scenarios {
		s := s
		go func() {
			defer wg.Done()
			path, err := ioutil.TempDir("", "")
			if err != nil {
				fmt.Printf("creating temp dir: %s", err)
				os.Exit(1)
			}
			if err := runScenario(path, s.Opts(path)); err != nil {
				fmt.Printf("running scenario: %s", err)
				os.Exit(1)
			}
			var m Metrics
			err = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				ext := filepath.Ext(info.Name())
				switch ext {
				case ".sst":
					m.NumSST++
					m.SizeSSTs += info.Size() / 1024
				case ".vlog":
					m.NumVLOG++
					m.SizeVLOGs += info.Size() / 1024
				}
				return nil
			})
			if err != nil {
				panic(err)
			}
			fmt.Printf("%s:\n\t%#v\n", s.Name, m)
			os.RemoveAll(path)
		}()
	}
	wg.Wait()
}

type Metrics struct {
	NumSST    int
	SizeSSTs  int64
	NumVLOG   int
	SizeVLOGs int64
}

func runScenario(path string, opts badger.Options) error {
	ds, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("creating datastore: %s", err)
	}
	defer func() {
		for {
			if err := ds.RunValueLogGC(0.01); err == badger.ErrNoRewrite {
				break
			}
		}
		if err := ds.Close(); err != nil {
			panic("Closing datastore: " + err.Error())
		}
		ds, _ = badger.Open(badger.DefaultOptions(path))
		if err := ds.Close(); err != nil {
			panic("Closing datastore: " + err.Error())
		}
	}()

	if err := runWorkload(ds); err != nil {
		return fmt.Errorf("running workload: %s", err)
	}

	return err
}

func runWorkload(ds *badger.DB) error {
	r := rand.New(rand.NewSource(22))
	keySize := 16
	valueSize := 1024
	numItems := 1000000

	keys := make([][]byte, numItems)
	value := make([]byte, valueSize)

	for i := 0; i < numItems; i++ {
		keys[i] = make([]byte, keySize)
		if _, err := r.Read(keys[i]); err != nil {
			return fmt.Errorf("generating key: %s", err)
		}
		if _, err := r.Read(value); err != nil {
			return fmt.Errorf("generating value: %s", err)
		}
		err := ds.Update(func(txn *badger.Txn) error {
			if err := txn.Set(keys[i], value); err != nil {
				return fmt.Errorf("put operation: %s", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("making update: %s", err)
		}

	}

	for i := 0; i < numItems; i++ {
		err := ds.Update(func(txn *badger.Txn) error {
			if err := txn.Delete(keys[i]); err != nil {
				return fmt.Errorf("delete key: %s", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("deleting: %s", err)
		}
	}
	return nil
}
