package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger2"
)

func main() {
	scenarios := []struct {
		Name string
		Opts func() *badger.Options
	}{
		{
			Name: "Default config",
			Opts: func() *badger.Options { return &badger.DefaultOptions },
		},
		{
			Name: "NumVersionToKeep0",
			Opts: func() *badger.Options {
				opts := badger.DefaultOptions
				opts.NumVersionsToKeep = 0
				return &opts
			},
		},
		{
			Name: "CompactL0OnClose",
			Opts: func() *badger.Options {
				opts := badger.DefaultOptions
				opts.CompactL0OnClose = true
				return &opts
			},
		},
		{
			Name: "Aggressive",
			Opts: func() *badger.Options {
				opts := badger.DefaultOptions
				opts.NumVersionsToKeep = 0
				opts.CompactL0OnClose = true
				opts.NumLevelZeroTables = 1
				opts.NumLevelZeroTablesStall = 2
				return &opts
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
			if err := runScenario(path, s.Opts()); err != nil {
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

func runScenario(path string, opts *badger.Options) error {
	ds, err := badger.NewDatastore(path, opts)
	if err != nil {
		return fmt.Errorf("creating datastore: %s", err)
	}
	defer func() {
		ds.DB.RunValueLogGC(0.001)
		if err := ds.Close(); err != nil {
			panic("Closing datastore: " + err.Error())
		}
	}()

	if err := runWorkload(ds); err != nil {
		return fmt.Errorf("running workload: %s", err)
	}

	return err
}

func runWorkload(ds *badger.Datastore) error {
	r := rand.New(rand.NewSource(22))
	keySize := 16
	valueSize := 1024 * 10
	numItems := 100000

	keys := make([]datastore.Key, numItems)
	value := make([]byte, valueSize)

	for i := 0; i < numItems; i++ {
		bKey := make([]byte, keySize)
		if _, err := r.Read(bKey); err != nil {
			return fmt.Errorf("generating key: %s", err)
		}
		key := datastore.NewKey(string(bKey))
		keys[i] = key
		if _, err := r.Read(value); err != nil {
			return fmt.Errorf("generating value: %s", err)
		}
		if err := ds.Put(key, value); err != nil {
			return fmt.Errorf("put operation: %s", err)
		}
	}

	for i := 0; i < numItems; i++ {
		if err := ds.Delete(keys[i]); err != nil {
			return fmt.Errorf("delete key: %s", err)
		}
	}
	return nil
}
