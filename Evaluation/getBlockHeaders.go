package main

import (
	"fmt"
	"log"
	"os"
	"github.com/btcsuite/btcd/rpcclient"
	"encoding/csv"
	"strconv"
	"sync"
)

func main() {
	hostAddrs := []string{"44.83", "44.82", "44.81", "44.85", "46.236", "46.80"}

	clients := initClients(hostAddrs)

	for _, client := range clients {	
		defer client.Shutdown()
	}

	// Get the current block count.
	blockCount, err := clients[0].GetBlockCount()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Block count: %d", blockCount)
	
	partitions := []int64{100000, 200000, 300000, 400000, 500000, blockCount}
	
	var wg sync.WaitGroup
	prevPartition := int64(0) // start from genesis block

	for index, partition := range partitions {
		f, err := os.OpenFile("/data/hh2214/bh/bh_" + strconv.FormatInt(partition, 10) + ".csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}
		w := csv.NewWriter(f)

		wg.Add(1)
		go delegate(clients[index], prevPartition, partition, w)
		fmt.Println("DELEGATED!!")
		prevPartition = partition + 1;
	}
	wg.Wait()
}

func initClients(hostAddrs []string) []*rpcclient.Client {
	var clients []*rpcclient.Client 
	for _, hostAddr := range hostAddrs {
		// Connect to local bitcoin core RPC server using HTTP POST mode.
		connCfg := &rpcclient.ConnConfig{
			Host:         "146.169." + hostAddr + ":8332", // connect to port 8332 
			User:         "a",
			Pass:         "a",
			HTTPPostMode: true, // Bitcoin core only supports HTTP POST mode
			DisableTLS:   true, // Bitcoin core does not provide TLS by default
		}
		// Notice the notification parameter is nil since notifications are
		// not supported in HTTP POST mode.
		client, err := rpcclient.New(connCfg, nil)
		if err != nil {
			log.Fatal(err)
		}

		clients = append(clients, client)
	}

	return clients;
}

func delegate(client *rpcclient.Client, startIndex int64, endIndex int64, w *csv.Writer) {
	var wg sync.WaitGroup
	var mutex = sync.Mutex{}

	for blockHeight := startIndex; blockHeight <= endIndex; blockHeight++ { // ignore genesis block
		wg.Add(1)
		go execute(blockHeight, client, w, &wg, &mutex)
	}
	wg.Wait()
}

func execute(blockHeight int64, client *rpcclient.Client, w *csv.Writer, wg *sync.WaitGroup, mutex *sync.Mutex){
	fmt.Println("Writing-" + strconv.FormatInt(blockHeight, 10))

	defer wg.Done()

	blockHash, err := client.GetBlockHash(int64(blockHeight))
	if err != nil {
		log.Fatal(err)
	}

	blockHeaderVerbose, err := client.GetBlockHeaderVerbose(blockHash)
	if err != nil {
		log.Fatal(err)
	}

	// add block header to CSV file
	mutex.Lock()
	
	prevHash := blockHeaderVerbose.PreviousHash
	if blockHeight == 0 { // checking for genesis block
		prevHash = "0000000000000000000000000000000000000000000000000000000000000000" // 64 char len default str 
	}

	w.Write([]string{
		strconv.FormatInt(int64(blockHeaderVerbose.Version), 10), // int32 value
		prevHash,
		blockHeaderVerbose.MerkleRoot,
		strconv.FormatInt(int64(blockHeaderVerbose.Time), 10),
		blockHeaderVerbose.Bits, // should really be called nBits
		strconv.FormatUint(uint64(blockHeaderVerbose.Nonce), 10),
		
		strconv.FormatInt(int64(blockHeaderVerbose.Height), 10), // int32 value  - a useful extra field - not included in 'block-header'

		// blockHeaderVerbose.Hash, 
		// strconv.FormatUint(uint64(blockHeaderVerbose.Confirmations), 10),
		// blockHeaderVerbose.VersionHex,
		// blockHeaderVerbose.NextHash,
		// strconv.FormatFloat(blockHeaderVerbose.Difficulty, 'f', -1, 64),
	})
	w.Flush()
	mutex.Unlock()
}