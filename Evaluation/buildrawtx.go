package main

import (
	"fmt"
    "bufio"
    "log"
	"os"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	//"sync"
	"encoding/csv"
	"runtime"
)

// logger configured to emit app name, line number, timestamps etc.
var mylog = log.New(os.Stderr, "app: ", log.LstdFlags|log.Lshortfile)

func main() {
    //file, err := os.Open("/vol/bitbucket/hh2214/rtx/txs.txt")
	file, err := os.Open("/vol/bitbucket/hh2214/rtx/txs-2.txt")
	fatal(err)
	
	defer file.Close()

	// Connect to local bitcoin core RPC server using HTTP POST mode.
	connCfg := &rpcclient.ConnConfig{
		Host:         "146.169.44.83:8332", //86:8332",
		User:         "a",
		Pass:         "a",
		HTTPPostMode: true, // Bitcoin core only supports HTTP POST mode
		DisableTLS:   true, // Bitcoin core does not provide TLS by default
	}

	// Notice the notification parameter is nil since notifications are
	// not supported in HTTP POST mode.
	client, err := rpcclient.New(connCfg, nil)
	fatal(err)

	f, err := os.OpenFile("/vol/bitbucket/hh2214/rtx/raw-seq.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	fatal(err)

	w := csv.NewWriter(f)
	//var wg sync.WaitGroup
	scanner := bufio.NewScanner(file)

	//maxGoroutines := 500000
    //guard := make(chan struct{}, maxGoroutines)

    for scanner.Scan() {
		//txHash := "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098"// 
		txHash := scanner.Text()
		if len(txHash) == 64 {
			//guard <- struct{}{}

			//wg.Add(1)
			//go func(txHash string) {
			//	defer wg.Done()

				txidHash, err := chainhash.NewHashFromStr(txHash)
				fatal(err)

				rawVerTx, err := client.GetRawTransactionVerbose(txidHash)
				fatal(err)

				w.Write([]string{
					rawVerTx.Txid, // txid of tx - not necessarily same as hash
					rawVerTx.Hash,  // hash of hex data
					rawVerTx.Hex, // tx hex data
				})
				w.Flush()
				
				//<-guard
			//}(txHash)
		}
	}
	fmt.Println("WAITING")
	//wg.Wait()
	
    if err := scanner.Err(); err != nil {
		fmt.Println("hmm boud to err tho")
		//log.Fatal(err)
    }
}

/*
func delegate(client *rpcclient.Client, wg *sync.WaitGroup, txHash string, w *csv.Writer) {
	//var wg sync.WaitGroup
	//var mutex = sync.Mutex{}

	//wg.Add(1)
	
	defer wg.Done()

	txidHash, err := chainhash.NewHashFromStr(txHash)
	fatal(err)

	rawVerTx, err := client.GetRawTransactionVerbose(txidHash)
	fatal(err)

	//mutex.Lock()
	w.Write([]string{
		rawVerTx.Hex,
	})
	w.Flush()
	//mutex.Unlock()

	//wg.Wait()
}
*/

func fatal(err error) {
	_, file, line, _ := runtime.Caller(1)
	if err != nil {
		mylog.Fatalln("At:", file, ":", line, "-", err)
	}
}


		// fmt.Println(scanner.Text())
		// txHash := "11172aed3a61fe64ae82cb02e886757d352a4452f956a0703d0809880538ba58"
		// txHash := "bb41a757f405890fb0f5856228e23b715702d714d59bf2b1feb70d8b2b4e3e08"
		//"8f05c4f40742fcfceb61702af0152b3c2d63be2c04826e036fd17d34ae294478"
		// 07a1b9c2e8fb05c93cdd05efc1fd34247406a4388e082aecb63ccf55ea1dd8c5


		//fmt.Println("Txid", rawVerTx.Txid)
		//fmt.Println("Hash", rawVerTx.Hash)
		//fmt.Println("Size", rawVerTx.Size)
		//fmt.Println("Vsize", rawVerTx.Vsize)
		//fmt.Println("Version", rawVerTx.Version)
		//fmt.Println("LockTime", rawVerTx.LockTime)
		//fmt.Println()
		//fmt.Println("BlockHash", rawVerTx.BlockHash)
		//fmt.Println("Confirmations", rawVerTx.Confirmations)
		//fmt.Println("Time", rawVerTx.Time)
		//fmt.Println("Blocktime", rawVerTx.Blocktime)
		//fmt.Println()
		/*
		for _, vin := range rawVerTx.Vin {
			fmt.Println("Coinbase", vin.Coinbase)
			fmt.Println("Txid", vin.Txid)
			fmt.Println("Vout", vin.Vout)
			fmt.Println("ScriptSig - ASM", vin.ScriptSig.Asm)
			fmt.Println("ScriptSig - HEX", vin.ScriptSig.Hex)
			fmt.Println("Sequence", vin.Sequence)
			fmt.Println("Witness", vin.Witness)
			
		}

		for _, vout := range rawVerTx.Vout {
			fmt.Println("Value", vout.Value)
			fmt.Println("N", vout.N)
			fmt.Println("ScriptPubKeyResult - ASM", vout.ScriptPubKey.Asm)
			fmt.Println("ScriptPubKeyResult - HEX", vout.ScriptPubKey.Hex)
			fmt.Println("ScriptPubKeyResult - ReqSigs", vout.ScriptPubKey.ReqSigs)
			fmt.Println("ScriptPubKeyResult - Type", vout.ScriptPubKey.Type)
			fmt.Println("ScriptPubKeyResult - Addresses", vout.ScriptPubKey.Addresses)
		}
		*/
		/*
		rawTx, err := client.GetRawTransaction(txidHash)
		if err != nil {
			log.Fatal(err)
		}*/

//		fmt.Println("RAW", rawTx)