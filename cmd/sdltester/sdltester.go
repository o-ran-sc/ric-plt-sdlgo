/*
   Copyright (c) 2019 AT&T Intellectual Property.
   Copyright (c) 2018-2019 Nokia.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*
 * This source code is part of the near-RT RIC (RAN Intelligent Controller)
 * platform project (RICP).
 */

package main

import (
	"fmt"
	"os"
	"time"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
)

/*
 * This program demonsrates the basic usage of sdlgo module.
 *
 * The following scenarios are provided:
 *
 * - write: Write data. Performance is measured.
 *
 * - read: Read data. Performance is measured.
 *
 * - remove: Remove data. Performance is measured.
 *
 * - noexist: Read non-existing data. Performance is measured and empty container is returned as nothing was
 *            found.
 *
 * - getall: Read all keys within a namespace. One can manually add keys under the used namespace and all
 *           those keys should be returned here. Performance is measured.
 *
 * - removeall: Remove all keys within a namespace. Performance is measured.
 *
 * - emptymap: Write an empty container. Performance is measured.
 *
 * - multiple: Make two writes. Performance is measured.
 *
 * - emptydata: Write empty data for a key. Performance is measured.
 *
 * - writeif and writeifnot: Write if old data (written with the "write" option) has remained and remains
 *                           unchanged during the function call. Do not write if data has changed. Performance
 *                           is measured.
 *
 * - removeif: Remove if old data (written with the "write" option) has remained and remains
 *             unchanged during the function call. Do not remove data if data has changed. Performance
 *             is measured.
 */

func main() {
	sdl := sdlgo.NewSyncStorage()

	if len(os.Args) > 1 {
		switch command := os.Args[1]; command {
		case "write":
			write(sdl)
		case "read":
			read(sdl)
		case "remove":
			remove(sdl)
		case "noexist":
			noexist(sdl)
		case "getall":
			getall(sdl)
		case "listkeys":
			listkeys(sdl)
		case "removeall":
			removeall(sdl)
		case "emptymap":
			emptymap(sdl)
		case "multiple":
			multiple(sdl)
		case "emptydata":
			emptydata(sdl)
		case "writeif":
			writeif(sdl)
		case "writeifnot":
			writeifnot(sdl)
		case "removeif":
			removeif(sdl)
		default:
			printHelp()
		}

	} else {
		printHelp()
	}
}

func printHelp() {
	fmt.Println("Usage: sdltester <command>")
	fmt.Println("Commands:")
	fmt.Println("write        Write data. Performance is measured")
	fmt.Println("read         Read data. Performance is measured")
	fmt.Println("remove       Remove data. Performance is measured")
	fmt.Println("noexist      Read non-existing data. Performance is measured and empty container is returned as nothing was")
	fmt.Println("             found")
	fmt.Println("getall       Read all keys within a namespace. One can manually add keys under the used namespace and all")
	fmt.Println("             those keys should be returned here. Performance is measured")
	fmt.Println("listkeys     List keys in the given namespace matching key search pattern.")
	fmt.Println("removeall    Remove all keys within a namespace. Performance is measured")
	fmt.Println("emptymap     Write an empty container. Performance is measured")
	fmt.Println("multiple     Make two writes. Performance is measured")
	fmt.Println("emptydata    Write empty data for a key. Performance is measured")
	fmt.Println("writeif      Write if old data (written with the \"write\" option) has remained and remains")
	fmt.Println("             unchanged during the function call. Do not write if data has changed. Performance")
	fmt.Println("             is measured")
	fmt.Println("writeifnot   Write only if key is not set. Performance is measured")
	fmt.Println("removeif     Remove if old data (written with the \"write\" option) has remained and remains")
	fmt.Println("             unchanged during the function call. Do not remove data if data has changed. Performance")
	fmt.Println("             is measured")
}

func write(sdl *sdlgo.SyncStorage) {
	data := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
		0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44}
	start := time.Now()
	err := sdl.Set("tag1", "key1", data)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Write: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func read(sdl *sdlgo.SyncStorage) {
	k := []string{"key1"}
	start := time.Now()
	data, err := sdl.Get("tag1", k)
	elapsed := time.Since(start)
	if err == nil {
		value, ok := data["key1"]
		if ok && value != nil {
			fmt.Printf("Read: %s\n", elapsed)
		} else {
			fmt.Printf("Read, not found: %s\n", elapsed)
		}

	} else {
		fmt.Println(err)
	}
}

func remove(sdl *sdlgo.SyncStorage) {
	k := []string{"key1"}
	start := time.Now()
	err := sdl.Remove("tag1", k)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Remove: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func noexist(sdl *sdlgo.SyncStorage) {
	start := time.Now()
	_, err := sdl.Get("tag1", []string{"no1", "no2"})
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Noexist: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func getall(sdl *sdlgo.SyncStorage) {
	start := time.Now()
	keys, err := sdl.GetAll("tag1")
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Getall: %s\n", elapsed)
		for _, i := range keys {
			fmt.Println(i)
		}
	} else {
		fmt.Println(err)
	}
}

func listkeys(sdl *sdlgo.SyncStorage) {
	start := time.Now()
	ns := "tag1"
	pattern := "*"
	keys, err := sdl.ListKeys(ns, pattern)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("ListKeys(%s, %s): %s\n", ns, pattern, elapsed)
		for _, i := range keys {
			fmt.Println(i)
		}
	} else {
		fmt.Println(err)
	}
}

func removeall(sdl *sdlgo.SyncStorage) {
	start := time.Now()
	err := sdl.RemoveAll("tag1")
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Removeall: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func emptymap(sdl *sdlgo.SyncStorage) {
	start := time.Now()
	err := sdl.Set("tag1", "", "")
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Emptymap: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func multiple(sdl *sdlgo.SyncStorage) {
	data := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
		0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44}
	start := time.Now()
	err := sdl.Set("tag1", "key1m", data)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Multiple: %s ", elapsed)
	} else {
		fmt.Println(err)
	}
	start = time.Now()
	err = sdl.Set("tag1", "key2m", data)
	elapsed = time.Since(start)
	if err == nil {
		fmt.Printf(" %s \n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func emptydata(sdl *sdlgo.SyncStorage) {
	data := []byte{}
	start := time.Now()
	err := sdl.Set("tag1", "key1", data)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Emptydata: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func writeif(sdl *sdlgo.SyncStorage) {
	oldVec := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
		0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44}
	newVec := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
		0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x66}
	start := time.Now()
	_, err := sdl.SetIf("tag1", "key1", oldVec, newVec)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Writeif: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func writeifnot(sdl *sdlgo.SyncStorage) {
	vec := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
		0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x88}
	start := time.Now()
	_, err := sdl.SetIfNotExists("tag1", "key1", vec)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Writeifnot: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}

func removeif(sdl *sdlgo.SyncStorage) {
	vec := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00,
		0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x88}
	start := time.Now()
	_, err := sdl.RemoveIf("tag1", "key1", vec)
	elapsed := time.Since(start)
	if err == nil {
		fmt.Printf("Removeif: %s\n", elapsed)
	} else {
		fmt.Println(err)
	}
}
