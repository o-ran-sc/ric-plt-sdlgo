/*
   Copyright (c) 2019 AT&T Intellectual Property.
   Copyright (c) 2018-2022 Nokia.

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

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
	"time"
)

var sdl *sdlgo.SyncStorage

func init() {
	sdl = sdlgo.NewSyncStorage()
}

func exampleSet() {
	err := sdl.Set("my-namespace", "somestringdata", "data", "someintdata", 42)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Data written successfully")
	}
}

func exampleGet() {
	retMap, err := sdl.Get("my-namespace", []string{"somestringdata", "someintdata"})
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Data read successfully")
		for k, v := range retMap {
			fmt.Println("\t", k, "key value is", v)
		}
	}
}

func exampleClose() {
	sdl.Close()
}

func sdlNotify(ch string, events ...string) {
	fmt.Printf("CB1 channel=%+v, events=%+v\n", ch, events[0])
}

func sdlNotify2(ch string, events ...string) {
	fmt.Printf("CB2 channel=%+v, events=%+v\n", ch, events[0])
}

func main() {
	sdl.SubscribeChannel("dcapterm_subsRTPM-localhost:55566", sdlNotify, "my-ch")
	sdl.SubscribeChannel("dcapterm_subsRTPM-localhost:55565", sdlNotify2, "my-ch")
	time.Sleep(3 * time.Second)
	sdl.SetAndPublish("dcapterm_subsRTPM-localhost:55566", []string{"my-ch", "my-event1"}, "my-key", "my-data")
	sdl.SetAndPublish("dcapterm_subsRTPM-localhost:55565", []string{"my-ch", "my-event2"}, "my-key", "my-data")

	time.Sleep(2 * time.Second)
	//sdl.UnsubscribeChannel("dcapterm_subsRTPM-localhost:55565", "my-ch")
	//time.Sleep(2 * time.Second)
	//sdl.SetAndPublish("dcapterm_subsRTPM-localhost:55565", []string{"my-ch", "my-event2"}, "my-key", "my-data")
	time.Sleep(2 * time.Second)

	//exampleSet()
	//exampleGet()
	//exampleClose()
}
