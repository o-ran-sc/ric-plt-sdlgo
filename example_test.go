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

package sdlgo_test

import (
	"fmt"

	"gerrit.oran-osc.org/r/ric-plt/sdlgo"
)

var sdl *sdlgo.SdlInstance

func init() {
	sdl = sdlgo.NewSdlInstance("namespace", sdlgo.NewDatabase())
}

func ExampleSdlInstance_Set() {
	err := sdl.Set("stringdata", "data", "intdata", 42)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Data written successfully")
	}
	// Output: Data written successfully
}

func ExampleSdlInstance_Get() {
	retMap, err := sdl.Get([]string{"strigdata", "intdata"})
	if err != nil {
		panic(err)
	} else {
		fmt.Println(retMap)
	}
}
