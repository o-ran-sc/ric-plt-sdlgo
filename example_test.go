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
