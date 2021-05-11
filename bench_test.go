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

package sdlgo_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
)

type nsKeysBenchmark struct {
	ns        string
	nsCount   int
	keyName   string
	keyCount  int
	keySize   int
	valueSize int
}

type keysBenchmark struct {
	keyName   string
	keyCount  int
	keySize   int
	valueSize int
}

type groupBenchmark struct {
	key         string
	member      string
	memberCount int
}

func (bm nsKeysBenchmark) String(oper string) string {
	return fmt.Sprintf("op=%s ns-cnt=%d key-cnt=%d key-sz=%d value-sz=%d",
		oper, bm.nsCount, bm.keyCount, bm.keySize, bm.valueSize)
}

func (bm keysBenchmark) String(oper string) string {
	return fmt.Sprintf("op=%s key-cnt=%d key-sz=%d value-sz=%d",
		oper, bm.keyCount, bm.keySize, bm.valueSize)
}

func (bm groupBenchmark) String(oper string) string {
	return fmt.Sprintf("op=%s, mbr-cnt=%d", oper, bm.memberCount)
}

func getNsKeyBenchmarkInput() []nsKeysBenchmark {
	return []nsKeysBenchmark{
		{"ns-a", 1, "a", 100, 10, 64},
		{"ns-b", 10, "b", 100, 10, 64},
		{"ns-c", 100, "c", 100, 10, 64},
		{"ns-d", 1000, "d", 100, 10, 64},
		{"ns-e", 10000, "e", 100, 10, 64},
	}
}

func getKeyBenchmarkInput() []keysBenchmark {
	return []keysBenchmark{
		{"a", 1, 10, 64},
		{"b", 1, 10, 1024},
		{"c", 1, 10, 64 * 1024},
		{"d", 1, 10, 1024 * 1024},
		{"e", 1, 10, 10 * 1024 * 1024},

		{"f", 1, 100, 64},
		{"g", 1, 100, 1024},
		{"h", 1, 100, 64 * 1024},
		{"i", 1, 100, 1024 * 1024},
		{"j", 1, 100, 10 * 1024 * 1024},

		{"k", 2, 10, 64},
		{"l", 10, 10, 64},
		{"m", 100, 10, 64},
		{"n", 1000, 10, 64},
		{"r", 5000, 10, 64},

		{"s", 2, 100, 64},
		{"t", 10, 100, 64},
		{"u", 100, 100, 64},
		{"v", 1000, 100, 64},
		{"x", 5000, 100, 64},
	}
}

func BenchmarkMultiNamespaceKeysWrite(b *testing.B) {
	benchmarks := getNsKeyBenchmarkInput()

	for _, bm := range benchmarks {
		b.Run(bm.String("ns-keys-set"), func(b *testing.B) {
			sdl := sdlgo.NewSyncStorage()
			value := strings.Repeat("1", bm.valueSize)
			keyVals := make([]string, 0)
			for i := 0; i < bm.keyCount; i++ {
				key := strings.Repeat(bm.keyName+strconv.Itoa(i), bm.keySize)
				keyVals = append(keyVals, key, value)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for n := 0; n < bm.nsCount; n++ {
						err := sdl.Set(bm.ns+strconv.Itoa(n), keyVals)
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		})
	}
}

func BenchmarkMultiNamespaceKeysRead(b *testing.B) {
	benchmarks := getNsKeyBenchmarkInput()

	for _, bm := range benchmarks {
		b.Run(bm.String("keys-get"), func(b *testing.B) {
			sdl := sdlgo.NewSyncStorage()
			keys := make([]string, 0)
			for i := 0; i < bm.keyCount; i++ {
				key := strings.Repeat(bm.keyName+strconv.Itoa(i), bm.keySize)
				keys = append(keys, key)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for n := 0; n < bm.nsCount; n++ {
						_, err := sdl.Get(bm.ns+strconv.Itoa(n), keys)
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		})
	}
}

func BenchmarkKeysWrite(b *testing.B) {
	benchmarks := getKeyBenchmarkInput()

	for _, bm := range benchmarks {
		b.Run(bm.String("keys-set"), func(b *testing.B) {
			sdl := sdlgo.NewSdlInstance("namespace", sdlgo.NewDatabase())
			value := strings.Repeat("1", bm.valueSize)
			keyVals := make([]string, 0)
			for i := 0; i < bm.keyCount; i++ {
				key := strings.Repeat(bm.keyName+strconv.Itoa(i), bm.keySize)
				keyVals = append(keyVals, key, value)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := sdl.Set(keyVals)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkKeysRead(b *testing.B) {
	benchmarks := getKeyBenchmarkInput()

	for _, bm := range benchmarks {
		b.Run(bm.String("keys-get"), func(b *testing.B) {
			sdl := sdlgo.NewSdlInstance("namespace", sdlgo.NewDatabase())
			keys := make([]string, 0)
			for i := 0; i < bm.keyCount; i++ {
				key := strings.Repeat(bm.keyName+strconv.Itoa(i), bm.keySize)
				keys = append(keys, key)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := sdl.Get(keys)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkGroupMemberAdd(b *testing.B) {
	benchmarks := []groupBenchmark{
		{"a", "x", 1},
		{"b", "x", 100},
		{"c", "x", 10000},
		{"d", "x", 1000000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String("group-add-member"), func(b *testing.B) {
			sdl := sdlgo.NewSdlInstance("namespace", sdlgo.NewDatabase())
			members := make([]string, 0)
			for i := 0; i < bm.memberCount; i++ {
				member := bm.member + strconv.Itoa(i)
				members = append(members, member)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := sdl.AddMember(bm.key, members)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
