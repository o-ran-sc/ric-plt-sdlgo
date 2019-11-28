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

/*
Package sdlgo provides a lightweight, high-speed interface for accessing shared data storage.

Shared Data Layer (SDL) is a concept where applications can use and share data using a common
storage. The storage must be optimised for very high tranactional throughput and very low
latency. Sdlgo is a library which provides applications an API to read and write data
to a common storage using key-value paradigm. In addition to this, sdlgo provides an
event mechanism that can be used to notify listeners that data was changed.

This SDL version assumes that the DBAAS service provided by O-RAN community is
working as a storage backend.

All functions except receiving of notifications are safe for concurrent usage by
multiple goroutines.

Namespace

A shared data layer connection is instantiated to a namespace and data is
always read and modified within the namespace. Namespace provides data isolation across clients.
Namespace instantiation happens when the sdlgo instance is created.
E.g. the following code sets the SDL instance to use "example" as a namespace
  sdl := sdlgo.NewSdlInstance("example", sdlgo.NewDatabase())
Applications can use several namespaces at the same time by creating several sdlgo instances.

Database connection

In addition to creating the SDL instance, application is responsible for creating the backend
database connection with NewDatabase() method. When the connection is created, sdlgo shall open a
tcp connection to backend database immediatelly. The resulting connection may be used with several
SDL instances if application needs to use several namespaces at a time. E.g.
  connection := sdlgo.NewDatabase()
  ns1 := sdlgo.NewSdlInstance("ns1", connection)
  ns2 := sdlgo.NewSdlInstance("ns2", connection)
For the backend database connection a circuit breaker design is used. If the connection fails,
an error is returned immediatelly to application. Restoration of the connection happens
automatically by SDL and application should retry the operation again after a while.

Database service is discovered by using environment variables DBAAS_SERVICE_HOST and
DBAAS_SERVICE_PORT. If not set, localhost and port 6379 are used by default.

Keys and data

Clients save key-value pairs. Keys are allways strings. The types of the values must be of a basic
type, i.e. string, integer or byte array or slice. This means that the internal structures, like
protobufs or JSON objects, must be serialised to a byte array or slice before passing it to SDL.
Clients are responsible for managing the keys within a namespace.

Some examples on how to set the data using different kind of input parameters:

Basic usage, keys and values are given as own parameters (with mixed data types)
  err := s.Set("key1", "value1", "key2", 2)

Keys and values inside a slice (again with mixed types, thus empty interface used as a type)
  exampleSlice := []interface{"key1", "value1", "key2", 2}
  err := s.Set(exampleSlice)

Data stored to a byte array
  data := make([]byte), 3
  data[0] = 1
  data[1] = 2
  data[2] = 3
  s.Set("key", data)

Keys and values stored into a map (byte array "data" used from previous example)
  mapData := map[string]interface{
    "key1" : "data",
    "key2" : 2,
    "key3" : data,
  }

When data is read from SDL storage, a map is returned where the requested key works as map key.
If the key was not found, the value for the given key is nil. It is possible to request several
key with one Get() call.

Groups

SDL groups are unordered collections of members where each member is unique. Using the SDL API
it is possible to add/remove members from a group, remove the whole group or do queries like the
size of a group and if member belongs to a group. Like key-value storage, groups are per namespace.

Events

Events are a publish-subscribe pattern to indicate interested parties that there has been a change
in data. Delivery of the events are not guaranteed. In SDL, events are happening via channels and
channels are per namespace. It is possible to publish several kinds of events through one channel.

In order to publish changes to SDL data, the publisher need to call an API function that supports
publishing. E.g.
   err := sdl.SetAndPublish([]string{"channel1", "event1", "channel2", "event2"}, "key", "value")
This example will publish event1 to channel1 and event2 in channel2 after writing the data.

When subscribing the channels, the application needs to first create an SDL instance for the desired
namespace. The subscription happens using the SubscribeChannel() API function. The parameters for
the function takes a callback function and one or many channels to be subscribed. When an event is
received for the given channel, the given callback function shall be called with one or many events.
It is possible to make several subscriptions for different channels using different callback
functions if different kind of handling is required.

  sdl := sdlgo.NewSdlInstance("namespace", sdlgo.NewDatabase())

  cb1 := func(channel string, event ...string) {
    fmt.Printf("cb1: Received %s from channel %s\n", event, channel)
  }

  cb2 := func(channel string, event ...string) {
    fmt.Printf("cb2: Received %s from channel %s\n", event, channel)
  }

  sdl.SubscribeChannel(cb1, "channel1", "channel2")
  sdl.SubscribeChannel(cb2, "channel3")

This example subscribes three channels from "namespace" and assigns cb1 for channel1 and channel2
whereas channel3 is assigned to cb2.

The callbacks are called from a context of a goroutine that is listening for the events. When
application receives events, the preferred way to do the required processing of an event (e.g. read
from SDL) is to do it in another context, e.g. by triggering applications own goroutine using Go
channels. By doing like this, blocking the receive routine and possibly loosing events, can be
avoided.
*/
package sdlgo
