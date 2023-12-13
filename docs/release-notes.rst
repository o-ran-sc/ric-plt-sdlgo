..
..  Copyright (c) 2019 AT&T Intellectual Property.
..  Copyright (c) 2019-2022 Nokia.
..
..  Licensed under the Creative Commons Attribution 4.0 International
..  Public License (the "License"); you may not use this file except
..  in compliance with the License. You may obtain a copy of the License at
..
..    https://creativecommons.org/licenses/by/4.0/
..
..  Unless required by applicable law or agreed to in writing, documentation
..  distributed under the License is distributed on an "AS IS" BASIS,
..  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
..
..  See the License for the specific language governing permissions and
..  limitations under the License.
..

Release-Notes
=============

This document provides the release notes of the sdlgo.

.. contents::
   :depth: 3
   :local:



Version history
---------------

[0.10.4] - 2023-12-13

* go version update

[0.10.3] - 2023-06-08

* go version update

[0.10.2] - 2022-09-30

* Fix Coverity issues about missing function call return value validations

[0.10.1] - 2022-08-12

* Upgrade Golang version to 1.18 in ci Dockerfile

[0.10.0] - 2022-03-14

* Enable redis/sentinel port and sentinel master name configuration

[0.9.6] - 2022-02-07

* Fix multi-namespace SDL event subscribe

[0.9.5] - 2022-01-20

* Pump Redis client version to v8.11.4 and fix Redis APIs to have a Golang
  Context type of argument.

[0.9.4] - 2022-01-12

* SDL CLI command to generate sdlcli shell completion file for bash

[0.9.3] - 2021-12-30

* Fix SDL CLI get -command to write results stdout stream when command success

[0.9.2] - 2021-12-22

* Fix SDL CLI healthcheck to ignore ghost Redis Sentinel entries

[0.9.1] - 2021-12-20

* Refactor and reduce code complexity, no API changes
* Validate namespace argument in SDL CLI get keys -command

[0.9.0] - 2021-12-16

* SDL CLI tool

[0.8.0] - 2021-09-22

* New ListKeys API in syncstorage
* Add deprecation warnings for SdlInstance APIs

[0.7.0] - 2021-05-11

* Add DB backend instance selection based on namespace value to balance DB load.

[0.6.0] - 2021-05-11

* Add SDL multi-namespace API 'SyncStorage'.

[0.5.5] - 2021-03-09

* Take DBAAS multi-channel publishing Redis modules into use.
* Fix potential type conversion crash in RemoveIf() and
  RemoveIfAndPublish() APIs.

[0.5.4] - 2020-10-07

* Fix Go routine race condition when new DB notifications are subscribed.

[0.5.3] - 2020-08-17

* Take Redis client version 6.15.9 into use.

[0.5.1] - 2019-11-1

* Add CI Dockerfile to run unit tests.

[0.5.0] - 2019-10-11

* Make underlying DB instance visible to clients.

[0.4.0] - 2019-09-26

* Add support for Redis sentinel configuration.

[0.3.1] - 2019-09-11

* Set MaxRetries count to 2 for Redis client.

[0.3.0] - 2019-08-14

* Add support for resource locking to enable applications to create locks for
  shared resources.

[0.2.1] - 2019-07-03

* Add support for multiple event publishing.

[0.2.0] - 2019-05-24

* Add support for SDL groups.

[0.1.1] - 2019-05-17

* Allow byte array/slice be given as value.

[0.1.0] - 2019-05-17

* Add support for notifications.

[0.0.2] - 2019-05-03

* Fix Close API call. Calling Close function from API caused a recursive call
  to itself.

[0.0.1] - 2019-04-17

* Initial version.
* Implement basic storage functions to create, read, update, and delete
  entries.
* Implement benchmark tests.
