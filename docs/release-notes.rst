..
..  Copyright (c) 2019 AT&T Intellectual Property.
..  Copyright (c) 2019 Nokia.
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
