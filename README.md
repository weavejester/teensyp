# TeensyP [![Build Status](https://github.com/weavejester/teensyp/actions/workflows/test.yml/badge.svg)](https://github.com/weavejester/teensyp/actions/workflows/test.yml)

A small, zero-dependency Clojure TCP server that uses Java NIO.

## Installation

Add the following dependency to your deps.edn file:

    dev.weavejester/teensyp {:mvn/version "0.2.3"}

Or to your Leiningen project file:

    [dev.weavejester/teensyp "0.2.3"]

## Usage

At minimum, TeensyP requires a port to listen on and a handler function:

```clojure
(require '[teensyp.server :as tcp])

(tcp/start-server {:port 3000, :handler demo-handler})
```

The handler function has three arities, and defines how the server
behaves.

```clojure
(import 'java.nio.ByteBuffer)

(defn demo-handler
  ([write]
   ;; With 1 argument, we can write ByteBuffers via the write function.
   ;; The return value is the channel's state.
   {:read-bytes 0})
  ([state ^ByteBuffer buffer write]
   ;; With 3 arguments, we handle read data stored in a ByteBuffer. We
   ;; can update the channel's state by returning a new value, and write
   ;; to the output channel with the write function.
   (update state :read-bytes + (.remaining buffer)))
  ([state exception]
   ;; With 2 arguments, we have the channel closing. If it closed due to an
   ;; exception, that exception is passed as the second argument, otherwise it
   ;; will be nil.
  ))
```

The handler maintains a `state`, which can be any Clojure data
structure, but is usually a map. This state is unique to the channel,
and is updated each time the channel is read from by the 3-arity form of
the handler.

The handler is guaranteed to be called sequentially for the same
channel. That is, the 1-arity accept is always first, the 2-arity close
is always last, and each handler call for the same channel must finish
before the next begins. The write queue is also guaranteed to be empty
when the handler is called.

The `write` function accepts a single `ByteBuffer` argument, or one of
three special marker objects:

- `teensyp.server/CLOSE`        - closes the channel
- `teensyp.server/PAUSE-READS`  - pause reads until resumed
- `teensyp.server/RESUME-READS` - resume reads

You may also specify a second argument to `write`. This is a zero
argument callback function that will be called when the write completes.

## Options

The `start-server` function takes a number of options:

| Key                  | Description                                     | Mandatory | Default |
|----------------------|-------------------------------------------------|-----------|---------|
| `:executor`          | An `ExecutorService` to use for handler calls   |           |         |
| `:handler`           | The handler function                            | Yes       |         |
| `:port`              | The port number to listen on                    | Yes       |         |
| `:read-buffer-size`  | The size in bytes of the channel read buffer    |           | 8K      |
| `:write-buffer-size` | The size in bytes of the channel write buffer   |           | 32K     |
| `:write-queue-size`  | The maximum number of writes that can be queued |           | 64      |

## Documentation

- [API Documentation](https://weavejester.github.io/teensyp)

## License

Copyright © 2025 James Reeves

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
