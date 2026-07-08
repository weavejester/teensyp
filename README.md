# TeensyP [![Build Status](https://github.com/weavejester/teensyp/actions/workflows/test.yml/badge.svg)](https://github.com/weavejester/teensyp/actions/workflows/test.yml)

A small, zero-dependency Clojure TCP server that uses Java NIO.

## Installation

Add the following dependency to your deps.edn file:

    dev.weavejester/teensyp {:mvn/version "0.7.3"}

Or to your Leiningen project file:

    [dev.weavejester/teensyp "0.7.3"]

## Usage

At minimum, TeensyP requires a port to listen on and a handler function:

```clojure
(require '[teensyp.server :as tcp])

(tcp/start-server {:port 3000, :handler demo-handler})
```

This returns a Closeable server instance.

### Handlers

The handler function has three arities, and defines how the server
behaves.

```clojure
(defn example-handler
  ([socket]
   ;; The 1 argument arity is called when the socket is accepted.
   ;; The return value is the session state.
   {:read-bytes 0})
  ([state socket ^java.nio.ByteBuffer buffer]
   ;; The 3 argument arity is called when the socket receives data from
   ;; the client, contained in a ByteBuffer. The return value is the
   ;; updated session state.
   (let [remaining (.remaining buffer)]
     (.position buffer (.limit buffer))  ; fake a read
     (update state :read-bytes + remaining)))
  ([state exception]
   ;; The 2 argument arity is called when the socket is closed. If it
   ;; closed due to an exception, that exception is passed as the
   ;; second argument, otherwise it will be nil.
   (println "Total bytes:" (:read-bytes state))))
```

The handler maintains a session `state`, which can be any Clojure data
structure, but is usually a map. This state is unique to the socket,
and is updated each time the socket is read from by the 3-arity form of
the handler.

Uncaught exceptions will close the channel and trigger the 2-arity form
of the handler.

### Sockets

The `socket` argument satisfies the `teensyp.server/Socket` protocol,
and represents the current connection between server and client. The
following functions are supported:

- `teensyp.server/write` - writes a buffer to the channel
- `teensyp.server/close` - closes the channel
- `teensyp.server/pause-reads`  - pause reads until resumed
- `teensyp.server/resume-reads` - resume reads

All of these functions are asynchronous, and can be supplied with an
optional zero-argument callback function that will be called once they
successfully complete.

I/O errors will close the socket and trigger the 2-arity form of the
handler.

### Guarantees

TeensyP makes several guarantees that apply per socket:

- The 1-arity accept is always called first.
- The 2-arity close is always called last.
- The current handler call for the socket must finish before the next
  can begin.
- The write queue must be empty before the read arity is called.

### Server Options

The `start-server` function takes a number of options:

| Key                    | Description                                         | Mandatory | Default |
|------------------------|-----------------------------------------------------|-----------|---------|
| `:control-queue-size`  | The max number of queued control events             |           | 32      |
| `:direct-read-buffer?` | Allocate a direct ByteBuffer for reads              |           | false   |
| `:executor`            | An `ExecutorService` to use for handler calls       |           |         |
| `:handler`             | The handler function                                | Yes       |         |
| `:port`                | The port number to listen on                        | Yes       |         |
| `:read-buffer-size`    | The size in bytes of the channel read buffer        |           | 8K      |
| `:recv-buffer-size`    | The receive buffer size (i.e. the SO_RCVBUF option) |           |         |
| `:reuse-address?`      | The SO_REUSEADDR socket option                      |           | false   |
| `:write-buffer-size`   | The size in bytes of the channel write buffer       |           | 32K     |
| `:write-queue-size`    | The maximum number of writes that can be queued     |           | 64      |

## Documentation

- [API Documentation](https://weavejester.github.io/teensyp)

## License

Copyright © 2026 James Reeves

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
