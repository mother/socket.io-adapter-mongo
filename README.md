# socket.io-adapter-mongo

[![Build Status](https://travis-ci.org/mother/socket.io-adapter-mongo.svg?branch=master)](https://travis-ci.org/mother/socket.io-adapter-mongo)

## How to use

```js
const io = require('socket.io')(3000);
const mongoAdapter = require('socket.io-adapter-mongo');
io.adapter(mongoAdapter('mongodb://localhost/test'));
```

By running socket.io with the `socket.io-adapter-mongo` adapter you can run
multiple socket.io instances in different processes or servers that can
all broadcast and emit events to and from each other.

If you need to emit events to socket.io instances from a non-socket.io
process, you should use [socket.io-emitter](https://github.com/socketio/socket.io-emitter).

## API

### adapter(uri[, opts])

`uri` is a a mongodb:// uri. For a list of options see below.

### adapter(opts)

The following options are allowed:

- `key`: the name of the key to pub/sub events on as prefix (`socket.io`)
- `collectionName`: the name of the capped collection to be used (`socket.io-message-queue`)
- `collectionSize`: the size of the capped collection to be used, in bytes (`1000000`) (10mb)
- `mongoose`: an existing mongoose instance that can be used instead of a mongodb:// uri

## License

MIT
