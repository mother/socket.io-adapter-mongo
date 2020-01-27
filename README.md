# socket.io-adapter-mongo

[![Build Status](https://travis-ci.org/mother/socket.io-adapter-mongo.svg?branch=master)](https://travis-ci.org/mother/socket.io-adapter-mongo)

## Installing

```sh
npm install @mother/socket.io-adapter-mongo --save
```

## How to use

```js
const io = require('socket.io')(3000);
const mongoAdapter = require('@mother/socket.io-adapter-mongo');
io.adapter(mongoAdapter('mongodb://localhost/test'));
```

or pass an object:

```js
const io = require('socket.io')(3000);
const mongoAdapter = require('@mother/socket.io-adapter-mongo');

io.adapter(mongoAdapter({
	uri: 'mongodb://localhost/test',
	key: 'socket.io',
	mOptions: {
		tls: true
	}
}));
```

By running socket.io with the `socket.io-adapter-mongo` adapter you can run
multiple socket.io instances in different processes or servers that can
all broadcast and emit events to and from each other.

If you need to emit events to socket.io instances from a non-socket.io
process, you should use [socket.io-emitter](https://github.com/socketio/socket.io-emitter).

## API

### adapter(uri[, opts])

`uri` is a a mongodb:// uri string

If using this method of calling the adapter, the uri string will used instead of the uri property in the options object.

For a list of options see below.

### adapter(opts)

The following options are allowed:

- `uri`: mongodb:// uri string this property or the `mongoose` property are **required**.
- `key`: the name of the key to pub/sub events on as prefix (`socket.io`)
- `collectionName`: the name of the capped collection to be used (`socket.io-message-queue`)
- `collectionSize`: the size of the capped collection to be used, in bytes (`1000000`) (10mb)
- `mongoose`: an existing Mongoose instance that can be used instead of a mongodb:// uri
- `mOptions`: options to pass to Mongoose

## Notes

The following options are passed to Mongoose by default:

- `useNewUrlParser`: true
- `useUnifiedTopology`: true

If you want to override these options, just include them in `mOptions`.

## License

MIT
