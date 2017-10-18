const Adapter = require('socket.io-adapter')
const Mongoose = require('mongoose').Mongoose
const debug = require('debug')('socket.io-mongo-adapter')
const msgpack = require('notepack.io')
const uid2 = require('uid2')

/**
 * Module exports.
 */

module.exports = adapter

const collectionNames = []

/**
 * Returns a MongoAdapter class.
 *
 * @return {MongoAdapter} adapter
 * @api public
 */

function adapter(uriArg, optionsArg = {}) {
   const options = typeof uriArg === 'object'
      ? uriArg
      : optionsArg

   const uri = typeof uriArg === 'object'
      ? null
      : uriArg

   const prefix = options.key || 'socket.io'
   const requestsTimeout = options.requestsTimeout || 5000

   const collectionName = options.collectionName || 'socket.io-message-queue'
   const collectionSize = options.collectionSize || 1000000 // 1MB
   const mongoose = options.mongoose || new Mongoose().connect(uri)

   // this server's key
   const uid = uid2(6)

   // Message Schema & Model
   let Message
   if (collectionNames.includes(collectionName)) {
      Message = mongoose.model(collectionName)
   } else {
      const messageSchema = new mongoose.Schema({
         channel: { type: String, trim: true },
         msg: { type: Buffer }
      }, {
         capped: collectionSize
      })

      Message = mongoose.model(collectionName, messageSchema)
      collectionNames.push(collectionName)
   }

   /**
    * Adapter constructor.
    *
    * @param {String} namespace name
    * @api public
    */

   function MongoAdapter(nsp) {
      Adapter.call(this, nsp)

      this.uid = uid
      this.prefix = prefix
      // this.requestsTimeout = requestsTimeout
      this.channel = `${prefix}#${nsp.name}#`

      this.channelMatches = (messageChannel, subscribedChannel) => (
         messageChannel.startsWith(subscribedChannel)
      )

      Message.count({})
      .then((count) => {
         if (count === 0) {
            return Message.create({
               channel: 'placeholder',
               msg: new Buffer('placeholder')
            })
         }
      })
      .then(() => {
         const cursor = Message
         .find({})
         .tailable()
         .cursor()

         cursor.on('data', this.onmessage.bind(this))
         cursor.on('error', (err) => {
            this.emit('error', err)
            cursor.destroy()
         })
      })
      .catch(err => (
         this.emit('error', err)
      ))
   }

   /**
    * Inherits from `Adapter`.
    */

   // eslint-disable-next-line no-proto
   MongoAdapter.prototype.__proto__ = Adapter.prototype

   /**
    * Called with a subscription message
    *
    * @api private
    */

   MongoAdapter.prototype.onmessage = function (record) {
      const channel = record.channel

      if (!this.channelMatches(channel, this.channel)) {
         return debug('ignore different channel')
      }

      const room = channel.slice(this.channel.length, -1)
      if (room !== '' && !this.rooms.hasOwnProperty(room)) { // eslint-disable-line no-prototype-builtins
         return debug('ignore unknown room %s', room)
      }

      const msg = record.msg
      const args = msgpack.decode(msg)

      if (uid === args.shift()) {
         return debug('ignore same uid')
      }

      const packet = args[0]

      if (packet && packet.nsp === undefined) {
         packet.nsp = '/'
      }

      if (!packet || packet.nsp !== this.nsp.name) {
         return debug('ignore different namespace')
      }

      args.push(true)

      this.broadcast.call(this, ...args)
   }

   /**
    * Broadcasts a packet.
    *
    * @param {Object} packet to emit
    * @param {Object} options
    * @param {Boolean} whether the packet came from another node
    * @api public
    */

   MongoAdapter.prototype.broadcast = function (packet, opts, remote) {
      packet.nsp = this.nsp.name

      if (!(remote || (opts && opts.flags && opts.flags.local))) {
         const msg = msgpack.encode([uid, packet, opts])
         let channel = this.channel
         if (opts.rooms && opts.rooms.length === 1) {
            const room = opts.rooms[0]
            channel += `${room}#`
         }

         debug('publishing message to channel %s', channel)
         Message.create({ channel, msg }, (err) => {
            debug(err)
         })
      }

      Adapter.prototype.broadcast.call(this, packet, opts)
   }

   /**
    * Gets a list of clients by sid.
    *
    * @param {Array} explicit set of rooms to check.
    * @param {Function} callback
    * @api public
    */

   MongoAdapter.prototype.clients = function (rooms, fn) {

   }

   /**
    * Gets the list of rooms a given client has joined.
    *
    * @param {String} client id
    * @param {Function} callback
    * @api public
    */

   MongoAdapter.prototype.clientRooms = function (id, fn) {

   }

   /**
    * Gets the list of all rooms (accross every node)
    *
    * @param {Function} callback
    * @api public
    */

   MongoAdapter.prototype.allRooms = function (fn) {

   }

   /**
    * Makes the socket with the given id join the room
    *
    * @param {String} socket id
    * @param {String} room name
    * @param {Function} callback
    * @api public
    */

   MongoAdapter.prototype.remoteJoin = function (id, room, fn) {

   }

   /**
    * Makes the socket with the given id leave the room
    *
    * @param {String} socket id
    * @param {String} room name
    * @param {Function} callback
    * @api public
    */

   MongoAdapter.prototype.remoteLeave = function (id, room, fn) {

   }

   /**
    * Makes the socket with the given id to be disconnected forcefully
    * @param {String} socket id
    * @param {Boolean} close if `true`, closes the underlying connection
    * @param {Function} callback
    * @api public
    */

   MongoAdapter.prototype.remoteDisconnect = function (id, close, fn) {

   }

   /**
    * Sends a new custom request to other nodes
    *
    * @param {Object} data (no binary)
    * @param {Function} callback
    * @api public
    */

   MongoAdapter.prototype.customRequest = function (data, fn) {

   }

   MongoAdapter.mongoose = mongoose
   // MongoAdapter.uid = uid
   // MongoAdapter.prefix = prefix
   // MongoAdapter.requestsTimeout = requestsTimeout

   return MongoAdapter
}
