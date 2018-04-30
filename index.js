const Adapter = require('socket.io-adapter')
const Mongoose = require('mongoose').Mongoose
const debug = require('debug')('socket.io-mongo-adapter')
const msgpack = require('notepack.io')
const uid2 = require('uid2')

const collectionNames = []

class MongoAdapter extends Adapter {
   constructor(options = {}, namespace) {
      super(namespace)

      this.uid = uid2(6)
      this.prefix = options.prefix || 'socket.io'
      this.channel = `${this.prefix}#${namespace.name}#`
      this.model = options.model
      // this.requestsTimeout = requestsTimeout

      this.model.count({})
      .then((count) => {
         if (count === 0) {
            return this.model.create({
               channel: 'placeholder',
               msg: new Buffer('placeholder')
            })
         }
      })
      .then(() => {
         return this.model.find({}).lean().select('_id').sort({ $natural: -1 }).limit(1)
         .then(latestDoc => Promise.resolve(latestDoc[0]._id))
      })
      .then((latestId) => {
         const cursor = this.model
         .find({ _id: { $gt: latestId } })
         .limit(1)
         .tailable({ awaitdata: true, numberOfRetries: -1 })
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

   channelMatches (messageChannel, subscribedChannel) {
      return messageChannel.startsWith(subscribedChannel)
   }

   // Private
   onmessage (record) {
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

      if (this.uid === args.shift()) {
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

   broadcast (packet, opts, remote) {
      packet.nsp = this.nsp.name

      if (!(remote || (opts && opts.flags && opts.flags.local))) {
         const msg = msgpack.encode([this.uid, packet, opts])
         let channel = this.channel
         if (opts.rooms && opts.rooms.length === 1) {
            const room = opts.rooms[0]
            channel += `${room}#`
         }

         debug('publishing message to channel %s', channel)
         this.model.create({ channel, msg }, (err) => {
            if (err) {
               debug('Error creating message', err)
            }
         })
      }

      super.broadcast(packet, opts)
   }

   /**
    * Gets a list of clients by sid.
    *
    * @param {Array} explicit set of rooms to check.
    * @param {Function} callback
    * @api public
    */

   clients (rooms, fn) {

   }

   /**
    * Gets the list of rooms a given client has joined.
    *
    * @param {String} client id
    * @param {Function} callback
    * @api public
    */

   clientRooms (id, fn) {

   }

   /**
    * Gets the list of all rooms (accross every node)
    *
    * @param {Function} callback
    * @api public
    */

   allRooms (fn) {

   }

   /**
    * Makes the socket with the given id join the room
    *
    * @param {String} socket id
    * @param {String} room name
    * @param {Function} callback
    * @api public
    */

   remoteJoin (id, room, fn) {

   }

   /**
    * Makes the socket with the given id leave the room
    *
    * @param {String} socket id
    * @param {String} room name
    * @param {Function} callback
    * @api public
    */

   remoteLeave (id, room, fn) {

   }

   /**
    * Makes the socket with the given id to be disconnected forcefully
    * @param {String} socket id
    * @param {Boolean} close if `true`, closes the underlying connection
    * @param {Function} callback
    * @api public
    */

   remoteDisconnect (id, close, fn) {

   }

   /**
    * Sends a new custom request to other nodes
    *
    * @param {Object} data (no binary)
    * @param {Function} callback
    * @api public
    */

   customRequest (data, fn) {

   }
}

/**
 * Returns a MongoAdapter class.
 *
 * @return {MongoAdapter} adapter
 * @api public
 */
module.exports = function adapter(uriArg, optionsArg = {}) {
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

   return MongoAdapter.bind(null, { model: Message, prefix })
}
