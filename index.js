const Adapter = require('socket.io-adapter')
const Mongoose = require('mongoose').Mongoose
const debug = require('debug')('socket.io-mongo-adapter')
const msgpack = require('notepack.io')
const uid2 = require('uid2')

// TEMP HACK TO REPLACE
// pub.send_command('pubsub', ['numsub', self.requestChannel]
const cursorIds = []

// Request types, for messages between nodes
const requestTypes = {
   clients: 0,
   clientRooms: 1,
   allRooms: 2,
   remoteJoin: 3,
   remoteLeave: 4,
   customRequest: 5,
   remoteDisconnect: 6
}

class MongoAdapter extends Adapter {
   constructor(options = {}, namespace) {
      super(namespace)

      this.uid = uid2(6)
      this.prefix = options.prefix || 'socket.io'
      this.channel = `${this.prefix}#${namespace.name}#`
      this.requestChannel = `${this.prefix}-request#${namespace.name}#`
      this.responseChannel = `${this.prefix}-response#${namespace.name}#`
      this.model = options.model
      this.db = options.db
      this.requests = {}
      this.requestsTimeout = options.requestsTimeout
      this.customHook = (data, cb) => cb(null)

      this.model.find({}).sort('-_id').limit(1)
         .then((docs) => {
            if (docs.length) {
               return docs[0]
            }

            return new Promise((resolve, reject) => {
               this.model.collection.insert({
                  channel: 'placeholder',
                  msg: Buffer.from('placeholder')
               }, {
                  safe: true
               }, function(err, docs) {
                  resolve(docs.ops[0])
               })
            })
         })
         .then((latestDoc) => {
            this.cursor = this.model.collection.find({
               _id: {
                  $gt: latestDoc._id
               }
            }, {
               tailable: true,
               awaitData: true,
               noCursorTimeout: true,
               numberOfRetries: -1
            })

            cursorIds.push(this.uid)

            this.cursor.on('data', (record) => {
               if (this.channelMatches(record.channel, this.channel)) {
                  this.onmessage(record)
               } else if (this.channelMatches(record.channel, this.requestChannel)) {
                  this.onrequest(record)
               } else if (this.channelMatches(record.channel, this.responseChannel)) {
                  this.onresponse(record)
               } else {
                  debug('ignoring unknown channel', record.channel)
               }
            })

            this.cursor.on('error', (err) => {
               this.emit('error', err)
               this.cursor.destroy()
            })

            this.cursor.on('end', () => {
               const index = cursorIds.indexOf(this.uid)
               if (index !== -1) {
                  cursorIds.splice(index, 1)
               }
            })
         })
         .catch((err) => {
            this.emit('error', err)
         })
   }

   disconnect(callback) {
      this.cursor.close((err) => {
         if (err) return callback(err)
         this.model.db.close((connectionErr) => {
            if (typeof callback === 'function') {
               callback(connectionErr)
            }
         })
      })
   }

   channelMatches(messageChannel, subscribedChannel) {
      return messageChannel.startsWith(subscribedChannel)
   }

   /**
    * Called with a subscription message
    *
    * @param {Object} record
    * @api private
    */

   onmessage(record) {
      const channel = record.channel
      if (!this.channelMatches(channel, this.channel)) {
         return debug('ignore different channel')
      }

      const room = channel.slice(this.channel.length, -1)
      if (room !== '' && !this.rooms.hasOwnProperty(room)) { // eslint-disable-line no-prototype-builtins
         return debug('ignore unknown room %s', room)
      }

      const msg = record.msg
      const args = msgpack.decode(msg.buffer)

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
    * Called on request from another node
    *
    * @param {Object} record
    * @api private
    */

   onrequest(record) {
      const channel = record.channel
      const msg = record.msg

      let request = null
      try {
         request = JSON.parse(msg)
      } catch (err) {
         this.emit('error', err)
         return
      }

      debug('received request %j', request, 'on', this.uid)

      switch (request.type) {
         case requestTypes.clients: {
            super.clients(request.rooms, (err, clients) => {
               if (err) {
                  this.emit('error', err)
                  return
               }

               const response = JSON.stringify({
                  requestid: request.requestid,
                  clients: clients
               })

               this.model.create({
                  channel: this.responseChannel,
                  msg: Buffer.from(response)
               }, (err) => {
                  if (err) {
                     this.emit('error', err)
                  }
               })
            })

            break
         }

         case requestTypes.clientRooms: {
            super.clientRooms(request.sid, (err, rooms) => {
               if (err) {
                  this.emit('error', err)
                  return
               }

               if (!rooms) {
                  return
               }

               const response = JSON.stringify({
                  requestid: request.requestid,
                  rooms: rooms
               })

               this.model.create({
                  channel: this.responseChannel,
                  msg: Buffer.from(response)
               }, (err) => {
                  if (err) {
                     this.emit('error', err)
                  }
               })
            })

            break
         }

         case requestTypes.allRooms: {
            const response = JSON.stringify({
               requestid: request.requestid,
               rooms: Object.keys(this.rooms)
            })

            this.model.create({
               channel: this.responseChannel,
               msg: Buffer.from(response)
            }, (err) => {
               if (err) {
                  this.emit('error', err)
               }
            })

            break
         }

         case requestTypes.remoteJoin: {
            const socket = this.nsp.connected[request.sid]
            if (!socket) {
               return
            }

            socket.join(request.room, () => {
               const response = JSON.stringify({
                  requestid: request.requestid
               })

               this.model.create({
                  channel: this.responseChannel,
                  msg: Buffer.from(response)
               }, (err) => {
                  if (err) {
                     this.emit('error', err)
                  }
               })
            })

            break
         }

         case requestTypes.remoteLeave: {
            const socket = this.nsp.connected[request.sid]
            if (!socket) {
               return
            }

            socket.leave(request.room, () => {
               const response = JSON.stringify({
                  requestid: request.requestid
               })

               this.model.create({
                  channel: this.responseChannel,
                  msg: Buffer.from(response)
               }, (err) => {
                  if (err) {
                     this.emit('error', err)
                  }
               })
            })

            break
         }

         case requestTypes.remoteDisconnect: {
            const socket = this.nsp.connected[request.sid]
            if (!socket) {
               return
            }

            socket.disconnect(request.close)

            const response = JSON.stringify({
               requestid: request.requestid
            })

            this.model.create({
               channel: this.responseChannel,
               msg: Buffer.from(response)
            }, (err) => {
               if (err) {
                  this.emit('error', err)
               }
            })

            break
         }

         case requestTypes.customRequest: {
            this.customHook(request.data, (data) => {
               const response = JSON.stringify({
                  requestid: request.requestid,
                  data: data
               })

               this.model.create({
                  channel: this.responseChannel,
                  msg: Buffer.from(response)
               }, (err) => {
                  if (err) {
                     this.emit('error', err)
                  }
               })
            })

            break
         }

         default: {
            debug('ignoring unknown request type: %s', request.type)
         }
      }
   }

   /**
    * Called on response from another node
    *
    * @param {Object} record
    * @api private
    */

   onresponse(record) {
      const channel = record.channel
      const msg = record.msg

      let response = null
      try {
         response = JSON.parse(msg)
      } catch (err) {
         this.emit('error', err)
         return
      }

      const requestid = response.requestid
      if (!requestid || !this.requests[requestid]) {
         debug('ignoring response with unknown request %j', response)
         return
      }

      debug('received response %j', response)

      const request = this.requests[requestid]

      switch (request.type) {
         case requestTypes.clients: {
            request.msgCount++

            // ignore if response does not contain 'clients' key
            if (!response.clients || !Array.isArray(response.clients)) return

            for (let i = 0; i < response.clients.length; i++) {
               request.clients[response.clients[i]] = true
            }

            if (request.msgCount === cursorIds.length /* request.numsub */ ) {
               clearTimeout(request.timeout)
               if (request.callback) process.nextTick(request.callback.bind(null, null, Object.keys(request.clients)))
               delete this.requests[requestid]
            }

            break
         }

         case requestTypes.clientRooms: {
            clearTimeout(request.timeout)
            if (request.callback) process.nextTick(request.callback.bind(null, null, response.rooms))
            delete this.requests[requestid]
            break
         }

         case requestTypes.allRooms: {
            request.msgCount++

            // ignore if response does not contain 'rooms' key
            if (!response.rooms || !Array.isArray(response.rooms)) return

            for (let i = 0; i < response.rooms.length; i++) {
               request.rooms[response.rooms[i]] = true
            }

            if (request.msgCount === cursorIds.length /* request.numsub */ ) {
               clearTimeout(request.timeout)
               if (request.callback) process.nextTick(request.callback.bind(null, null, Object.keys(request.rooms)))
               delete this.requests[requestid]
            }

            break
         }

         case requestTypes.remoteJoin:
         case requestTypes.remoteLeave:
         case requestTypes.remoteDisconnect:
            clearTimeout(request.timeout)
            if (request.callback) process.nextTick(request.callback.bind(null, null))
            delete this.requests[requestid]
            break

         case requestTypes.customRequest: {
            request.msgCount++
            request.replies.push(response.data)

            if (request.msgCount === cursorIds.length /* request.numsub */ ) {
               clearTimeout(request.timeout)
               if (request.callback) process.nextTick(request.callback.bind(null, null, request.replies))
               delete self.requests[requestid]
            }

            break
         }

         default: {
            debug('ignoring unknown request type: %s', request.type)
         }
      }
   }

   /**
    * Broadcasts a packet.
    *
    * @param {Object} packet to emit
    * @param {Object} options
    * @param {Boolean} whether the packet came from another node
    * @api public
    */

   broadcast(packet, opts, remote) {
      packet.nsp = this.nsp.name
      if (!(remote || (opts && opts.flags && opts.flags.local))) {
         const msg = msgpack.encode([this.uid, packet, opts])
         let channel = this.channel
         if (opts.rooms && opts.rooms.length === 1) {
            const room = opts.rooms[0]
            channel += `${room}#`
         }

         debug('publishing message to channel %s', channel)
         this.model.create({
            channel,
            msg
         }, (err) => {
            if (err) {
               this.emit('error', err)
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

   clients(rooms, fn) {
      if (typeof rooms === 'function') {
         fn = rooms
         rooms = null
      }

      rooms = rooms || []

      const self = this
      const requestid = uid2(6)

      let numsub = cursorIds.length
      // if (err) {
      //    this.emit('error', err)
      //    if (fn) fn(err)
      //    return
      // }

      debug('waiting for %d responses to "clients" request', numsub)

      const request = JSON.stringify({
         requestid: requestid,
         type: requestTypes.clients,
         rooms: rooms
      })

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
         const request = self.requests[requestid]
         if (fn) process.nextTick(fn.bind(null, new Error('timeout reached while waiting for clients response'), Object.keys(request.clients)))
         delete self.requests[requestid]
      }, self.requestsTimeout)

      self.requests[requestid] = {
         type: requestTypes.clients,
         numsub: numsub,
         msgCount: 0,
         clients: {},
         callback: fn,
         timeout: timeout
      }

      this.model.create({
         channel: this.requestChannel,
         msg: Buffer.from(request)
      }, (err) => {
         if (err) {
            this.emit('error', err)
         }
      })
   }

   /**
    * Gets the list of rooms a given client has joined.
    *
    * @param {String} client id
    * @param {Function} callback
    * @api public
    */

   clientRooms(id, fn) {
      const self = this
      const requestid = uid2(6)
      const rooms = this.sids[id]

      if (rooms) {
         if (fn) process.nextTick(fn.bind(null, null, Object.keys(rooms)))
         return
      }

      const request = JSON.stringify({
         requestid: requestid,
         type: requestTypes.clientRooms,
         sid: id
      })

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
         if (fn) process.nextTick(fn.bind(null, new Error('timeout reached while waiting for rooms response')))
         delete self.requests[requestid]
      }, self.requestsTimeout)

      this.requests[requestid] = {
         type: requestTypes.clientRooms,
         callback: fn,
         timeout: timeout
      }

      this.model.create({
         channel: this.requestChannel,
         msg: Buffer.from(request)
      }, (err) => {
         if (err) {
            this.emit('error', err)
         }
      })
   }

   /**
    * Gets the list of all rooms (accross every node)
    *
    * @param {Function} callback
    * @api public
    */

   allRooms(fn) {
      const self = this
      const requestid = uid2(6)

      let numsub = cursorIds.length
      // pub.send_command('pubsub', ['numsub', self.requestChannel], function(err, numsub) {
      //    if (err) {
      //       self.emit('error', err)
      //       if (fn) fn(err)
      //       return
      //    }

      debug('waiting for %d responses to "allRooms" request', numsub)

      const request = JSON.stringify({
         requestid: requestid,
         type: requestTypes.allRooms
      })

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
         const request = self.requests[requestid]
         if (fn) process.nextTick(fn.bind(null, new Error('timeout reached while waiting for allRooms response'), Object.keys(request.rooms)))
         delete self.requests[requestid]
      }, self.requestsTimeout)

      self.requests[requestid] = {
         type: requestTypes.allRooms,
         numsub: numsub,
         msgCount: 0,
         rooms: {},
         callback: fn,
         timeout: timeout
      }

      this.model.create({
         channel: this.requestChannel,
         msg: Buffer.from(request)
      }, (err) => {
         if (err) {
            this.emit('error', err)
         }
      })
   }

   /**
    * Makes the socket with the given id join the room
    *
    * @param {String} socket id
    * @param {String} room name
    * @param {Function} callback
    * @api public
    */

   remoteJoin(id, room, fn) {
      const self = this
      const requestid = uid2(6)

      const socket = this.nsp.connected[id]
      if (socket) {
         socket.join(room, fn)
         return
      }

      const request = JSON.stringify({
         requestid: requestid,
         type: requestTypes.remoteJoin,
         sid: id,
         room: room
      })

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
         if (fn) process.nextTick(fn.bind(null, new Error('timeout reached while waiting for remoteJoin response')))
         delete self.requests[requestid]
      }, self.requestsTimeout)

      self.requests[requestid] = {
         type: requestTypes.remoteJoin,
         callback: fn,
         timeout: timeout
      }

      this.model.create({
         channel: this.requestChannel,
         msg: Buffer.from(request)
      }, (err) => {
         if (err) {
            this.emit('error', err)
         }
      })
   }

   /**
    * Makes the socket with the given id leave the room
    *
    * @param {String} socket id
    * @param {String} room name
    * @param {Function} callback
    * @api public
    */

   remoteLeave(id, room, fn) {
      const self = this
      const requestid = uid2(6)
      const socket = this.nsp.connected[id]
      if (socket) {
         socket.leave(room, fn)
         return
      }

      const request = JSON.stringify({
         requestid: requestid,
         type: requestTypes.remoteLeave,
         sid: id,
         room: room
      })

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
         if (fn) process.nextTick(fn.bind(null, new Error('timeout reached while waiting for remoteLeave response')))
         delete self.requests[requestid]
      }, self.requestsTimeout)

      self.requests[requestid] = {
         type: requestTypes.remoteLeave,
         callback: fn,
         timeout: timeout
      }

      this.model.create({
         channel: this.requestChannel,
         msg: Buffer.from(request)
      }, (err) => {
         if (err) {
            this.emit('error', err)
         }
      })
   }

   /**
    * Makes the socket with the given id to be disconnected forcefully
    * @param {String} socket id
    * @param {Boolean} close if `true`, closes the underlying connection
    * @param {Function} callback
    * @api public
    */

   remoteDisconnect(id, close, fn) {
      const self = this
      const requestid = uid2(6)

      const socket = this.nsp.connected[id]
      if (socket) {
         socket.disconnect(close)
         if (fn) process.nextTick(fn.bind(null, null))
         return
      }

      const request = JSON.stringify({
         requestid: requestid,
         type: requestTypes.remoteDisconnect,
         sid: id,
         close: close
      })

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
         if (fn) process.nextTick(fn.bind(null, new Error('timeout reached while waiting for remoteDisconnect response')))
         delete self.requests[requestid]
      }, self.requestsTimeout)

      this.requests[requestid] = {
         type: requestTypes.remoteDisconnect,
         callback: fn,
         timeout: timeout
      }

      this.model.create({
         channel: this.requestChannel,
         msg: Buffer.from(request)
      }, (err) => {
         if (err) {
            this.emit('error', err)
         }
      })
   }

   /**
    * Sends a new custom request to other nodes
    *
    * @param {Object} data (no binary)
    * @param {Function} callback
    * @api public
    */

   customRequest(data, fn) {
      if (typeof data === 'function') {
         fn = data
         data = null
      }

      const self = this
      const requestid = uid2(6)

      const numsub = cursorIds.length
      // pub.send_command('pubsub', ['numsub', self.requestChannel], function(err, numsub) {
      //    if (err) {
      //       self.emit('error', err)
      //       if (fn) fn(err)
      //       return
      //    }

      debug('waiting for %d responses to "customRequest" request', numsub)

      const request = JSON.stringify({
         requestid: requestid,
         type: requestTypes.customRequest,
         data: data
      })

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
         const request = self.requests[requestid]
         if (fn) process.nextTick(fn.bind(null, new Error('timeout reached while waiting for customRequest response'), request.replies))
         delete self.requests[requestid]
      }, self.requestsTimeout)

      self.requests[requestid] = {
         type: requestTypes.customRequest,
         numsub: numsub,
         msgCount: 0,
         replies: [],
         callback: fn,
         timeout: timeout
      }

      this.model.create({
         channel: this.requestChannel,
         msg: Buffer.from(request)
      }, (err) => {
         if (err) {
            this.emit('error', err)
         }
      })
   }
}

/**
 * Returns a MongoAdapter class.
 *
 * @return {MongoAdapter} adapter
 * @api public
 */

module.exports = function adapter(uriArg, optionsArg = {}) {
   const options = typeof uriArg === 'object' ?
      uriArg :
      optionsArg

   const uri = typeof uriArg === 'object' ?
      null :
      uriArg

   const prefix = options.key || 'socket.io'
   const requestsTimeout = options.requestsTimeout || 5000

   const collectionName = options.collectionName || 'socket.io-message-queue'
   const collectionSize = options.collectionSize || 100000 // 100KB
   const mongoose = options.mongoose || new Mongoose()
   if (!options.mongoose && uri) {
      mongoose.connect(uri)
   }

   // mongoose.set('debug', true)
   // Message Schema & Model
   let Message
   if (mongoose.modelNames().includes(collectionName)) {
      Message = mongoose.model(collectionName)
   } else {
      const messageSchema = new mongoose.Schema({
         channel: { type: String, trim: true },
         msg: { type: Buffer }
      }, { capped: collectionSize })

      Message = mongoose.model(collectionName, messageSchema)
   }

   return MongoAdapter.bind(null, {
      model: Message,
      prefix,
      requestsTimeout,
      db: mongoose.connection.db
   })
}
