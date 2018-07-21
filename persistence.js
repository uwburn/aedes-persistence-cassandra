'use strict'

var debug = require('debug')('aedes-persistence-cassandra')

var util = require('util')
var CachedPersistence = require('aedes-cached-persistence')
var Packet = CachedPersistence.Packet
var cassandra = require('cassandra-driver')
var pump = require('pump')
var through = require('through2')
var Qlobber = require('qlobber').Qlobber
var uuidv4 = require('uuid/v4')
var filter = require('stream-filter')
var throughv = require('throughv')
var msgpack = require('msgpack-lite')
var qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#'
}

function CassandraPersistence (opts) {
  if (!(this instanceof CassandraPersistence)) {
    return new CassandraPersistence(opts)
  }

  opts = opts || {}

  this._opts = opts
  this._client = null

  this._getRetainedChunkBound = this._getRetainedChunk.bind(this)
  CachedPersistence.call(this, opts)
}

util.inherits(CassandraPersistence, CachedPersistence)

CassandraPersistence.prototype._connect = function (cb) {
  if (this._opts.db) {
    cb(null, this._opts.db)
    return
  }

  var conn = this._opts.cassandra || {
    contactPoints: ['127.0.0.1'],
    keyspace: 'aedes'
  }

  cb(null, new cassandra.Client(conn))
}

CassandraPersistence.prototype._setup = function () {
  if (this.ready) {
    return
  }

  var that = this

  this._connect(function (err, client) {
    if (err) {
      this.emit('error', err)
      return
    }

    that._client = client

    debug('Retrieving stored subscriptions with qos > 0')
    client.stream('SELECT * FROM subscription_topic WHERE qos > 0 ALLOW FILTERING', [], { prepare: true }).on('data', function (row) {
      that._trie.add(row.topic, {
        clientId: row.client_id,
        topic: row.topic,
        qos: row.qos
      })
    }).on('end', function () {
      that.emit('ready')
    }).on('error', function (err) {
      that.emit('error', err)
    })
  })
}

CassandraPersistence.prototype.storeRetained = function (packet, cb) {
  if (!this.ready) {
    this.once('ready', this.storeRetained.bind(this, packet, cb))
    return
  }

  if (packet.payload.length > 0) {
    debug('Storing retained packet with topic ' + packet.topic)
    this._client.execute('INSERT INTO retained (topic, packet) VALUES (?, ?)', [packet.topic, msgpack.encode(packet)], { prepare: true }, cb)
  } else {
    debug('Removing retained packet')
    this._client.execute('DELETE FROM retained WHERE topic = ?', [packet.topic], { prepare: true }, cb)
  }
}

CassandraPersistence.prototype._getRetainedChunk = function (topic, enc, cb) {
  debug('Retrieving retained packets with topic ' + topic)
  this._client.execute('SELECT * FROM retained WHERE topic = ?', [topic], { prepare: true }, cb)
}

CassandraPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  var that = this
  var qlobber = new Qlobber(qlobberOpts)

  for (var i = 0; i < patterns.length; i++) {
    qlobber.add(patterns[i])
  }

  var stream = through.obj(that._getRetainedChunkBound)

  debug('Retrieving retained packets')
  this._client.execute('SELECT * FROM retained', [], { prepare: true }, function (err, result) {
    if (err) {
      stream.emit('error', err)
    } else {
      matchRetained(stream, result.rows, qlobber)
    }
  })

  return pump(stream, throughv.obj(decodeRetainedPacket))
}

CassandraPersistence.prototype.createRetainedStream = function (pattern) {
  return this.createRetainedStreamCombi([pattern])
}

function matchRetained (stream, retaineds, qlobber) {
  for (var i = 0, l = retaineds.length; i < l; i++) {
    if (qlobber.test(retaineds[i].topic)) {
      stream.write(retaineds[i].topic)
    }
  }
  stream.end()
}

function decodeRetainedPacket (result, enc, cb) {
  var packet = msgpack.decode(result.rows[0].packet)
  cb(null, packet)
}

CassandraPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
    return
  }

  var that = this

  var published = 0
  var errored = false
  var queries = subs.map(function (sub) {
    debug('Storing subscription with client.id ' + client.id + ' and topic ' + sub.topic)

    return {
      query: 'INSERT INTO subscription_topic (client_id, topic, qos) VALUES (?, ?, ?)',
      params: [client.id, sub.topic, sub.qos]
    }
  }).concat(subs.map(function (sub) {
    return {
      query: 'INSERT INTO subscription_client (topic, client_id, qos) VALUES (?, ?, ?)',
      params: [sub.topic, client.id, sub.qos]
    }
  }))

  this._addedSubscriptions(client, subs, function (err) {
    finish(err)
    that._client.batch(queries, { prepare: true }, finish)
  })

  function finish (err) {
    if (err && !errored) {
      errored = true
      cb(err, client)
      return
    } else if (errored) {
      return
    }
    published++
    if (published === 2) {
      cb(null, client)
    }
  }
}

function toSub (topic) {
  return {
    topic: topic
  }
}

CassandraPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
    return
  }

  var published = 0
  var errored = false
  var queries = subs.map(function (sub) {
    debug('Removing subscription with client.id ' + client.id + ' and topic ' + sub)

    return {
      query: 'DELETE FROM subscription_topic WHERE client_id = ? AND topic = ?',
      params: [client.id, sub]
    }
  }).concat(subs.map(function (sub) {
    return {
      query: 'DELETE FROM subscription_client WHERE topic = ? AND client_id = ?',
      params: [sub, client.id]
    }
  }))

  this._client.batch(queries, { prepare: true }, finish)
  this._removedSubscriptions(client, subs.map(toSub), finish)

  function finish (err) {
    if (err && !errored) {
      errored = true
      cb(err, client)
      return
    }
    published++
    if (published === 2 && !errored) {
      cb(null, client)
    }
  }
}

CassandraPersistence.prototype.subscriptionsByClient = function (client, cb) {
  if (!this.ready) {
    this.once('ready', this.subscriptionsByClient.bind(this, client, cb))
    return
  }

  debug('Retrieving subscriptions with client.id ' + client.id)
  this._client.execute('SELECT * FROM subscription_topic WHERE client_id = ?', [client.id], { prepare: true }, function (err, result) {
    if (err) {
      cb(err)
      return
    }

    var toReturn = result.rows.map(function (row) {
      return {
        topic: row.topic,
        qos: row.qos
      }
    })

    cb(null, toReturn.length > 0 ? toReturn : null, client)
  })
}

CassandraPersistence.prototype.countOffline = function (cb) {
  debug('Counting offline clients')

  var that = this

  this._client.execute('SELECT DISTINCT client_id, COUNT(1) FROM subscription_topic LIMIT 10000000', [], { prepare: true }, function (err, result) {
    if (err) {
      return cb(err)
    }

    cb(null, that._trie.subscriptionsCount, result.rows[0].count)
  })
}

CassandraPersistence.prototype.destroy = function (cb) {
  if (!this.ready) {
    this.once('ready', this.destroy.bind(this, cb))
    return
  }

  if (this._destroyed) {
    throw new Error('destroyed called twice!')
  }

  this._destroyed = true

  cb = cb || noop

  if (this._opts.db) {
    cb()
  } else {
    this._client.close(function () {
      // swallow err in case of close
      cb()
    })
  }
}

CassandraPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.outgoingEnqueue.bind(this, sub, packet, cb))
    return
  }

  debug('Recording outgoing packet with client.id ' + sub.clientId)
  var id = uuidv4()
  var queries = [{
    query: 'INSERT INTO outgoing_packet (client_id, id, packet) VALUES (?, ?, ?)',
    params: [sub.clientId, id, msgpack.encode(packet)]
  }, {
    query: 'INSERT INTO outgoing_broker (client_id, broker_counter, broker_id, id) VALUES (?, ?, ?, ?)',
    params: [sub.clientId, packet.brokerCounter, packet.brokerId, id]
  }]

  if (packet.messageId) {
    queries.push({
      query: 'INSERT INTO outgoing_message (client_id, message_id, id) VALUES (?, ?, ?)',
      params: [sub.clientId, packet.messageId, id]
    })
  }

  this._client.batch(queries, { prepare: true }, cb)
}

function asPacket (obj, enc, cb) {
  cb(null, msgpack.decode(obj.packet))
}

CassandraPersistence.prototype.outgoingStream = function (client) {
  debug('Streaming outgoing packet with client.id ' + client.id)
  return pump(
    this._client.stream('SELECT * FROM outgoing_packet WHERE client_id = ?', [client.id], { prepare: true }),
    through.obj(asPacket)
  )
}

function updateWithMessageId (_client, client, packet, cb) {
  debug('Updating outgoing packet with client.id ' + client.id + ', brokerCounter ' + packet.brokerCounter + ' and brokerId ' + packet.brokerId)
  _client.execute('SELECT * FROM outgoing_broker WHERE client_id = ? AND broker_counter = ? AND broker_id = ?', [client.id, packet.brokerCounter, packet.brokerId], { prepare: true }, function (err, result) {
    if (err) {
      return cb(err)
    }

    if (result.rows.length === 0) {
      return cb(null, client, packet)
    }

    var id = result.rows[0].id

    var queries = [{
      query: 'INSERT INTO outgoing_packet (client_id, id, packet) VALUES (?, ?, ?)',
      params: [client.id, id, msgpack.encode(packet)]
    }, {
      query: 'INSERT INTO outgoing_message (client_id, message_id, id) VALUES (?, ?, ?)',
      params: [client.id, packet.messageId, id]
    }]

    _client.batch(queries, { prepare: true }, function (err) {
      if (err) {
        return cb(err)
      }

      return cb(null, client, packet)
    })
  })
}

function updatePacket (_client, client, packet, cb) {
  debug('Updating outgoing packet with client.id ' + client.id + ' and messageId ' + packet.messageId)
  _client.execute('SELECT * FROM outgoing_message WHERE client_id = ? AND message_id = ?', [client.id, packet.messageId], { prepare: true }, function (err, result) {
    if (err) {
      return cb(err)
    }

    if (result.rows.length === 0) {
      cb(null, client, packet)
    }

    var id = result.rows[0].id

    var queries = [{
      query: 'INSERT INTO outgoing_packet (client_id, id, packet) VALUES (?, ?, ?)',
      params: [client.id, id, msgpack.encode(packet)]
    }, {
      query: 'INSERT INTO outgoing_broker (client_id, broker_counter, broker_id, id) VALUES (?, ?, ?)',
      params: [client.id, packet.brokerCounter, packet.brokerId, id]
    }]

    _client.batch(queries, { prepare: true }, function (err) {
      if (err) {
        return cb(err)
      }

      return cb(null, client, packet)
    })
  })
}

CassandraPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.outgoingUpdate.bind(this, client, packet, cb))
    return
  }
  if (packet.brokerId) {
    updateWithMessageId(this._client, client, packet, cb)
  } else {
    updatePacket(this._client, client, packet, cb)
  }
}

CassandraPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.outgoingClearMessageId.bind(this, client, packet, cb))
    return
  }

  var that = this

  debug('Removing outgoing packet with client.id ' + client.id + ' and messageId ' + packet.messageId)
  this._client.execute('SELECT * FROM outgoing_message WHERE client_id = ? AND message_id = ?', [client.id, packet.messageId], { prepare: true }, function (err, result) {
    if (err) {
      return cb(err)
    }

    if (result.rows.length === 0) {
      return cb(null)
    }

    var id = result.rows[0].id

    that._client.execute('SELECT * FROM outgoing_packet WHERE client_id = ? AND id = ?', [client.id, id], { prepare: true }, function (err, result) {
      if (err) {
        return cb(err)
      }

      var retrievedPacket
      if (result.rows.length !== 0) {
        retrievedPacket = msgpack.decode(result.rows[0].packet)
      }

      var queries = [{
        query: 'DELETE FROM outgoing_message WHERE client_id = ? AND message_id = ?',
        params: [client.id, packet.messageId]
      }, {
        query: 'DELETE FROM outgoing_packet WHERE client_id = ? AND id = ?',
        params: [client.id, id]
      }]

      if (retrievedPacket && retrievedPacket.brokerCounter && retrievedPacket.brokerId) {
        queries.push({
          query: 'DELETE FROM outgoing_broker WHERE client_id = ? AND broker_counter = ? AND broker_id = ?',
          params: [client.id, retrievedPacket.brokerCounter, retrievedPacket.brokerId]
        })
      }

      that._client.batch(queries, { prepare: true }, cb)
    })
  })
}

CassandraPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.incomingStorePacket.bind(this, client, packet, cb))
    return
  }

  var newp = new Packet(packet)
  newp.messageId = packet.messageId

  debug('Storing incoming packet with client.id' + client.id + ' and messageId ' + packet.messageId)
  this._client.execute('INSERT INTO incoming (client_id, message_id, packet) VALUES (?, ?, ?)', [client.id, packet.message_id, msgpack.encode(newp)], { prepare: true }, cb)
}

CassandraPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.incomingGetPacket.bind(this, client, packet, cb))
    return
  }

  debug('Retrieving incoming packet with client.id ' + client.id + ' and messageId' + packet.messageId)
  this._client.execute('SELECT * FROM incoming WHERE client_id = ? AND message_id = ?', [client.id, packet.messageId], { prepare: true }, function (err, result) {
    if (err) {
      return cb(err)
    }

    if (result.rows.length === 0) {
      return cb(new Error('packet not found'), null, client)
    }

    var packet = msgpack.decode(result.rows[0].packet)

    if (packet && packet.payload) {
      packet.payload = packet.payload.buffer
    }

    cb(null, packet, client)
  })
}

CassandraPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.incomingDelPacket.bind(this, client, packet, cb))
    return
  }

  debug('Removing incoming packet with client.id ' + client.id + ' and messageId' + packet.messageId)
  this._client.execute('DELETE FROM incoming WHERE client_id = ? AND message_id = ?', [client.id, packet.messageId], { prepare: true }, cb)
}

CassandraPersistence.prototype.putWill = function (client, packet, cb) {
  if (!this.ready) {
    this.once('ready', this.putWill.bind(this, client, packet, cb))
    return
  }

  debug('Storing last will with client.id ' + client.id)
  packet.clientId = client.id
  packet.brokerId = this.broker.id
  this._client.execute('INSERT INTO will (client_id, packet) VALUES (?, ?)', [client.id, msgpack.encode(packet)], { prepare: true }, cb)
}

CassandraPersistence.prototype.getWill = function (client, cb) {
  debug('Retrieving last will with client.id ' + client.id)
  this._client.execute('SELECT * FROM will WHERE client_id = ?', [client.id], { prepare: true }, function (err, result) {
    if (err) {
      return cb(err)
    }

    if (result.rows.length === 0) {
      cb(null, null, client)
      return
    }

    var packet = msgpack.decode(result.rows[0].packet)

    if (packet && packet.payload) {
      packet.payload = packet.payload.buffer
    }

    cb(null, packet, client)
  })
}

CassandraPersistence.prototype.delWill = function (client, cb) {
  var that = this

  this.getWill(client, function (err, packet) {
    if (err || !packet) {
      cb(err, null, client)
      return
    }

    debug('Removing last will with client.id ' + client.id)
    that._client.execute('DELETE FROM will WHERE client_id = ?', [client.id], { prepare: true }, function (err) {
      cb(err, packet, client)
    })
  })
}

CassandraPersistence.prototype.streamWill = function (brokers) {
  debug('Streaming last wills')
  var stream = this._client.stream('SELECT * FROM will', [], { prepare: true })
  if (brokers) {
    return pump(stream, through.obj(asPacket), filter(function (packet) {
      return brokers[packet.brokerId]
    }))
  } else {
    return pump(stream, through.obj(asPacket))
  }
}

CassandraPersistence.prototype.getClientList = function (topic) {
  var stream
  if (topic) {
    debug('Streaming subscribed clients with topic ' + topic)
    this._client.stream('SELECT * FROM subscription_client WHERE topic = ?', [topic], { prepare: true })
  } else {
    debug('Streaming subscribed clients')
    this._client.stream('SELECT * FROM subscription_client', [], { prepare: true })
  }

  return pump(stream, through.obj(function asPacket (obj, enc, cb) {
    this.push(obj.clientId)
    cb()
  }))
}

function noop () { }

module.exports = CassandraPersistence
