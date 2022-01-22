"use strict";

const util = require("util");
const CachedPersistence = require("aedes-cached-persistence");
const Packet = CachedPersistence.Packet;
const cassandra = require("cassandra-driver");
const pump = require("pump");
const through = require("through2");
const Qlobber = require("qlobber").Qlobber;
const uuidv4 = require("uuid").v4;

const qlobberOpts = {
  separator: "/",
  wildcard_one: "+",
  wildcard_some: "#",
  match_empty_levels: true
};

function noop() { }

function CassandraPersistence(opts) {
  if (!(this instanceof CassandraPersistence)) {
    return new CassandraPersistence(opts);
  }

  opts = opts || {};

  let ttl = opts.ttl != null ? { ...opts.ttl } : {};
  if (typeof ttl.packets === "number") {
    ttl.packets = {
      retained: ttl.packets,
      will: ttl.packets,
      outgoing: ttl.packets,
      incoming: ttl.packets
    };
  }
  else if (ttl.packets == null) {
    ttl.packets = {};
  }

  ttl.packets.retained = ttl.packets.retained || 0;
  ttl.packets.will = ttl.packets.will || 0;
  ttl.packets.outgoing = ttl.packets.outgoing || 0;
  ttl.packets.incoming = ttl.packets.incoming || 0;
  ttl.subscriptions = ttl.subscriptions || 0;

  this._opts = opts;
  this._opts.ttl = ttl;
  this._shutdownClient = false;
  this._client = null;
  this.retainedQueue = []; // used for storing retained packets with ordered bulks
  this.executing = false; // used as lock while a bulk is executing

  CachedPersistence.call(this, opts);
}

util.inherits(CassandraPersistence, CachedPersistence);

function streamQos12Subscriptions(that) {
  return that._client.stream("SELECT * FROM subscription_qos12", [], { prepare: true });
}

CassandraPersistence.prototype._setup = function() {
  if (this.ready) {
    return;
  }

  const that = this;

  if (that._opts.client) {
    that._client = that._opts.client;
  }
  else {
    const defaultOpts = { contactPoints: ["localhost:9042"], localDataCenter: "datacenter1", keyspace: "aedes" };
    const cassandraOpts = that._opts.cassandra ? Object.assign(defaultOpts, that._opts.kafka) : defaultOpts;

    that._shutdownClient = true;
    that._client = new cassandra.Client(cassandraOpts);
  }

  streamQos12Subscriptions(that)
    .on("data", function(row) {
      that._trie.add(row.topic, {
        topic: row.topic,
        clientId: row.client_id,
        qos: row.qos
      });
    }).on("end", function() {
      that.emit("ready");
    }).on("error", function(err) {
      that.emit("error", err);
    });
};

CassandraPersistence.prototype.storeRetained = function(packet, cb) {
  if (!this.ready) {
    this.once("ready", this.storeRetained.bind(this, packet, cb));
    return;
  }

  this.retainedQueue.push({ packet, cb });
  processRetained(this);
};

async function processRetained(that) {
  if (!that.executing && that.retainedQueue.length > 0) {
    that.executing = true;
    const batch = [];
    const onEnd = [];

    while (that.retainedQueue.length) {
      const p = that.retainedQueue.shift();
      onEnd.push(p.cb);

      if (p.packet.payload.length > 0) {
        batch.push({
          query: "INSERT INTO retained (topic, broker_id, broker_counter, cmd, dup, qos, payload) VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?",
          params: [
            p.packet.topic,
            p.packet.brokerId,
            p.packet.brokerCounter,
            p.packet.cmd,
            p.packet.dup,
            p.packet.qos,
            p.packet.payload,
            that._opts.ttl.packets.retained
          ]
        });
      }
      else {
        batch.push({
          query: "DELETE FROM retained WHERE topic = ?",
          params: [
            p.packet.topic
          ]
        });
      }
    }

    await that._client.batch(batch, { prepare: true });

    while (onEnd.length) {
      onEnd.shift().call();
    }

    that.executing = false;
    processRetained(that);
  }
}

function filterAndParseRetained(row, enc, cb) {
  if (this.matcher.match(row.topic).length > 0) {
    this.push({
      topic: row.topic,
      brokerId: row.broker_id,
      brokerCounter: row.broker_counter != null ? row.broker_counter.toNumber() : null,
      cmd: row.cmd,
      dup: row.dup,
      qos: row.qos,
      payload: row.payload
    });
  }
  cb();
}

CassandraPersistence.prototype.createRetainedStream = function(pattern) {
  return this.doCreateRetainedStream([pattern]);
};

CassandraPersistence.prototype.doCreateRetainedStream = function(patterns) {
  const filterAndParseStream = through.obj(filterAndParseRetained);
  filterAndParseStream.matcher = new Qlobber(qlobberOpts);

  for (let i = 0; i < patterns.length; i++) {
    filterAndParseStream.matcher.add(patterns[i], true);
  }

  return pump(
    this._client.stream("SELECT * FROM retained", [], { prepare: true }),
    filterAndParseStream
  );
};

CassandraPersistence.prototype.addSubscriptions = function(client, subs, cb) {
  if (!this.ready) {
    this.once("ready", this.addSubscriptions.bind(this, client, subs, cb));
    return;
  }

  let published = 0;
  let errored = false;
  const batch = [];
  const that = this;

  let uniqSubs = {};
  for (let s of subs) {
    uniqSubs[s.topic] = s;
  }
  subs = Object.values(uniqSubs);

  subs
    .forEach(function(sub) {
      const params = [client.id, sub.topic, sub.qos, that._opts.ttl.subscriptions];

      batch.push({
        query: "INSERT INTO subscription (client_id, topic, qos) VALUES (?, ?, ?) USING TTL ?",
        params
      }, {
        query: "INSERT INTO subscription_by_topic (client_id, topic, qos) VALUES (?, ?, ?) USING TTL ?",
        params
      });

      if (sub.qos > 0) {
        batch.push({
          query: "INSERT INTO subscription_qos12 (client_id, topic, qos) VALUES (?, ?, ?) USING TTL ?",
          params
        });
      }
      else {
        batch.push({
          query: "DELETE FROM subscription_qos12 WHERE client_id = ? AND topic = ?",
          params: [client.id, sub.topic]
        });
      }
    });

  this._client.batch(batch, { prepare: true }).then(function() {
    finish();
  }).catch(finish);
  this._addedSubscriptions(client, subs, finish);

  function finish(err) {
    errored = err;
    published++;
    if (published === 2) {
      cb(errored, client);
    }
  }
};

function toSub(topic) {
  return {
    topic: topic
  };
}

CassandraPersistence.prototype.removeSubscriptions = function(client, subs, cb) {
  if (!this.ready) {
    this.once("ready", this.removeSubscriptions.bind(this, client, subs, cb));
    return;
  }

  let published = 0;
  let errored = false;
  const batch = [];
  subs
    .forEach(function(topic) {
      const params = [client.id, topic];

      batch.push({
        query: "DELETE FROM subscription WHERE client_id = ? AND topic = ?",
        params
      }, {
        query: "DELETE FROM subscription_by_topic WHERE client_id = ? AND topic = ?",
        params
      }, {
        query: "DELETE FROM subscription_qos12 WHERE client_id = ? AND topic = ?",
        params
      });
    });


  this._client.batch(batch).then(function() {
    finish();
  }).catch(finish);
  this._removedSubscriptions(client, subs.map(toSub), finish);

  function finish(err) {
    if (err && !errored) {
      errored = true;
      cb(err, client);
      return;
    }
    published++;
    if (published === 2 && !errored) {
      cb(null, client);
    }
  }
};

CassandraPersistence.prototype.subscriptionsByClient = function(client, cb) {
  if (!this.ready) {
    this.once("ready", this.subscriptionsByClient.bind(this, client, cb));
    return;
  }

  this._client.execute("SELECT * FROM subscription WHERE client_id = ?", [client.id], { prepare: true }, function(err, result) {
    if (err) {
      cb(err);
      return;
    }

    const subs = result.rows.map(function(row) {
      return {
        topic: row.topic,
        qos: row.qos
      };
    });

    cb(null, subs.length > 0 ? subs : null, client);
  });
};

CassandraPersistence.prototype.countOffline = function(cb) {
  let clientsCount = 0;
  const that = this;
  this._client.stream("SELECT COUNT(*) FROM subscription GROUP BY client_id", [], { prepare: true })
    .on("data", function() {
      clientsCount++;
    }).on("end", function() {
      cb(null, that._trie.subscriptionsCount, clientsCount);
    }).on("error", cb);
};

CassandraPersistence.prototype.destroy = function(cb) {
  if (!this.ready) {
    this.once("ready", this.destroy.bind(this, cb));
    return;
  }

  if (this._destroyed) {
    throw new Error("destroyed called twice!");
  }

  this._destroyed = true;

  cb = cb || noop;

  if (this._opts.client) {
    cb();
  }
  else {
    if (this._shutdownClient) {
      this._client.shutdown(function() {
        // swallow err in case of close
        cb();
      });
    }
    else {
      cb();
    }
  }
};

CassandraPersistence.prototype.outgoingEnqueue = function(sub, packet, cb) {
  this.outgoingEnqueueCombi([sub], packet, cb);
};

CassandraPersistence.prototype.outgoingEnqueueCombi = function(subs, packet, cb) {
  if (!this.ready) {
    this.once("ready", this.outgoingEnqueueCombi.bind(this, subs, packet, cb));
    return;
  }

  if (!subs || subs.length === 0) {
    return cb(null, packet);
  }

  const newp = new Packet(packet);

  const that = this;
  const batch = [];
  subs.map(function(sub) {
    const params = [sub.clientId, uuidv4(), newp.messageId, newp.brokerId, newp.brokerCounter, newp.cmd, newp.topic, newp.qos, newp.retain, newp.dup, newp.payload, that._opts.ttl.packets.outgoing];

    batch.push({
      query: "INSERT INTO outgoing (client_id, ref, message_id, broker_id, broker_counter, cmd, topic, qos, retain, dup, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
      params
    });

    if (newp.messageId != null) {
      batch.push({
        query: "INSERT INTO outgoing_by_message_id (client_id, ref, message_id, broker_id, broker_counter, cmd, topic, qos, retain, dup, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
        params
      });
    }

    if (newp.brokerId != null && newp.brokerCounter != null) {
      batch.push({
        query: "INSERT INTO outgoing_by_broker (client_id, ref, message_id, broker_id, broker_counter, cmd, topic, qos, retain, dup, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
        params
      });
    }
  });

  this._client.batch(batch, { prepare: true }, function(err) {
    cb(err, packet);
  });
};

function asPacket(row) {
  return {
    messageId: row.message_id != null ? row.message_id.toNumber() : null,
    brokerId: row.broker_id,
    brokerCounter: row.broker_counter != null ? row.broker_counter.toNumber() : null,
    cmd: row.cmd,
    topic: row.topic,
    qos: row.qos,
    retain: row.retain,
    dup: row.dup,
    payload: row.payload
  };
}

CassandraPersistence.prototype.outgoingStream = function(client) {
  return pump(
    this._client.stream("SELECT * FROM outgoing WHERE client_id = ?", [client.id], { prepare: true }),
    through.obj(function(row, enc, cb) {
      cb(null, asPacket(row));
    }));
};

async function updateWithMessageId(that, client, packet, cb) {
  const result = await that._client.execute("SELECT * FROM outgoing_by_broker WHERE client_id = ? AND broker_id = ? AND broker_counter = ?", [
    client.id,
    packet.brokerId,
    packet.brokerCounter
  ], { prepare: true });

  const oldRow = result.rows[0];
  if (oldRow == null) {
    cb(new Error("Existing outgoing message not found"));
    return;
  }

  const batch = [
    {
      query: "UPDATE outgoing_by_broker SET message_id = ? WHERE client_id = ? AND broker_id = ? AND broker_counter = ?",
      params: [
        packet.messageId,
        client.id,
        packet.brokerId,
        packet.brokerCounter
      ]
    }
  ];

  if (oldRow.messageId != null && oldRow.message_id.toNumber() != packet.messageId) {
    batch.push({
      query: "DELETE FROM outgoing_by_message_id WHERE client_id = ? AND message_id = ?",
      params: [oldRow.client_id, oldRow.message_id.toNumber()]
    });
  }

  const messageId = packet.messageId != null ? packet.messageId : (oldRow.message_id != null ? oldRow.message_id.toNumber() : null);
  batch.push({
    query: "INSERT INTO outgoing_by_message_id (client_id, ref, message_id, broker_id, broker_counter, cmd, topic, qos, retain, dup, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
    params: [client.id, oldRow.ref, messageId, oldRow.broker_id, oldRow.broker_counter, oldRow.cmd, oldRow.topic, oldRow.qos, oldRow.retain, oldRow.dup, oldRow.payload, that._opts.ttl.packets.outgoing]
  }, {
    query: "UPDATE outgoing SET message_id = ? WHERE client_id = ? AND ref = ?",
    params: [
      packet.messageId,
      client.id,
      oldRow.ref
    ]
  });

  that._client.batch(batch, { prepare: true }, function(err) {
    cb(err, client, packet);
  });
}

async function updatePacket(that, client, packet, cb) {
  const result = await that._client.execute("SELECT * FROM outgoing_by_message_id WHERE client_id = ? AND message_id = ?", [
    client.id,
    packet.messageId
  ], { prepare: true });

  const oldRow = result.rows[0];
  if (oldRow == null) {
    cb(new Error("Existing outgoing message not found"));
    return;
  }

  const brokerId = packet.brokerId != null ? packet.brokerId : oldRow.broker_id;
  const brokerCounter = packet.brokerCounter != null ? packet.brokerCounter : (oldRow.broker_counter != null ? oldRow.broker_counter.toNumber() : null);
  const params = [client.id, oldRow.ref, packet.messageId, brokerId, brokerCounter, packet.cmd, packet.topic, packet.qos, packet.retain, packet.dup, packet.payload, that._opts.ttl.packets.outgoing];

  const batch = [{
    query: "INSERT INTO outgoing_by_message_id (client_id, ref, message_id, broker_id, broker_counter, cmd, topic, qos, retain, dup, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
    params
  }, {
    query: "UPDATE outgoing SET message_id = ? WHERE client_id = ? AND ref = ?",
    params: [
      packet.messageId,
      client.id,
      oldRow.ref
    ]
  }];

  if (oldRow.broker_id!= null && oldRow.broker_counter != null && (oldRow.broker_id != packet.brokerId || oldRow.broker_counter.toNumber() != packet.brokerCounter)) {
    batch.push({
      query: "DELETE FROM outgoing_by_broker WHERE client_id = ? AND broker_id = ? AND broker_counter = ?",
      params: [oldRow.client_id, oldRow.broker_id, oldRow.broker_counter.toNumber()]
    });
  }

  batch.push({
    query: "INSERT INTO outgoing_by_broker (client_id, ref, message_id, broker_id, broker_counter, cmd, topic, qos, retain, dup, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
    params
  });

  that._client.batch(batch, { prepare: true }, function(err) {
    cb(err, client, packet);
  });
}

CassandraPersistence.prototype.outgoingUpdate = function(client, packet, cb) {
  if (!this.ready) {
    this.once("ready", this.outgoingUpdate.bind(this, client, packet, cb));
    return;
  }
  if (packet.brokerId) {
    updateWithMessageId(this, client, packet, cb);
  }
  else {
    updatePacket(this, client, packet, cb);
  }
};

CassandraPersistence.prototype.outgoingClearMessageId = async function(client, packet, cb) {
  if (!this.ready) {
    this.once("ready", this.outgoingClearMessageId.bind(this, client, packet, cb));
    return;
  }

  let oldRow;
  try {
    const result = await this._client.execute("SELECT * FROM outgoing_by_message_id WHERE client_id = ? AND message_id = ?", [client.id, packet.messageId], { prepare: true });

    if (!result.rows.length) {
      return cb(null);
    }

    oldRow = result.rows[0];
  }
  catch (err) {
    cb(err);
  }

  if (oldRow == null) {
    cb(new Error("Existing outgoing message not found"));
    return;
  }

  const batch = [
    {
      query: "DELETE FROM outgoing WHERE client_id = ? AND ref = ?",
      params: [client.id, oldRow.ref]
    },
    {
      query: "DELETE FROM outgoing_by_message_id WHERE client_id = ? AND message_id = ?",
      params: [client.id, oldRow.message_id.toNumber()]
    }
  ];

  if (oldRow.broker_id != null && oldRow.broker_counter != null) {
    batch.push({
      query: "DELETE FROM outgoing_by_broker WHERE client_id = ? AND broker_id = ? AND broker_counter = ?",
      params: [client.id, oldRow.broker_id, oldRow.broker_counter.toNumber()]
    });
  }

  this._client.batch(batch, { prepare: true }, function(err) {
    cb(err, asPacket(oldRow));
  });
};

CassandraPersistence.prototype.incomingStorePacket = function(client, packet, cb) {
  if (!this.ready) {
    this.once("ready", this.incomingStorePacket.bind(this, client, packet, cb));
    return;
  }

  const newp = new Packet(packet);
  newp.messageId = packet.messageId;

  let query = "INSERT INTO incoming (client_id, message_id, broker_id, broker_counter, cmd, topic, qos, retain, dup, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?";
  const params = [
    client.id,
    newp.messageId,
    newp.brokerId,
    newp.brokerCounter,
    newp.cmd,
    newp.topic,
    newp.qos,
    newp.retain,
    newp.dup,
    newp.payload,
    this._opts.ttl.packets.incoming
  ];

  this._client.execute(query, params, { prepare: true }, cb);
};

CassandraPersistence.prototype.incomingGetPacket = function(client, packet, cb) {
  if (!this.ready) {
    this.once("ready", this.incomingGetPacket.bind(this, client, packet, cb));
    return;
  }

  this._client.execute("SELECT * FROM incoming WHERE client_id = ? AND message_id = ?", [
    client.id,
    packet.messageId
  ], { prepare: true }, function(err, result) {
    if (err) {
      cb(err);
      return;
    }

    if (!result.rows.length) {
      cb(new Error("packet not found"), null, client);
      return;
    }

    cb(null, asPacket(result.rows[0]), client);
  });
};

CassandraPersistence.prototype.incomingDelPacket = function(client, packet, cb) {
  if (!this.ready) {
    this.once("ready", this.incomingDelPacket.bind(this, client, packet, cb));
    return;
  }

  this._client.execute("DELETE FROM incoming WHERE client_id = ? AND message_id = ?", [
    client.id,
    packet.messageId
  ], { prepare: true }, cb);
};

CassandraPersistence.prototype.putWill = function(client, packet, cb) {
  if (!this.ready) {
    this.once("ready", this.putWill.bind(this, client, packet, cb));
    return;
  }

  packet.clientId = client.id;
  packet.brokerId = this.broker.id;

  let query = "INSERT INTO last_will (client_id, broker_id, topic, qos, retain, payload) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?";
  const params = [
    packet.clientId,
    packet.brokerId,
    packet.topic,
    packet.qos,
    packet.retain,
    packet.payload,
    this._opts.ttl.packets.will
  ];

  this._client.execute(query, params, { prepare: true }, function(err) {
    cb(err, client);
  });
};

CassandraPersistence.prototype.getWill = function(client, cb) {
  this._client.execute("SELECT * FROM last_will WHERE client_id = ?", [client.id], { prepare: true }, function(err, result) {
    if (err) {
      cb(err);
      return;
    }

    if (!result.rows.length) {
      cb(null, null, client);
      return;
    }

    cb(null, asPacket(result.rows[0]), client);
  });
};

CassandraPersistence.prototype.delWill = function(client, cb) {
  const that = this;
  this.getWill(client, function(err, packet) {
    if (err || !packet) {
      cb(err, null, client);
      return;
    }

    that._client.execute("DELETE FROM last_will WHERE client_id = ?", [client.id], { prepare: true }, function(err) {
      cb(err, packet, client);
    });
  });
};

CassandraPersistence.prototype.streamWill = function(brokers) {
  const stream = this._client.stream("SELECT * FROM last_will", [], { prepare: true });

  const brokerIds = Object.keys(brokers);

  return pump(stream, through.obj(function(row, enc, cb) {
    if (brokerIds.includes(row.broker_id)) {
      cb(null);
      return;
    }

    cb(null, asPacket(row));
  }));
};

CassandraPersistence.prototype.getClientList = function(topic) {
  let stream;
  if (topic) {
    stream = this._client.stream("SELECT * FROM subscription_by_topic WHERE topic = ?", [topic], { prepare: true });
  }
  else {
    stream = this._client.stream("SELECT * FROM subscription", [], { prepare: true });
  }

  return pump(stream, through.obj(function(row, enc, cb) {
    this.push(row.client_id);
    cb();
  }));
};

module.exports = CassandraPersistence;
