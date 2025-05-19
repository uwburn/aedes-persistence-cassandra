"use strict";

const CachedPersistence = require("aedes-cached-persistence");
const Packet = CachedPersistence.Packet;
const cassandra = require("cassandra-driver");
const { Qlobber } = require("qlobber");
const uuidv1 = require("uuid").v1;
const msgpack = require("msgpack-lite");

const qlobberOpts = {
  separator: "/",
  wildcard_one: "+",
  wildcard_some: "#",
  match_empty_levels: true
};

function decodeRow(row) {
  return msgpack.decode(row.packet);
}

module.exports = class AsyncCassandraPersistence {

  constructor(opts = {}) {
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
  }

  streamQos12Subscriptions() {
    return this._client.stream("SELECT * FROM subscription_qos12", [], { prepare: true });
  }

  async setup() {
    if (this._opts.client) {
      this._client = this._opts.client;
    }
    else {
      const defaultOpts = { contactPoints: ["localhost:9042"], localDataCenter: "datacenter1", keyspace: "aedes" };
      const cassandraOpts = this._opts.cassandra ? Object.assign(defaultOpts, this._opts.cassandra) : defaultOpts;

      this._shutdownClient = true;
      this._client = new cassandra.Client(cassandraOpts);
    }

    for await (const row of this._client.stream("SELECT * FROM subscription_qos12", [], { prepare: true })) {
      this._trie.add(row.topic, {
        topic: row.topic,
        clientId: row.client_id,
        qos: row.qos,
        nl: row.nl,
        rap: row.rap,
        rh: row.rh
      });
    }
  }

  async storeRetained(packet) {
    if (packet.payload.length === 0) {
      await this._client.execute("DELETE FROM retained WHERE topic = ?", [
        packet.topic
      ]);
    }
    else {
      await this._client.execute("INSERT INTO retained (topic, packet) VALUES (?, ?)", [
        packet.topic,
        msgpack.encode(packet)
      ], { prepare: true });
    }
  }

  createRetainedStream(pattern) {
    return this.createRetainedStreamCombi([pattern]);
  }

  async * createRetainedStreamCombi(patterns) {
    const matcher = new Qlobber(qlobberOpts);

    for (let i = 0; i < patterns.length; i++) {
      matcher.add(patterns[i], true);
    }

    for await (const row of this._client.stream("SELECT * FROM retained", [], { prepare: true })) {
      if (matcher.match(row.topic).length > 0) {
        yield decodeRow(row);
      }
    }
  }

  async addSubscriptions(client, subs) {
    const batch = [];

    let uniqSubs = {};
    for (let s of subs) {
      uniqSubs[s.topic] = s;
    }
    subs = Object.values(uniqSubs);

    subs.forEach((sub) => {
      const params = [client.id, sub.topic, sub.qos, sub.nl, sub.rap, sub.rh, this._opts.ttl.subscriptions];

      batch.push({
        query: "INSERT INTO subscription (client_id, topic, qos, nl, rap, rh) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?",
        params
      }, {
        query: "INSERT INTO subscription_by_topic (client_id, topic, qos, nl, rap, rh) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?",
        params
      });

      if (sub.qos > 0) {
        batch.push({
          query: "INSERT INTO subscription_qos12 (client_id, topic, qos, nl, rap, rh) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?",
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

    await this._client.batch(batch, { prepare: true });
  }

  toSub(topic) {
    return {
      topic
    };
  }

  async removeSubscriptions(client, subs) {
    const batch = [];
    subs.forEach(function(topic) {
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


    await this._client.batch(batch);
  }

  async subscriptionsByClient(client) {
    const result = await this._client.execute("SELECT * FROM subscription WHERE client_id = ?", [client.id], { prepare: true });

    return result.rows.map(function(row) {
      return {
        topic: row.topic,
        qos: row.qos,
        nl: row.nl,
        rap: row.rap,
        rh: row.rh
      };
    });
  }

  async countOffline() {
    const subsCount = this._trie.subscriptionsCount;
    let clientsCount = 0;

    // eslint-disable-next-line no-unused-vars
    for await (const row of this._client.stream("SELECT COUNT(*) FROM subscription GROUP BY client_id", [], { prepare: true })) {
      clientsCount++;
    }

    return { subsCount, clientsCount };
  }

  async destroy() {
    if (this._opts.client) {
      return;
    }

    if (this._shutdownClient) {
      try {
        await this._client.shutdown();
      }
      catch (err) {
        // Ignored
      }
    }
  }

  async outgoingEnqueue(sub, packet) {
    await this.outgoingEnqueueCombi([sub], packet);
  }

  async outgoingEnqueueCombi(subs, packet) {
    if (!subs || subs.length === 0) {
      return packet;
    }

    const encodedPacket = msgpack.encode(new Packet(packet));

    const batch = [];
    subs.forEach((sub) => {
      const ref = uuidv1();

      batch.push({
        query: "INSERT INTO outgoing (client_id, ref, packet) VALUES (?, ?, ?) USING TTL ?",
        params: [
          sub.clientId,
          ref,
          encodedPacket,
          this._opts.ttl.packets.outgoing
        ]
      });

      batch.push({
        query: "INSERT INTO outgoing_by_broker (client_id, broker_id, broker_counter, ref, packet) VALUES (?, ?, ?, ?, ?) USING TTL ?",
        params: [
          sub.clientId,
          packet.brokerId,
          packet.brokerCounter,
          ref,
          encodedPacket,
          this._opts.ttl.packets.outgoing
        ]
      });
    });

    await this._client.batch(batch, { prepare: true });
  }

  async * outgoingStream(client) {
    for await (const row of this._client.stream("SELECT * FROM outgoing WHERE client_id = ?", [client.id], { prepare: true })) {
      yield decodeRow(row);
    }
  }

  async updateWithMessageId(client, packet) {
    const result = await this._client.execute("SELECT * FROM outgoing_by_broker WHERE client_id = ? AND broker_id = ? AND broker_counter = ?", [
      client.id,
      packet.brokerId,
      packet.brokerCounter
    ], { prepare: true });

    const oldRow = result.rows[0];
    if (oldRow == null) {
      throw new Error("Existing outgoing message not found");
    }

    const decodedPacket = decodeRow(oldRow);
    const encodedPacket = msgpack.encode(packet);

    const batch = [{
      query: "INSERT INTO outgoing (client_id, ref, packet) VALUES (?, ?, ?) USING TTL ?",
      params: [
        client.id,
        oldRow.ref,
        encodedPacket,
        this._opts.ttl.packets.outgoing
      ]
    }, {
      query: "INSERT INTO outgoing_by_broker (client_id, broker_id, broker_counter, ref, packet) VALUES (?, ?, ?, ?, ?) USING TTL ?",
      params: [
        client.id,
        packet.brokerId,
        packet.brokerCounter,
        oldRow.ref,
        encodedPacket,
        this._opts.ttl.packets.outgoing
      ]
    }];

    if (decodedPacket.messageId != packet.messageId && decodedPacket.messageId != null) {
      batch.push({
        query: "DELETE FROM outgoing_by_message_id WHERE client_id = ? AND message_id = ?",
        params: [
          client.id,
          decodedPacket.messageId
        ]
      });
    }

    if (packet.messageId != null) {
      batch.push({
        query: "INSERT INTO outgoing_by_message_id (client_id, message_id, ref, packet) VALUES (?, ?, ?, ?) USING TTL ?",
        params: [
          client.id,
          packet.messageId,
          oldRow.ref,
          encodedPacket,
          this._opts.ttl.packets.outgoing
        ]
      });
    }

    await this._client.batch(batch, { prepare: true });
  }

  async updatePacket(client, packet) {
    const result = await this._client.execute("SELECT * FROM outgoing_by_message_id WHERE client_id = ? AND message_id = ?", [
      client.id,
      packet.messageId
    ], { prepare: true });

    const oldRow = result.rows[0];
    if (oldRow == null) {
      throw new Error("Existing outgoing message not found");
    }

    const decodedPacket = decodeRow(oldRow);
    const encodedPacket = msgpack.encode(packet);

    const batch = [{
      query: "INSERT INTO outgoing (client_id, ref, packet) VALUES (?, ?, ?) USING TTL ?",
      params: [
        client.id,
        oldRow.ref,
        encodedPacket,
        this._opts.ttl.packets.outgoing
      ]
    }, {
      query: "INSERT INTO outgoing_by_message_id (client_id, message_id, ref, packet) VALUES (?, ?, ?, ?) USING TTL ?",
      params: [
        client.id,
        packet.messageId,
        oldRow.ref,
        encodedPacket,
        this._opts.ttl.packets.outgoing
      ]
    }];

    if (decodedPacket.brokerId != null && decodedPacket.brokerCounter != null && (decodedPacket.brokerId != packet.brokerId || decodedPacket.brokerCounter != packet.brokerCounter)) {
      batch.push({
        query: "DELETE FROM outgoing_by_broker WHERE client_id = ? AND broker_id = ? AND broker_counter = ?",
        params: [
          client.id,
          decodedPacket.brokerId,
          decodedPacket.brokerCounter
        ]
      });
    }

    if (packet.brokerId != null && packet.brokerCounter != null) {
      batch.push({
        query: "INSERT INTO outgoing_by_broker (client_id, broker_id, broker_counter, ref, packet) VALUES (?, ?, ?, ?, ?) USING TTL ?",
        params: [
          client.id,
          packet.brokerId,
          packet.brokerCounter,
          oldRow.ref,
          encodedPacket,
          this._opts.ttl.packets.outgoing
        ]
      });
    }

    await this._client.batch(batch, { prepare: true });
  }

  async outgoingUpdate(client, packet) {
    if (packet.brokerId) {
      await this.updateWithMessageId(client, packet);
    }
    else {
      await this.updatePacket(client, packet);
    }
  }

  async outgoingClearMessageId(client, packet) {
    if (packet.messageId == null) {
      return;
    }

    let oldRow;
    const result = await this._client.execute("SELECT * FROM outgoing_by_message_id WHERE client_id = ? AND message_id = ?", [client.id, packet.messageId], { prepare: true });

    oldRow = result.rows[0];

    if (oldRow == null) {
      return null;
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

    await this._client.batch(batch, { prepare: true });

    return decodeRow(oldRow);
  }

  async incomingStorePacket(client, packet) {
    const newp = new Packet(packet);
    newp.messageId = packet.messageId;

    let query = "INSERT INTO incoming (client_id, message_id, packet) VALUES (?, ?, ?) USING TTL ?";
    const params = [
      client.id,
      newp.messageId,
      msgpack.encode(newp),
      this._opts.ttl.packets.incoming
    ];

    await this._client.execute(query, params, { prepare: true });
  }

  async incomingGetPacket(client, packet) {
    const result = await this._client.execute("SELECT * FROM incoming WHERE client_id = ? AND message_id = ?", [
      client.id,
      packet.messageId
    ], { prepare: true });

    const row = result.rows[0];

    if (row == null) {
      throw new Error("Existing incoming message not found");
    }

    return decodeRow(row);
  }

  async incomingDelPacket(client, packet) {
    await this._client.execute("DELETE FROM incoming WHERE client_id = ? AND message_id = ?", [
      client.id,
      packet.messageId
    ], { prepare: true });
  }

  async putWill(client, packet) {
    packet.clientId = client.id;
    packet.brokerId = this.broker.id;

    let query = "INSERT INTO last_will (client_id, packet) VALUES (?, ?) USING TTL ?";
    const params = [
      packet.clientId,
      msgpack.encode(packet),
      this._opts.ttl.packets.will
    ];

    await this._client.execute(query, params, { prepare: true });
  }

  async getWill(client) {
    const result = await this._client.execute("SELECT * FROM last_will WHERE client_id = ?", [client.id], { prepare: true });

    const row = result.rows[0];

    if (row == null) {
      return null;
    }

    return decodeRow(row);
  }

  async delWill(client) {
    const will = await this.getWill(client);

    if (will != null) {
      await this._client.execute("DELETE FROM last_will WHERE client_id = ?", [client.id], { prepare: true });
    }

    return will;
  }

  async * streamWill(brokers) {
    const brokerIds = brokers != null ? Object.keys(brokers) : null;

    for await (const row of this._client.stream("SELECT * FROM last_will", [], { prepare: true })) {
      const lastWill = decodeRow(row);

      if (brokerIds != null && brokerIds.includes(lastWill.brokerId)) {
        continue;
      }

      yield decodeRow(row);
    }
  }

  async * getClientList(topic) {
    let stream;
    if (topic) {
      stream = this._client.stream("SELECT * FROM subscription_by_topic WHERE topic = ?", [topic], { prepare: true });
    }
    else {
      stream = this._client.stream("SELECT * FROM subscription", [], { prepare: true });
    }

    for await (const sub of stream) {
      yield sub.client_id;
    }
  }
};
