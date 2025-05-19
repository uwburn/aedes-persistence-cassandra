"use strict";

const { test } = require("node:test");
const CassandraPersistence = require("./persistence");
let abs = require("aedes-persistence/abstract");
const CassandraShift = require("cassandra-shift");
const cassandra = require("cassandra-driver");
const fs = require("fs");

const client = new cassandra.Client({ contactPoints: ["localhost:9042"], localDataCenter: "datacenter1" });
let init = true;

abs({
  test: test,
  persistence: async function() {
    if (init) {
      await client.execute("DROP KEYSPACE IF EXISTS aedes");

      try {
        fs.mkdirSync("./migration");
        fs.copyFileSync("./aedes.cql", "./migration/01__aedes.cql");

        await new CassandraShift([client], {
          keyspace: "aedes",
          ensureKeyspace: true,
          useKeyspace: true,
          dir: `${__dirname}/migration`
        }).migrate();

        init = false;
      }
      finally {
        fs.unlinkSync("./migration/01__aedes.cql");
        fs.rmdirSync("./migration");
      }
    }

    await client.execute("TRUNCATE incoming");
    await client.execute("TRUNCATE last_will");
    await client.execute("TRUNCATE migration_history");
    await client.execute("TRUNCATE outgoing");
    await client.execute("TRUNCATE outgoing_by_broker");
    await client.execute("TRUNCATE outgoing_by_message_id");
    await client.execute("TRUNCATE retained");
    await client.execute("TRUNCATE subscription");
    await client.execute("TRUNCATE subscription_by_topic");
    await client.execute("TRUNCATE subscription_qos12");

    const persistence = CassandraPersistence({
      client
    });

    return persistence;
  }
});

test.after(async function() {
  await client.execute("DROP KEYSPACE IF EXISTS aedes");

  process.exit(0);
});