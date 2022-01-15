# aedes-persistence-cassandra

[Aedes][aedes] [persistence][persistence], backed by [Cassandra][cassandra].

See [aedes-persistence][persistence] for the full API, and [Aedes][aedes] for usage.

_**NOTE**: preliminary version, WIP._

## Install

```bash
npm install aedes aedes-persistence-cassandra --save
```

or

```bash
yarn add mqemitter-kafka aedes-persistence-cassandra
```

Target keyspace must be initialized with `aedes.cql` script.

## API

### AedesPersistenceCassandra([opts])

Creates a new instance of aedes-persistence-cassandra.

### Options

- `ttl`: Used to set a ttl (time to live) to documents stored in collections
  - `packets`: Could be an integer value that specify the ttl in seconds of all packets collections or an Object that specifies for each collection its ttl in seconds. Packets collections are: `incoming`, `outgoing`, `retained`, `will`.
  - `susbscriptions`: Set a ttl (in seconds)
- `cassandra`: Extra options to pass to Cassandra driver (see [cassandra-driver](https://github.com/datastax/nodejs-driver)) (alternative to `client`)
- `client`: Existing `cassandra-driver` client instance (alternative to `cassandra`)

If neither `cassandra` or `client` option are supplied, connection to `localhost:9042` will be attempted with local datacenter `datacenter1` and keyspace `aedes`.

**Supplying an external client it's recommended.**

### Examples

```js
const AedesPersistenceCassandra = require("aedes-persistence-cassandra");

AedesPersistenceCassandra({
  cassandra: { 
    contactPoints: ["localhost:9042"],
    localDataCenter: "datacenter1",
    keyspace: "aedes"
  },
  // Optional ttl settings
  ttl: {
      packets: 300, // Number of seconds
      subscriptions: 300,
  }
})
```

With the previous configuration all packets will have a ttl of 300 seconds. You can also provide different ttl settings for each packet type:

```js
ttl: {
      packets: {
        incoming: 100,
        outgoing: 100,
        will: 300,
        retained: -1
      }, // Number of seconds
      subscriptions: 300,
}
```

If you want a specific packet type to be **persistent** just set corresponding ttl to `null` or `undefined`.

## Acknowledgements

Implementation inspired after [aedes-persistence-mongodb](https://github.com/moscajs/aedes-persistence-mongodb).

## License

MIT

[aedes]: https://github.com/moscajs/aedes
[persistence]: https://github.com/moscajs/aedes-persistence
[cassandra]: https://cassandra.apache.org/_/index.html
