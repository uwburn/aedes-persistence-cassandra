CREATE TABLE subscription(
	client_id TEXT,
	topic TEXT,
	qos INT,
	nl BOOLEAN,
	rap BOOLEAN,
	rh INT,
	PRIMARY KEY(client_id,topic)
);

CREATE TABLE subscription_by_topic(
	topic TEXT,
	client_id TEXT,
	qos INT,
	nl BOOLEAN,
	rap BOOLEAN,
	rh INT,
	PRIMARY KEY(topic, client_id)
);

CREATE TABLE subscription_qos12(
	client_id TEXT,
	topic TEXT,
	qos INT,
	nl BOOLEAN,
	rap BOOLEAN,
	rh INT,
	PRIMARY KEY(client_id,topic)
);

CREATE TABLE retained(
	topic TEXT,
	packet BLOB,
	PRIMARY KEY(topic)
) WITH gc_grace_seconds = 600
  AND compaction = {
	'compaction_window_size': '8', 
    'compaction_window_unit': 'HOURS', 
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'
	};

CREATE TABLE last_will(
	client_id TEXT,
	packet BLOB,
	PRIMARY KEY(client_id)
) WITH gc_grace_seconds = 600
  AND compaction = {
	'compaction_window_size': '8', 
    'compaction_window_unit': 'HOURS', 
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'
	};

CREATE TABLE outgoing(
	client_id TEXT,
	ref UUID,
	packet BLOB,
	PRIMARY KEY(client_id, ref)
) WITH gc_grace_seconds = 600
  AND compaction = {
	'compaction_window_size': '8', 
    'compaction_window_unit': 'HOURS', 
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'
	};

CREATE TABLE outgoing_by_message_id(
	client_id TEXT,
	message_id BIGINT,
	ref UUID,
	packet BLOB,
	PRIMARY KEY(client_id, message_id)
) WITH gc_grace_seconds = 600
  AND compaction = {
	'compaction_window_size': '8', 
    'compaction_window_unit': 'HOURS', 
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'
	};

CREATE TABLE outgoing_by_broker(
	client_id TEXT,
	broker_id TEXT,
	broker_counter BIGINT,
	ref UUID,
	packet BLOB,
	PRIMARY KEY(client_id, broker_id, broker_counter)
) WITH gc_grace_seconds = 600
  AND compaction = {
	'compaction_window_size': '8', 
    'compaction_window_unit': 'HOURS', 
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'
	};

CREATE TABLE incoming(
	client_id TEXT,
	message_id BIGINT,
	packet BLOB,
	PRIMARY KEY(client_id, message_id)
) WITH gc_grace_seconds = 600
  AND compaction = {
	'compaction_window_size': '8', 
    'compaction_window_unit': 'HOURS', 
    'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'
	};