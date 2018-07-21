CREATE KEYSPACE aedes
WITH durable_writes = true
AND replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

CREATE TABLE aedes.incoming (
	client_id text,
	message_id text,
	packet blob,
	PRIMARY KEY (client_id, message_id)
);

CREATE TABLE aedes.outgoing_broker (
	client_id text,
	broker_counter int,
	broker_id text,
	id uuid,
	PRIMARY KEY (client_id, broker_counter, broker_id)
);

CREATE TABLE aedes.outgoing_message (
	client_id text,
	message_id bigint,
	id uuid,
	PRIMARY KEY (client_id, message_id)
);

CREATE TABLE aedes.outgoing_packet (
	client_id text,
	id uuid,
	packet blob,
	PRIMARY KEY (client_id, id)
);

CREATE TABLE aedes.retained (
	topic text,
	packet blob,
	PRIMARY KEY (topic)
);

CREATE TABLE aedes.subscription_client (
	topic text,
	client_id text,
	qos int,
	PRIMARY KEY (topic, client_id)
);

CREATE TABLE aedes.subscription_topic (
	client_id text,
	topic text,
	qos int,
	PRIMARY KEY (client_id, topic)
);

CREATE TABLE aedes.will (
	client_id text,
	packet blob,
	PRIMARY KEY (client_id)
);