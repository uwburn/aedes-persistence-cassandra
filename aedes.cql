CREATE TABLE subscription(
	client_id TEXT,
	topic TEXT,
	qos INT,
	PRIMARY KEY(client_id,topic)
);

CREATE TABLE subscription_by_topic(
	topic TEXT,
	client_id TEXT,
	qos INT,
	PRIMARY KEY(topic, client_id)
);

CREATE TABLE subscription_qos12(
	client_id TEXT,
	topic TEXT,
	qos INT,
	PRIMARY KEY(client_id,topic)
);

CREATE TABLE retained(
	topic TEXT,
	packet BLOB,
	PRIMARY KEY(topic)
);

CREATE TABLE last_will(
	client_id TEXT,
	packet BLOB,
	PRIMARY KEY(client_id)
);

CREATE TABLE outgoing(
	client_id TEXT,
	ref UUID,
	message_id BIGINT,
	broker_id TEXT,
	broker_counter BIGINT,
	cmd TEXT,
	topic TEXT,
	qos INT,
	retain BOOLEAN,
	dup BOOLEAN,
	payload BLOB,
	payload_type TEXT,
	PRIMARY KEY(client_id, ref)
);

CREATE TABLE outgoing_by_message_id(
	client_id TEXT,
	ref UUID,
	message_id BIGINT,
	broker_id TEXT,
	broker_counter BIGINT,
	cmd TEXT,
	topic TEXT,
	qos INT,
	retain BOOLEAN,
	dup BOOLEAN,
	payload BLOB,
	payload_type TEXT,
	PRIMARY KEY(client_id, message_id)
);

CREATE TABLE outgoing_by_broker(
	client_id TEXT,
	ref UUID,
	broker_id TEXT,
	broker_counter BIGINT,
	message_id BIGINT,
	cmd TEXT,
	topic TEXT,
	qos INT,
	retain BOOLEAN,
	dup BOOLEAN,
	payload BLOB,
	payload_type TEXT,
	PRIMARY KEY(client_id, broker_id, broker_counter)
);

CREATE TABLE incoming(
	client_id TEXT,
	message_id BIGINT,
	broker_id TEXT,
	broker_counter BIGINT,
	cmd TEXT,
	topic TEXT,
	qos INT,
	retain BOOLEAN,
	dup BOOLEAN,
	payload BLOB,
	payload_type TEXT,
	PRIMARY KEY(client_id, message_id)
);