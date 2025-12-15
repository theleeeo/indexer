CREATE TABLE IF NOT EXISTS relations (
	id SERIAL PRIMARY KEY,
	resource VARCHAR NOT NULL,
	resource_id VARCHAR NOT NULL,
	related_resource VARCHAR NOT NULL,
	related_resource_id VARCHAR NOT NULL,
	UNIQUE (resource, resource_id, related_resource, related_resource_id)
);
CREATE INDEX IF NOT EXISTS idx_resource ON relations (resource, resource_id);
CREATE INDEX IF NOT EXISTS idx_related_resource ON relations (related_resource, related_resource_id);