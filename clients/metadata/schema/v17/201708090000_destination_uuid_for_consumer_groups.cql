ALTER TABLE consumer_groups ADD destination_uuid uuid;
CREATE INDEX ON consumer_groups (destination_uuid);
