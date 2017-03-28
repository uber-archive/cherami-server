ALTER TABLE destinations ADD kafka_cluster text;
ALTER TABLE destinations ADD kafka_topics set<text>;
ALTER TABLE destinations_by_path ADD kafka_cluster text;
ALTER TABLE destinations_by_path ADD kafka_topics set<text>;
