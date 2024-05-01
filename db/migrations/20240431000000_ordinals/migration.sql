CREATE TABLE events
(
  id             SERIAL PRIMARY KEY,
  type_id        SMALLINT NOT NULL, --1,InscriptionCreated;2,InscriptionTransferred
  block_height   BIGINT   NOT NULL,
  inscription_id TEXT     NOT NULL,
  location       TEXT,              -- Will hold either 'location' or 'new_location' based on type
  old_location   TEXT,              -- Only used for InscriptionTransferred
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_events_block_height ON events (block_height);
CREATE INDEX idx_events_inscription_id ON events (inscription_id);
