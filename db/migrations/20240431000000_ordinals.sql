CREATE TABLE event (
    id             SERIAL PRIMARY KEY
  , type_id        SMALLINT NOT NULL --1=InscriptionCreated; 2=InscriptionTransferred
  , block_height   INTEGER  NOT NULL
  , inscription_id TEXT     NOT NULL
  , location       TEXT -- Will hold either 'location' or 'new_location' based on type
  , old_location   TEXT -- Only used for InscriptionTransferred
  , created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_events_block_height ON event (block_height);
CREATE INDEX idx_events_inscription_id ON event (inscription_id);

CREATE TABLE inscription (
    id                   SERIAL   PRIMARY KEY
  , genesis_id           TEXT     NOT NULL
  , number               INTEGER  NOT NULL
  , content_type         VARCHAR
  , content_length       INTEGER
  , metadata             TEXT
  , genesis_block_height INTEGER  NOT NULL
  , genesis_block_time   BIGINT   NOT NULL
  , sat_number           BIGINT
  , sat_rarity           INTEGER
  , sat_block_height     INTEGER
  , sat_block_time       BIGINT
  , fee                  BIGINT   NOT NULL
  , charms               SMALLINT NOT NULL
  , children             TEXT
  , parents              TEXT
);

CREATE UNIQUE INDEX idx_unique_inscription_genesis_id ON inscription (genesis_id);

CREATE TABLE location (
    id             SERIAL PRIMARY KEY,
    inscription_id INTEGER REFERENCES inscription (id),
    block_height   INTEGER NOT NULL,
    block_time     BIGINT  NOT NULL,
    tx_id          TEXT    NOT NULL,
    tx_index       INTEGER NOT NULL,
    to_address     TEXT    NOT NULL,
    cur_output     TEXT    NOT NULL,
    cur_offset     BIGINT  NOT NULL,
    from_address   TEXT,
    prev_output    TEXT,
    prev_offset    BIGINT,
    value          BIGINT
);

CREATE UNIQUE INDEX location_inscription_id_tx_id_tx_index_unique ON location (inscription_id, tx_id, tx_index);