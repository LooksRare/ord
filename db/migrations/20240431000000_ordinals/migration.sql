create table inscriptions
(
    id                  bigserial
        primary key,
    genesis_id          text                                   not null, --TODO tx_id + index, should be unique
    number              bigint                                 not null  --TODO jubilee number, uniqueness depends on indexer implementation
        constraint inscriptions_number_unique
            unique,
    sat_ordinal         numeric                                not null,
    sat_rarity          text                                   not null,
    sat_coinbase_height bigint                                 not null,
    mime_type           text                                   not null, --TODO not sure we need it
    content_type        text                                   not null,
    content_length      bigint                                 not null, --TODO not sure we need it
    content             bytea                                  not null, --TODO should be moved to S3
    fee                 numeric                                not null, --TODO mint fee should belong to event?
    curse_type          text,
    updated_at          timestamp with time zone default now() not null,
    recursive           boolean                  default false,
    classic_number      bigint,
    metadata            text,                                            --TODO needs structuring, currently dump of whatever
    parent              text                                             --genesis_id of another inscription, used to create "collections"
);
