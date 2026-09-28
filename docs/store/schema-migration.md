# Responses Store Schema Migration (v2/v3 → v4)

Schema **version 4** scopes conversation-item identity and position uniqueness
to the complete state owner (`tenant_id`, `owner_issuer`, `owner_subject`). This
allows two owners to use the same item identifiers without one owner's rows
colliding with the other owner's rows.

Version 3 changed the responses table's JSON payload columns from `TEXT` to
native binary columns (`BLOB` on SQLite, `BYTEA` on PostgreSQL). A version 2
deployment must apply that conversion first, then apply the version 4 item-table
migration below.

The proxy **refuses to start** against an older schema version. Migration is a
one-time, operator-run step and is not applied automatically. Take a backup
before running it.

Substitute the configured table names for these placeholders:

- `<responses_table>`: `openai_responses` by default
- `<items_table>`: `openai_conversation_items` by default for the
  `openai_conversations` filter

If the store does not configure an items table, no table shape changes are
required for v3 → v4. Update only the schema-version row.

## PostgreSQL: v3 → v4

Run the following in one transaction. The primary-key constraint name shown is
PostgreSQL's default for a table created by Praxis.

```sql
BEGIN;

ALTER TABLE <items_table>
    DROP CONSTRAINT <items_table>_pkey;

DROP INDEX IF EXISTS idx_<items_table>_position;

ALTER TABLE <items_table>
    ADD PRIMARY KEY (tenant_id, owner_issuer, owner_subject, item_id);

CREATE UNIQUE INDEX idx_<items_table>_position
    ON <items_table>
       (tenant_id, owner_issuer, owner_subject, conversation_id, position);

UPDATE <responses_table>_schema_version SET version = 4;

COMMIT;
```

For a store without an items table:

```sql
UPDATE <responses_table>_schema_version SET version = 4;
```

## SQLite: v3 → v4

SQLite cannot replace a primary key in place, so rebuild the items table in one
transaction:

```sql
BEGIN;

DROP INDEX IF EXISTS idx_<items_table>_conversation;
DROP INDEX IF EXISTS idx_<items_table>_position;

ALTER TABLE <items_table> RENAME TO <items_table>_v3;

CREATE TABLE <items_table> (
    item_id         TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    owner_issuer    TEXT NOT NULL,
    owner_subject   TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    item_data       TEXT NOT NULL,
    created_at      BIGINT NOT NULL,
    position        BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, owner_issuer, owner_subject, item_id)
);

INSERT INTO <items_table>
    (item_id, tenant_id, owner_issuer, owner_subject, conversation_id,
     item_data, created_at, position)
SELECT item_id, tenant_id, owner_issuer, owner_subject, conversation_id,
       item_data, created_at, position
FROM <items_table>_v3;

DROP TABLE <items_table>_v3;

CREATE INDEX idx_<items_table>_conversation
    ON <items_table>(conversation_id, position, item_id);

CREATE UNIQUE INDEX idx_<items_table>_position
    ON <items_table>
       (tenant_id, owner_issuer, owner_subject, conversation_id, position);

UPDATE <responses_table>_schema_version SET version = 4;

COMMIT;
```

For a store without an items table:

```sql
UPDATE <responses_table>_schema_version SET version = 4;
```

## Version 2 deployments

Before applying the v3 → v4 migration, convert the response payload columns to
the version 3 binary representation.

PostgreSQL:

```sql
BEGIN;

ALTER TABLE <responses_table>
    ALTER COLUMN response_object TYPE BYTEA USING convert_to(response_object, 'UTF8'),
    ALTER COLUMN input           TYPE BYTEA USING convert_to(input, 'UTF8'),
    ALTER COLUMN messages        TYPE BYTEA USING convert_to(messages, 'UTF8');

UPDATE <responses_table>_schema_version SET version = 3;

COMMIT;
```

SQLite:

```sql
BEGIN;

UPDATE <responses_table>
SET response_object = CAST(response_object AS BLOB),
    input           = CAST(input AS BLOB),
    messages        = CAST(messages AS BLOB);

UPDATE <responses_table>_schema_version SET version = 3;

COMMIT;
```

After that transaction commits, apply the backend's v3 → v4 migration above.

## The `openai_conversations` filter

This filter generates an internal, always-empty
`<conversations_table>_unused_responses` table and stores the schema version in
`<conversations_table>_unused_responses_schema_version`. Use that generated
responses-table name as `<responses_table>` in the commands above, and migrate
the configured conversations items table before stamping version 4.
