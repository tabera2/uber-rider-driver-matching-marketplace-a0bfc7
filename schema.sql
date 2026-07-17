-- The marketplace's durable state. If the dispatch process dies, THIS is what
-- survives, and it is the only thing the system trusts on restart.

CREATE TABLE IF NOT EXISTS trips (
    trip_id          CHAR(32)     NOT NULL,
    rider_id         VARCHAR(64)  NOT NULL,
    pickup_lat       DOUBLE       NOT NULL,
    pickup_lng       DOUBLE       NOT NULL,
    state            ENUM('requested','matched','en_route','complete','cancelled')
                     NOT NULL DEFAULT 'requested',
    driver_id        VARCHAR(64)  NULL,
    fare_cents       INT          NULL,
    -- The client's dedupe key. UNIQUE is the whole idempotency mechanism:
    -- a retried request collides here instead of creating a second trip.
    idempotency_key  VARCHAR(80)  NOT NULL,
    version          INT          NOT NULL DEFAULT 0,
    created_ms       BIGINT       NOT NULL,
    updated_ms       BIGINT       NOT NULL,
    PRIMARY KEY (trip_id),
    UNIQUE KEY uk_trips_idem (idempotency_key),
    KEY ix_trips_state (state),
    KEY ix_trips_driver_live (driver_id, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- The driver's assignment ledger. One row per driver per LIVE trip.
-- The UNIQUE key on driver_id is what makes double-booking physically
-- impossible: the database refuses the second claim, it is not a race we win
-- by being fast. A claim is deleted when the trip reaches a terminal state.
CREATE TABLE IF NOT EXISTS driver_claims (
    driver_id   VARCHAR(64) NOT NULL,
    trip_id     CHAR(32)    NOT NULL,
    claimed_ms  BIGINT      NOT NULL,
    PRIMARY KEY (driver_id),
    UNIQUE KEY uk_claims_trip (trip_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
