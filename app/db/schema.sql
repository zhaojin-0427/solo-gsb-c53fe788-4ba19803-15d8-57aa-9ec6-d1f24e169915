-- Distributed quota decision API: schema and stored functions.
-- All token arithmetic is NUMERIC(18,4); time is measured in seconds (float).

-- A token bucket row is exactly one of the three policy levels:
--   tenant  -> (tenant, '', '')
--   subject -> (tenant, subject, '')
--   action  -> (tenant, subject, action)
CREATE TABLE IF NOT EXISTS quota_buckets (
    tenant       TEXT        NOT NULL,
    subject      TEXT        NOT NULL DEFAULT '',
    action       TEXT        NOT NULL DEFAULT '',
    level        TEXT        NOT NULL CHECK (level IN ('tenant','subject','action')),
    capacity     NUMERIC(18,4) NOT NULL CHECK (capacity > 0),
    refill_rate  NUMERIC(18,4) NOT NULL CHECK (refill_rate >= 0),
    tokens       NUMERIC(18,4) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, subject, action)
);

-- Reservation lifecycle:
--   granted -> committed   (commit consumes no tokens)
--   granted -> cancelled   (cancel refunds exactly once)
--   granted -> expired     (reaper refunds exactly once)
--   denied is terminal and holds the idempotent decision.
CREATE TABLE IF NOT EXISTS quota_reservations (
    request_id   TEXT        PRIMARY KEY,
    tenant       TEXT        NOT NULL,
    subject      TEXT        NOT NULL,
    action       TEXT        NOT NULL,
    cost         NUMERIC(18,4) NOT NULL CHECK (cost > 0),
    fingerprint  JSONB       NOT NULL,
    status       TEXT        NOT NULL CHECK (status IN ('granted','committed','cancelled','expired','denied')),
    decision     JSONB       NOT NULL,
    expires_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT expired_has_deadline CHECK (status <> 'granted' OR expires_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_reservations_recycle
    ON quota_reservations (expires_at)
    WHERE status = 'granted';

-- ---------------------------------------------------------------------------
-- quota_reserve: atomic three-level check-and-deduct.
--
-- Locks the tenant / subject / action buckets in fixed global order
-- (tenant -> subject -> action, each via SELECT ... FOR UPDATE), so
-- concurrent grants are serialized and can never oversell; deadlocks
-- between reserve calls are impossible. Every missing bucket row is
-- upserted first with the DEFAULT_* parameters so the API needs no
-- separate provisioning step.
--
-- Result JSONB:
--   {"outcome":"conflict"}
--   {"outcome":"granted", "available":[{level,tokens_available,...}x3]}
--   {"outcome":"denied", "levels":[{level, tokens_available, required,
--                                  retry_after_seconds, retryable}x all
--                            deficient levels]}
-- A second OUT parameter reports whether this call actually inserted the
-- reservation row (false => idempotent replay of the stored decision).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION quota_reserve(
    p_request_id   TEXT,
    p_tenant       TEXT,
    p_subject      TEXT,
    p_action       TEXT,
    p_cost         NUMERIC,
    p_fingerprint  JSONB,
    p_ttl_seconds  DOUBLE PRECISION,
    p_def_capacity NUMERIC,
    p_def_refill   NUMERIC
) RETURNS TABLE(result JSONB, is_new BOOLEAN)
LANGUAGE plpgsql AS $$
DECLARE
    v_status     TEXT;
    v_fp         JSONB;
    v_decision   JSONB;
    v_expires    TIMESTAMPTZ;
    v_i          INT;
    v_ten        TEXT; v_sub TEXT; v_act TEXT;
    v_cap        NUMERIC;
    v_rate       NUMERIC;
    v_tokens     NUMERIC;
    v_updated    TIMESTAMPTZ;
    v_now        TIMESTAMPTZ := now();
    v_avail      NUMERIC;
    v_wait       DOUBLE PRECISION;
    v_retryable  BOOLEAN;
    v_avail_arr  JSONB[] := ARRAY[]::JSONB[];
    v_short_arr  JSONB[] := ARRAY[]::JSONB[];
    v_max_wait   DOUBLE PRECISION := 0;
    v_any_impossible BOOLEAN := FALSE;
    v_keys       TEXT[] := ARRAY[
        ARRAY[p_tenant,'',''],
        ARRAY[p_tenant,p_subject,''],
        ARRAY[p_tenant,p_subject,p_action]
    ];
    v_levels     TEXT[] := ARRAY['tenant','subject','action'];
BEGIN
    -- 1) Claim the request id. Concurrent inserts serialize here; the
    --    loser reads the winner's row and replays its decision.
    INSERT INTO quota_reservations
        (request_id, tenant, subject, action, cost, fingerprint,
         status, decision, expires_at)
    VALUES (p_request_id, p_tenant, p_subject, p_action, p_cost,
            p_fingerprint, 'granted', '{}'::JSONB,
            now() + make_interval(secs => p_ttl_seconds))
    ON CONFLICT (request_id) DO NOTHING;

    SELECT status, fingerprint, decision, expires_at
      INTO v_status, v_fp, v_decision, v_expires
      FROM quota_reservations WHERE request_id = p_request_id;

    IF NOT FOUND THEN
        -- Should not happen after the upsert, but stay explicit.
        RETURN QUERY SELECT jsonb_build_object('outcome','granted') , FALSE;
        RETURN;
    END IF;

    IF v_fp IS DISTINCT FROM p_fingerprint THEN
        RETURN QUERY SELECT jsonb_build_object('outcome','conflict'), FALSE;
        RETURN;
    END IF;

    IF v_status <> 'granted' OR v_decision <> '{}'::JSONB THEN
        -- Previously resolved decision: replay it byte-for-byte.
        RETURN QUERY SELECT v_decision, FALSE;
        RETURN;
    END IF;

    -- 2) Ensure all three bucket rows exist, then lock them in global order.
    FOR v_i IN 1 .. 3 LOOP
        v_ten := v_keys[v_i][1]; v_sub := v_keys[v_i][2]; v_act := v_keys[v_i][3];
        INSERT INTO quota_buckets
            (tenant, subject, action, level, capacity, refill_rate,
             tokens, updated_at)
        VALUES (v_ten, v_sub, v_act,
                CASE WHEN v_act <> '' THEN 'action'
                     WHEN v_sub <> '' THEN 'subject'
                     ELSE 'tenant' END,
                p_def_capacity, p_def_refill, p_def_capacity, v_now)
        ON CONFLICT (tenant, subject, action) DO NOTHING;
    END LOOP;

    FOR v_i IN 1 .. 3 LOOP
        v_ten := v_keys[v_i][1]; v_sub := v_keys[v_i][2]; v_act := v_keys[v_i][3];
        SELECT capacity, refill_rate, tokens, updated_at
          INTO v_cap, v_rate, v_tokens, v_updated
          FROM quota_buckets
         WHERE tenant = v_ten AND subject = v_sub AND action = v_act
         ORDER BY tenant, subject, action
         FOR UPDATE OF quota_buckets;

        v_avail := LEAST(v_cap,
                         v_tokens + v_rate * EXTRACT(EPOCH FROM (v_now - v_updated)));

        v_avail_arr := v_avail_arr || jsonb_build_object(
            'level', v_levels[v_i],
            'tenant', v_ten,
            'subject', NULLIF(v_sub, ''),
            'action', NULLIF(v_act, ''),
            'tokens_available', ROUND(v_avail, 4),
            'capacity', v_cap,
            'refill_rate', v_rate);

        IF v_avail < p_cost THEN
            IF p_cost > v_cap OR v_rate = 0 THEN
                -- Bucket can never hold enough (cost above capacity, or a
                -- non-refilling bucket that is currently short): no finite
                -- retry_after.
                v_retryable := FALSE;
                v_wait := NULL;
                v_any_impossible := TRUE;
            ELSE
                v_retryable := TRUE;
                v_wait := CEIL((p_cost - v_avail) / v_rate)::DOUBLE PRECISION;
                IF v_wait > v_max_wait THEN
                    v_max_wait := v_wait;
                END IF;
            END IF;
            v_short_arr := v_short_arr || jsonb_build_object(
                'level', v_levels[v_i],
                'tokens_available', ROUND(v_avail, 4),
                'required', p_cost,
                'retry_after_seconds', v_wait,
                'retryable', v_retryable);
        END IF;
    END LOOP;

    IF v_short_arr IS DISTINCT FROM ARRAY[]::JSONB[] THEN
        -- 3a) Deny: deduct nothing, persist the decision for replay.
        v_decision := jsonb_build_object(
            'outcome', 'denied',
            'request_id', p_request_id,
            'cost', p_cost,
            'levels', v_short_arr,
            'retry_after_seconds',
                CASE WHEN v_any_impossible THEN NULL ELSE v_max_wait END);

        UPDATE quota_reservations
           SET status = 'denied', decision = v_decision, updated_at = now()
         WHERE request_id = p_request_id;

        RETURN QUERY SELECT v_decision, TRUE;
        RETURN;
    END IF;

    -- 3b) Grant: deduct from all three levels atomically (lazy refill).
    FOR v_i IN 1 .. 3 LOOP
        v_ten := v_keys[v_i][1]; v_sub := v_keys[v_i][2]; v_act := v_keys[v_i][3];
        UPDATE quota_buckets b
           SET tokens = LEAST(capacity,
                       tokens + refill_rate *
                       EXTRACT(EPOCH FROM (now() - updated_at))) - p_cost,
               updated_at = now()
         WHERE tenant = v_ten AND subject = v_sub AND action = v_act;
    END LOOP;

    v_decision := jsonb_build_object(
        'outcome', 'granted',
        'request_id', p_request_id,
        'cost', p_cost,
        'expires_at', v_expires,
        'available', v_avail_arr);

    UPDATE quota_reservations
       SET decision = v_decision, updated_at = now()
     WHERE request_id = p_request_id;

    RETURN QUERY SELECT v_decision, TRUE;
END;
$$;

-- ---------------------------------------------------------------------------
-- quota_commit: commit consumes no tokens.
--   granted   -> committed (idempotent success)
--   committed -> replay
--   otherwise -> 409 terminal state
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION quota_commit(
    p_request_id  TEXT,
    p_fingerprint JSONB
) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
    v_status TEXT;
    v_fp     JSONB;
BEGIN
    SELECT status, fingerprint INTO v_status, v_fp
      FROM quota_reservations WHERE request_id = p_request_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('outcome','not_found');
    END IF;
    IF v_fp IS DISTINCT FROM p_fingerprint THEN
        RETURN jsonb_build_object('outcome','conflict');
    END IF;
    IF v_status = 'committed' THEN
        RETURN jsonb_build_object('outcome','committed','replayed',TRUE);
    END IF;
    IF v_status <> 'granted' THEN
        RETURN jsonb_build_object('outcome', v_status);
    END IF;

    UPDATE quota_reservations
       SET status = 'committed', updated_at = now()
     WHERE request_id = p_request_id;
    RETURN jsonb_build_object('outcome','committed','replayed',FALSE);
END;
$$;

-- Refund cost to the three buckets in the same fixed global order used by
-- reserve, keeping bucket locks ordered to avoid deadlocks. Refund is
-- clamped at capacity and is invoked from exactly one state transition
-- (cancel wins / expiry loses, or vice versa), so it happens at most once.
CREATE OR REPLACE FUNCTION quota_refund_locked(
    p_tenant TEXT, p_subject TEXT, p_action TEXT, p_cost NUMERIC
) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_keys   TEXT[] := ARRAY[
        ARRAY[p_tenant,'',''],
        ARRAY[p_tenant,p_subject,''],
        ARRAY[p_tenant,p_subject,p_action]
    ];
    v_i  INT;
BEGIN
    FOR v_i IN 1 .. 3 LOOP
        UPDATE quota_buckets
           SET tokens = LEAST(capacity,
                       tokens + refill_rate *
                       EXTRACT(EPOCH FROM (now() - updated_at))) + p_cost,
               updated_at = now()
         WHERE tenant = v_keys[v_i][1]
           AND subject = v_keys[v_i][2]
           AND action = v_keys[v_i][3];
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- quota_cancel:
--   granted -> cancelled with a single refund (row lock makes the
--              cancel-vs-reaper race decide exactly one winner)
--   cancelled/expired -> idempotent success, no second refund (a cancel
--              arriving just after timeout returns outcome=expired so the
--              caller knows the reservation was reaped instead)
--   committed/denied  -> 409, no refund
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION quota_cancel(
    p_request_id  TEXT,
    p_fingerprint JSONB
) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
    v_status   TEXT;
    v_fp       JSONB;
    v_tenant   TEXT;
    v_subject  TEXT;
    v_action   TEXT;
    v_cost     NUMERIC;
BEGIN
    SELECT status, fingerprint, tenant, subject, action, cost
      INTO v_status, v_fp, v_tenant, v_subject, v_action, v_cost
      FROM quota_reservations WHERE request_id = p_request_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('outcome','not_found');
    END IF;
    IF v_fp IS DISTINCT FROM p_fingerprint THEN
        RETURN jsonb_build_object('outcome','conflict');
    END IF;

    IF v_status IN ('cancelled','expired') THEN
        -- Idempotent: the refund already happened exactly once (either by
        -- this cancel or by the expiry reaper).
        RETURN jsonb_build_object('outcome', v_status, 'refunded', FALSE,
                                  'replayed', TRUE);
    END IF;
    IF v_status = 'granted' THEN
        -- Refund first (ordered bucket locks), then flip the state.
        PERFORM quota_refund_locked(v_tenant, v_subject, v_action, v_cost);
        UPDATE quota_reservations
           SET status = 'cancelled', expires_at = NULL, updated_at = now()
         WHERE request_id = p_request_id;
        RETURN jsonb_build_object('outcome','cancelled','refunded',TRUE,
                                  'replayed',FALSE);
    END IF;
    RETURN jsonb_build_object('outcome', v_status);
END;
$$;

-- ---------------------------------------------------------------------------
-- quota_recycle_expired: batch reaper. SKIP LOCKED makes several API
-- replicas safe to run concurrently; the post-lock recheck of status and
-- expires_at guarantees an already-cancelled reservation is never refunded.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION quota_recycle_expired(p_batch INT DEFAULT 100)
RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
    v_rec   RECORD;
    v_count INT := 0;
BEGIN
    FOR v_rec IN
        SELECT request_id, tenant, subject, action, cost
          FROM quota_reservations
         WHERE status = 'granted' AND expires_at <= now()
         ORDER BY expires_at
         LIMIT p_batch
         FOR UPDATE SKIP LOCKED
    LOOP
        -- Recheck after acquiring the row lock (cancel may have won).
        PERFORM 1 FROM quota_reservations
          WHERE request_id = v_rec.request_id
            AND status = 'granted' AND expires_at <= now();
        IF FOUND THEN
            PERFORM quota_refund_locked(v_rec.tenant, v_rec.subject,
                                        v_rec.action, v_rec.cost);
            UPDATE quota_reservations
               SET status = 'expired', updated_at = now()
             WHERE request_id = v_rec.request_id;
            v_count := v_count + 1;
        END IF;
    END LOOP;
    RETURN v_count;
END;
$$;

-- ---------------------------------------------------------------------------
-- Policy management. Updating a policy never touches reservations, so it
-- only affects new reserve calls. Token level is lazily refilled first;
-- lowering the capacity clamps current tokens instead of discarding them.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION quota_upsert_policy(
    p_tenant      TEXT,
    p_subject     TEXT,
    p_action      TEXT,
    p_level       TEXT,
    p_capacity    NUMERIC,
    p_refill_rate NUMERIC
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO quota_buckets AS b
        (tenant, subject, action, level, capacity, refill_rate,
         tokens, updated_at)
    VALUES (p_tenant, p_subject, p_action, p_level,
            p_capacity, p_refill_rate, p_capacity, now())
    ON CONFLICT (tenant, subject, action) DO UPDATE
       SET capacity    = EXCLUDED.capacity,
           refill_rate = EXCLUDED.refill_rate,
           tokens      = LEAST(
                            EXCLUDED.capacity,
                            b.tokens + b.refill_rate *
                              EXTRACT(EPOCH FROM (now() - b.updated_at))),
           updated_at  = now();
END;
$$;
