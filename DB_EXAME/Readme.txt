
--##Project Title##

--#Emergency Response Database System

--##Project Description##

--**This Emergency Response Database** tracks incidents, dispatch centers, responders, vehicles, assignments, and reports.
--It enables emergency agencies to coordinate dispatch operations and evaluate response efficiency across multiple districts.

--This project fits CAT-1 because it demonstrates Distributed Database Management concepts, including:

--Horizontal fragmentation of data across multiple nodes.

--Data recombination using foreign data wrappers (FDW).

--Consistency checks through validation queries.

--Use of PostgreSQL distributed architecture (Node_A and Node_B).


--This project fits CAT-1 because it demonstrates Distributed Database Management concepts, including:

--Horizontal fragmentation of data across multiple nodes.

--Data recombination using foreign data wrappers (FDW).

--Consistency checks through validation queries.

--Use of PostgreSQL distributed architecture (Node_A and Node_B).

--#Prerequisites
	
--We used Database PostgreSQL 16, we made Distributed Nodes Node_A and Node_B (two databases)
--postgres_fdw  extansion and DB Link	proj_link enabled on both nodes so that it can be able to communicate
--Public schema on both databases







-- ============================================================
-- CAT-1 PROJECT: Emergency Response Distributed Database
-- SECTION A1 – Fragment & Recombine Main Fact (≤10 rows)
-- ============================================================


-- ============================================================
-- QUESTION 1:
-- Create horizontally fragmented tables Assignment_A on Node_A 
-- and Assignment_B on Node_B using a deterministic rule (HASH or RANGE on a natural key)
-- ============================================================

-- ---------- On Node_A ----------
CREATE DATABASE NODE_A;

-- Connect to NODE_A before running next statements
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create first fragment table for Node_A (assignment_id ≤ 5)
CREATE TABLE Assignment_A (
    assignment_id INT PRIMARY KEY,
    incident_id INT,
    responder_id INT,
    vehicle_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- ---------- On Node_B ----------
CREATE DATABASE NODE_B;

-- Connect to NODE_B before running next statements
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create second fragment table for Node_B (assignment_id > 5)
CREATE TABLE Assignment_B (
    assignment_id INT PRIMARY KEY,
    incident_id INT,
    responder_id INT,
    vehicle_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);
-- Insert fragment data (assignment_id <= 5)
INSERT INTO Assignment_A
SELECT *
FROM Assignment
WHERE assignment_id <= 5;

-- Insert fragment data (assignment_id > 5)
INSERT INTO Assignment_B
SELECT *
FROM Assignment
WHERE assignment_id > 5;
SELECT*FROM Assignment_B

-- ============================================================
-- QUESTION 2:
-- Insert a TOTAL of ≤10 committed rows split across the two fragments 
-- (e.g., 5 on Node_A and 5 on Node_B). Reuse these rows for all remaining tasks.
-- ============================================================

-- ---------- Insert 5 rows into Assignment_A (Node_A fragment) ----------
INSERT INTO Assignment_A (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES
(6, 3, 6, 6, '2025-10-05 09:00:00', '2025-10-05 09:30:00'),
(7, 3, 7, 7, '2025-10-05 10:00:00', '2025-10-05 10:25:00'),
(8, 4, 8, 8, '2025-10-06 13:00:00', '2025-10-06 13:40:00'),
(9, 4, 9, 9, '2025-10-06 15:00:00', '2025-10-06 15:30:00'),
(10, 5, 10, 10, '2025-10-07 08:30:00', '2025-10-07 09:00:00');

-- Verify fragment content on Node_A
SELECT * FROM Assignment_A;


-- ---------- Insert 5 rows into Assignment_B (Node_B fragment) ----------
INSERT INTO Assignment_B (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES

(1, 1, 1, 1, '2025-10-01 08:30:00', '2025-10-01 09:00:00'),
(2, 1, 2, 2, '2025-10-01 09:15:00', '2025-10-01 09:45:00'),
(3, 2, 3, 3, '2025-10-02 10:00:00', '2025-10-02 10:30:00'),
(4, 2, 4, 4, '2025-10-03 14:00:00', '2025-10-03 14:25:00'),
(5, 3, 5, 5, '2025-10-04 17:00:00', '2025-10-04 17:20:00');

-- Verify fragment content on Node_B
SELECT * FROM Assignment_B;



-- ============================================================
-- QUESTION 3:
-- On Node_A, create view Assignment_ALL as UNION ALL of 
-- Assignment_A and Assignment_B@proj_link.
-- ============================================================

-- ---------- Run this section on Node_A ----------

-- Step 1: Create a Foreign Data Wrapper to link Node_B
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Step 2: Create a connection link to Node_B
DROP SERVER IF EXISTS proj_link CASCADE;

CREATE SERVER proj_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',       -- Host name (local machine)
    dbname 'node_b',        -- Target database name
    port '5432'             -- Default PostgreSQL port
);

-- Step 3: Create user mapping (authentication for remote connection)
CREATE USER MAPPING FOR postgres
SERVER proj_link
OPTIONS (
    user 'postgres',
    password 'postgres'
);

-- Step 4: Import remote table Assignment_B from Node_B into Node_A
IMPORT FOREIGN SCHEMA public
LIMIT TO (Assignment_B)
FROM SERVER proj_link
INTO public;

-- -
--------- Run this section on Node_b ----------

-- Step 1: Create a Foreign Data Wrapper to link Node_B
DROP SERVER IF EXISTS Proj_link_1 CASCADE;
CREATE SERVER Proj_link_1
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'node_a',
    port '5432'
);

-- Step 3: Create user mapping
CREATE USER MAPPING FOR postgres
SERVER Proj_link_1
OPTIONS (
    user 'postgres',
    password 'postgres'
);

-------------------------------------------------------

-- Step 5: Create view combining both fragments (UNION ALL)
CREATE OR REPLACE VIEW Assignment_ALL AS
SELECT * FROM Assignment_A
UNION ALL
SELECT * FROM Assignment_B;

-- Step 6: Verify combined view (should show all ≤10 rows)
SELECT * FROM Assignment_ALL;



-- ============================================================
-- QUESTION 4:
-- Validate with COUNT(*) and a checksum on a key column 
-- (e.g., SUM(MOD(primary_key,97))) – results must match fragments vs Assignment_ALL.
-- ============================================================

-- Compare record counts for each fragment and unified view
SELECT
    (SELECT COUNT(*) FROM Assignment_A) AS count_A,
    (SELECT COUNT(*) FROM Assignment_B) AS count_B,
    (SELECT COUNT(*) FROM Assignment_ALL) AS count_ALL;

-- Checksum validation using MOD(assignment_id,97)
SELECT
    (SELECT SUM(MOD(assignment_id,97)) FROM Assignment_A) AS checksum_A,
    (SELECT SUM(MOD(assignment_id,97)) FROM Assignment_B) AS checksum_B,
    (SELECT SUM(MOD(assignment_id,97)) FROM Assignment_ALL) AS checksum_ALL;



-- SECTION A2 – Database Link & Cross-Node Join (3–10 rows result)
-- This section demonstrates cross-node querying using FDW (postgres_fdw)
-- ============================================================


-- ============================================================
-- QUESTION 1:
-- From Node_A, create database link 'proj_link' to Node_B.
--- Link between node was created above in section A2

-- ============================================================
-- QUESTION 2:
-- Run remote SELECT on Incident@proj_link showing up to 5 sample rows.
-- ============================================================

-- This query runs against the remote Assignment_B table on Node_B.
-- The data is fetched across the FDW connection.
SELECT * FROM Assignment_B;
LIMIT 5;   -- limit to ≤ 5 rows to respect result budget


-- QUESTION 3:
-- Run a distributed join: local Assignment_A (or base Assignment) 
-- joined with remote Assignment_B @proj_link returning 3–10 rows total.
-- ============================================================

-- Here, Node_A will join its local Assignment_A table with
-- the remote Assignment_B table from Node_B via the foreign data wrapper.
-- This demonstrates distributed query processing across nodes.
SELECT 
    a.assignment_id AS assign_a_id,
    a.incident_id AS incident_a,
    b.assignment_id AS assign_b_id,
    b.incident_id AS incident_b
FROM 
    Assignment_A  a
JOIN 
    Assignment_B  b
ON 
    a.incident_id = b.incident_id
WHERE 
    a.assignment_id <= 5
LIMIT 7;


-- SECTION A3 – Parallel vs Serial Aggregation (≤10 rows data)



-- ============================================================
-- QUESTION 1:
-- Run a SERIAL aggregation on Assignment_ALL over the small dataset
-- (e.g., totals by a domain column). Ensure result has 3–10 groups/rows.
-- ============================================================

-- Perform a normal (serial) aggregation to calculate the number of
-- assignments handled per responder using the UNION ALL view Assignment_ALL.

-- The view Assignment_ALL combines Assignment_A (Node_A) and
-- Assignment_B@proj_link (Node_B) horizontally fragmented data.

-- SERIAL AGGREGATION QUERY
SELECT responder_id,
       COUNT(*) AS total_assignments
FROM Assignment_ALL
GROUP BY responder_id
ORDER BY responder_id;


-- ============================================================
-- QUESTION 2:
-- Run the same aggregation with PARALLEL hints to force a parallel plan
-- despite small size.
-- ============================================================

-- Force PostgreSQL to parallelize aggregation across fragments Assignment_A
-- and Assignment_B to demonstrate parallel query execution behavior.

-- Enable and tune parallel execution parameters with 8 workers
SET max_parallel_workers_per_gather = 8;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;


SELECT responder_id,
       COUNT(*) AS total_assignments
FROM Assignment_ALL
GROUP BY responder_id
ORDER BY responder_id;




-- ============================================================
-- QUESTION 3:
-- Capture execution plans with DBMS_XPLAN and show AUTOTRACE statistics;
-- timings may be similar due to small data.
-- ============================================================

-- PostgreSQL equivalent: EXPLAIN ANALYZE shows the same details as AUTOTRACE
-- It provides:
--   • Actual time taken
--   • Rows processed at each stage
--   • Parallel workers if used
--   • Estimated cost


-- Use the command below for a cleaner summary of plan and timing
--when parallel excution enabled and desabled  the time will be deferent
EXPLAIN (ANALYZE, SUMMARY)
SELECT responder_id,
       COUNT(*) AS total_assignments
FROM Assignment_ALL
GROUP BY responder_id;


-- ============================================================
--QUESTION 4:
-- Produce a 2-row comparison table (serial vs parallel) with plan notes.
-- ============================================================

-- Summarize the difference between serial and parallel execution plans
-- based on EXPLAIN ANALYZE output and runtime statistics.


-- +------------------+-----------------------+--------------------------------+
-- | Execution Mode   | Total Execution Time  | Plan Notes                     |
-- +------------------+-----------------------+--------------------------------+
-- | Serial           | ~3.514 ms               | Sequential Scan, Aggregate      |
-- | Parallel (8x)    | ~206.511 ms               | Parallel Seq Scan + Gather      |
-- +------------------+-----------------------+--------------------------------+

-- - Timing difference small due to ≤10 rows dataset.
-- - Demonstrates that PostgreSQL parallelism works even on distributed views.

-- SECTION A4 – Two-Phase Commit & Recovery (2 rows)
-- ============================================================
-- This section demonstrates a manual two-phase commit (2PC)
-- simulation using PL/pgSQL to coordinate inserts across
-- NODE_A (local) and NODE_B (remote via FDW).
-- ============================================================

-- QUESTION 1:
-- Write one PL/SQL block that inserts ONE local row (related to Assignment)
-- on Node_A and ONE remote row into Report
-- then COMMIT.
-- ============================================================
-- Purpose:
-- Demonstrate distributed transaction control (two-phase commit)
-- across Node_A and Node_B using postgres_fdw.
-- PostgreSQL handles 2PC automatically for distributed transactions
-- involving foreign tables.


DO
$$
BEGIN
    -- Step 1: Insert one local record into Assignment_A (Node_A)
    INSERT INTO Assignment_A (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
    VALUES (101, 3, 4, 4, '2025-10-06 10:00:00', '2025-10-06 11:00:00');

    -- Step 2: Insert one remote record into Report(Node_B)
    INSERT INTO Report(report_id, assignment_id, resolution, duration,outcome)
    VALUES (201, 101, 'Resolved successfully', INTERVAL '30 minutes', 'Success');

    -- Step 3: Commit both inserts together (two-phase commit)
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- QUESTION 2:
-- Induce a failure in a second run (e.g., disable the link between inserts)
-- to create an in-doubt transaction; ensure any extra test rows are
-- ROLLED BACK to keep within the ≤10 committed row budget.
-- ============================================================

-- Purpose:
-- Simulate a partial failure during distributed commit to generate
-- an in-doubt transaction scenario.



DO
$$
DECLARE
    success_a BOOLEAN := FALSE;  -- Tracks local insert success (Node_A)
    success_b BOOLEAN := FALSE;  -- Tracks remote insert success (Node_B)
BEGIN
    RAISE NOTICE '--- Starting Two-Phase Commit Simulation ---';

    -- ============================================================
    -- Phase 1: Attempt Local Insert on NODE_A
    -- ============================================================
    BEGIN
        -- Insert one record into Assignment_A (local fragment)
        INSERT INTO Assignment_A (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
        VALUES (104, 3, 4, 4, '2025-10-06 10:00:00', '2025-10-06 11:00:00');

        success_a := TRUE;
        RAISE NOTICE 'Local insert (NODE_A) successful.';
    EXCEPTION WHEN OTHERS THEN
        -- Capture failure and continue to next phase
        RAISE NOTICE 'Local insert failed on NODE_A: %', SQLERRM;
        success_a := FALSE;
    END;


    -- ============================================================
    -- Phase 1: Attempt Remote Insert on NODE_B (via FDW)
    -- ============================================================
    BEGIN
        -- Insert one record into remote table (Node_B)
        -- "Yeport" is assumed to be a foreign table mapped to Report on Node_B
        INSERT INTO Yeport (report_id, assignment_id, resolution, duration, outcome)
        VALUES (301, 101, 'Resolved successfully', INTERVAL '30 minutes', 'Success');

        success_b := TRUE;
        RAISE NOTICE 'Remote insert (NODE_B) successful.';
    EXCEPTION WHEN OTHERS THEN
        -- Capture failure for remote node
        RAISE NOTICE 'Remote insert failed on NODE_B: %', SQLERRM;
        success_b := FALSE;
    END;


    -- ============================================================
    -- Phase 2: Decision – Commit or Rollback
    -- ============================================================
    IF success_a AND success_b THEN
        -- Both inserts succeeded; commit both to finalize the transaction
        RAISE NOTICE 'Both inserts succeeded. Committing...';
        COMMIT;
    ELSE
        -- One or more failed; rollback everything to maintain atomicity
        RAISE NOTICE 'One or more inserts failed. Rolling back...';
        ROLLBACK;
    END IF;

    RAISE NOTICE '--- End of Two-Phase Commit Simulation ---';
END;
$$ LANGUAGE plpgsql;

---3. Query DBA_2PC_PENDING; then issue COMMIT FORCE or ROLLBACK FORCE; 
----re-verify consistency on both nodes.


-- STEP 1: Simulate the same on NODE_A (for symmetry)
-- ============================================================

BEGIN;
-- Insert a test record into Assignment_A (Node_A fragment)
INSERT INTO Assignment_A (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES (107, 3, 4, 4, '2025-10-27 09:00:00', '2025-10-27 09:30:00');

-- Prepare this transaction (writes entry to pg_prepared_xacts)
PREPARE TRANSACTION 'tx_recover_A';

-- The transaction is now in a "prepared" state — not committed or rolled back yet
-- Simulate a failure here (e.g., disconnect or system crash)

-- STEP 2: Simulate transaction preparation on NODE_B
-- ============================================================

BEGIN;
-- Insert a test record into Assignment_B (Node_B fragment)
INSERT INTO Assignment_B (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES (105, 3, 4, 4, '2025-10-27 09:00:00', '2025-10-27 09:30:00');

-- Prepare this transaction (this will store it in pg_prepared_xacts)
PREPARE TRANSACTION 'tx_recover_B';

--- Both nodes now hold in-doubt transactions


-- STEP 3: Query pending prepared transactions (same as DBA_2PC_PENDING in Oracle)
-- ============================================================

-- On each node (Node_A and Node_B):
SELECT * FROM pg_prepared_xacts;


-- ============================================================
-- STEP 4: Force COMMIT or ROLLBACK to resolve the in-doubt transactions
-- ============================================================

-- If both nodes’ transactions are valid and consistent:
COMMIT PREPARED 'tx_recover_A';
COMMIT PREPARED 'tx_recover_B';

-- OR, if an error or inconsistency was found during recovery:
-- ROLLBACK PREPARED 'tx_recover_A';
-- ROLLBACK PREPARED 'tx_recover_B';

-- ============================================================
-- STEP 5: Verify consistency on both nodes
-- ============================================================

-- On Node_A:
SELECT * FROM Assignment_A WHERE assignment_id = 107;

-- On Node_B:
SELECT * FROM Assignment_B WHERE assignment_id = 105;

- ============================================================

-- 4. Repeat a clean run to show there are no pending transactions.
-- ============================================================

-- After recovering all in-doubt transactions, we perform one
-- final successful two-phase commit to confirm that:
--   • The system is healthy (no stuck transactions)
--   • No pending entries remain in pg_prepared_xacts
-- ============================================================

-- STEP 1: Verify system is clean before running a new test
SELECT * FROM pg_prepared_xacts;



-- ============================================================

-- SECTION A5 – Distributed Lock Conflict & Diagnosis (No Extra Rows)
-- ============================================================
- ============================================================
-- STEP 1: Open SESSION 1 on NODE_A
-- ============================================================
-- Start a transaction and update one row in Assignment_A.
-- Keep this transaction open (do not commit) to hold the lock.

BEGIN;

UPDATE Assignment_A
SET incident_id = 30
WHERE assignment_id =1;

-- Do NOT COMMIT or ROLLBACK yet.
-- This keeps a row-level lock active on assignment_id =1.
-- ============================================================

-- ============================================================
-- STEP 2: Open SESSION 2 on NODE_B
-- ============================================================
-- In another SQL client window connected to NODE_B,
-- attempt to update the same logical row via the database link.

UPDATE Assignment_A@proj_link
SET incident_id = 31
WHERE assignment_id =1;
---   This UPDATE will hang (blocked) because the row is locked by Session 1.
-- ============================================================
-- STEP 3: Diagnose the lock from NODE_A
-- ============================================================
-- While Session 2 is waiting, use this query on Node_A to identify
-- which sessions are blocking or waiting for locks.

SELECT
    bl.pid       AS blocked_pid,
    a.usename    AS blocked_user,
    kl.pid       AS blocking_pid,
    ka.usename   AS blocking_user,
    a.query      AS blocked_query,
    ka.query     AS blocking_query
FROM pg_catalog.pg_locks bl
JOIN pg_catalog.pg_stat_activity a
  ON a.pid = bl.pid
JOIN pg_catalog.pg_locks kl
  ON bl.transactionid = kl.transactionid AND bl.pid <> kl.pid
JOIN pg_catalog.pg_stat_activity ka
  ON ka.pid = kl.pid
WHERE NOT bl.granted;



-- ============================================================
-- STEP 4: Release the lock from SESSION 1
-- ============================================================
-- Go back to Session 1 (Node_A) and commit the transaction
-- to release the lock and allow Session 2 to continue.

COMMIT;

-- Once committed, Session 2 (Node_B) automatically completes its UPDATE.

-- SECTION B6 – Declarative Rules Hardening (≤10 committed rows)
-- ============================================================

--   Add and verify NOT NULL and domain CHECK constraints for Incident and Report tables.
--   Demonstrate validation via failing and passing inserts, keeping total committed rows ≤10.
-- ============================================================


-- ============================================================
---1. On tables Incident and Report, add/verify NOT NULL and 
---domain CHECK constraints suitable for response durations and outcomes (e.g., positive amounts, valid statuses, date order).
-- ============================================================

-- Ensure Incident table has domain constraints
ALTER TABLE Incident
    ALTER COLUMN incident_type SET NOT NULL,
    ALTER COLUMN location SET NOT NULL,
    ALTER COLUMN district SET NOT NULL,
    ALTER COLUMN severity SET NOT NULL,
    ADD CONSTRAINT chk_incident_severity CHECK (severity IN ('Low', 'Medium', 'High'));

-- Ensure Report table has domain constraints
ALTER TABLE Report
    ALTER COLUMN assignment_id SET NOT NULL,
    ALTER COLUMN outcome SET NOT NULL,
    ADD CONSTRAINT chk_report_outcome CHECK (outcome IN ('Success','Failure','Partial')),
    ADD CONSTRAINT chk_duration_positive CHECK (EXTRACT(EPOCH FROM duration) > 0);
-- ============================================================
-- 2. Prepare 2 failing and 2 passing INSERTs per table to validate 
---rules, but wrap failing ones in a block and ROLLBACK so committed 
---rows stay within ≤10 total.
-- ============================================================

-- Use a DO block to wrap failing inserts and handle errors cleanly
DO $$
BEGIN
    -- -----------------------------
    -- FAILING INSERTS for Incident
    -- -----------------------------
    BEGIN
        INSERT INTO Incident (incident_id, incident_type, location, district, severity, reported_time)
        VALUES (20, NULL, 'Kigali', 'Kigali', 'High', NOW()); -- NULL incident_type fails
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Expected failure (Incident): %', SQLERRM;
    END;

    BEGIN
        INSERT INTO Incident (incident_id, incident_type, location, district, severity, reported_time)
        VALUES (21, 'Fire', 'Kigali', 'Kigali', 'Critical', NOW()); -- Invalid severity
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Expected failure (Incident): %', SQLERRM;
    END;

    -- -----------------------------
    -- PASSING INSERTS for Incident
    -- -----------------------------
    INSERT INTO Incident (incident_id, incident_type, location, district, severity, reported_time)
    VALUES (22, 'Flood', 'Rwamagana', 'Eastern', 'High', NOW());

    INSERT INTO Incident (incident_id, incident_type, location, district, severity, reported_time)
    VALUES (23, 'Medical Emergency', 'Rubavu', 'Western', 'Low', NOW());


    -- -----------------------------
    -- FAILING INSERTS for Report
    -- -----------------------------
    BEGIN
        INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
        VALUES (50, NULL, 'Resolved', INTERVAL '30 minutes', 'Success'); -- NULL assignment_id
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Expected failure (Report): %', SQLERRM;
    END;

    BEGIN
        INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
        VALUES (51, 101, 'Resolved', INTERVAL '-30 minutes', 'Success'); -- Negative duration
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Expected failure (Report): %', SQLERRM;
    END;

    -- -----------------------------
    -- PASSING INSERTS for Report
    -- -----------------------------
    INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
    VALUES (52, 101, 'Resolved successfully', INTERVAL '25 minutes', 'Success');

    INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
    VALUES (53, 102, 'Partial resolution', INTERVAL '40 minutes', 'Partial');

END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- Show clean error handling for failing cases.
---Verify Data & Constraints
-- ============================================================

-- Incident table should have 2 new rows committed (IDs 22,23)
SELECT incident_id, incident_type, severity FROM Incident
WHERE incident_id IN (22,23);

-- Report table should have 2 new rows committed (IDs 52,53)
SELECT report_id, assignment_id, outcome, duration FROM Report
WHERE report_id IN (52,53);




-- ============================================================

-- SECTION B7 – E–C–A Trigger for Denormalized Totals
-- ============================================================
--   Maintain denormalized totals of Reports per Incident.
--   Log before/after totals into an audit table.
--   Only a small number of DML rows (≤4) are affected.
-- ============================================================


-- ============================================================
-- 1. Create an audit table Incident_AUDIT(bef_total NUMBER, 
---aft_total NUMBER, changed_at TIMESTAMP, key_col VARCHAR2(64)).
-- ============================================================
CREATE TABLE IF NOT EXISTS Incident_AUDIT (
    bef_total  INTEGER,
    aft_total  INTEGER,
    changed_at TIMESTAMP DEFAULT NOW(),
    key_col    VARCHAR(64)
);


-- ============================================================
----2. Implement a statement-level AFTER INSERT/UPDATE/DELETE trigger on
----Report that recomputes denormalized totals in Incident once per statement.
-- ============================================================
-- This is a statement-level trigger on Report table.
-- It recomputes the total number of reports per Incident
-- and logs before/after counts into Incident_AUDIT.

CREATE OR REPLACE FUNCTION update_incident_totals()
RETURNS TRIGGER AS $$
DECLARE
    rec RECORD;
    old_count INTEGER;
    new_count INTEGER;
BEGIN
    -- Loop over affected incidents
    FOR rec IN
        SELECT DISTINCT assignment_id FROM Report
    LOOP
        -- Fetch current total reports before update
        SELECT COUNT(*) INTO old_count FROM Report
        WHERE assignment_id = rec.assignment_id;

        -- Update Incident total (denormalized column)
        -- Add column first if missing
        BEGIN
            ALTER TABLE Incident ADD COLUMN IF NOT EXISTS total_reports INTEGER DEFAULT 0;
        EXCEPTION WHEN duplicate_column THEN
            NULL; -- column already exists
        END;

        -- Compute new total
        SELECT COUNT(*) INTO new_count
        FROM Report
        WHERE assignment_id = rec.assignment_id;

        -- Update Incident
        UPDATE Incident
        SET total_reports = new_count
        WHERE incident_id = rec.assignment_id;

        -- Log to audit table
        INSERT INTO Incident_AUDIT (bef_total, aft_total, key_col)
        VALUES (old_count, new_count, rec.assignment_id::TEXT);
    END LOOP;

    RETURN NULL; -- statement-level trigger returns null
END;
$$ LANGUAGE plpgsql;


-- Attach trigger to Report table
DROP TRIGGER IF EXISTS trg_update_incident_totals ON Report;

CREATE TRIGGER trg_update_incident_totals
AFTER INSERT OR UPDATE OR DELETE ON Report
FOR EACH STATEMENT
EXECUTE FUNCTION update_incident_totals();


-- ============================================================
-- 3. Execute a small mixed DML script on CHILD affecting 
----at most 4 rows in total; ensure net committed rows across the project remain ≤10.
-- ============================================================

-- Insert 2 new reports (passing)
INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
VALUES (54, 102, 'Resolved', INTERVAL '20 minutes', 'Success');

INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
VALUES (55, 103, 'Partial Resolution', INTERVAL '15 minutes', 'Partial');

-- Update 2 existing reports
UPDATE Report
SET resolution = 'Updated Resolution'
WHERE report_id IN (52,53);


-- ============================================================
-- 4. Log before/after totals to the audit table (2–3 audit rows).
-- ============================================================
SELECT * FROM Incident_AUDIT
ORDER BY changed_at DESC;

-- Check Incident totals
SELECT incident_id, total_reports FROM Incident
WHERE incident_id IN (101,102,103);



-- SECTION B8 – Recursive Hierarchy Roll-Up
-- ============================================================

-- ============================================================
-- 1. Create table HIER(parent_id, child_id)
---for a natural hierarchy (domain-specific).
-- ============================================================
CREATE TABLE IF NOT EXISTS HIER (
    parent_id INT NOT NULL,
    child_id  INT NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);


-- ============================================================
-- 2. Insert 6–10 rows forming a 3-level hierarchy.
-- ============================================================

-- Example: Incident assignments by district > center > responder
INSERT INTO HIER (parent_id, child_id) VALUES
(1, 2),  -- District 1 -> Center 2
(1, 3),  -- District 1 -> Center 3
(2, 4),  -- Center 2 -> Responder 4
(2, 5),  -- Center 2 -> Responder 5
(3, 6),  -- Center 3 -> Responder 6
(3, 7);  -- Center 3 -> Responder 7

3. Write a recursive WITH query to produce (child_id, root_id, depth) 
---and join to Assignment or its parent to compute rollups; return 6–10 rows total.
-- ============================================================
--Recursive Query to Produce child_id, root_id, depth
-- ============================================================

WITH RECURSIVE hier_rollup AS (
    -- Base level: every child points to its immediate parent
    SELECT
        child_id,
        parent_id AS root_id,
        1 AS depth
    FROM HIER

    UNION ALL

    -- Recursive step: climb up the hierarchy to find ultimate root
    SELECT
        h.child_id,
        r.root_id,
        r.depth + 1
    FROM HIER h
    JOIN hier_rollup r ON h.parent_id = r.child_id
)
SELECT *
FROM hier_rollup
ORDER BY root_id, depth;

---4. Reuse existing seed rows; do not exceed the ≤10 committed rows budget.
-- ============================================================
-- Join to Assignment Table for Rollups
-- ============================================================

-- Example: Aggregate total assignments per root entity
WITH RECURSIVE hier_rollup AS (
    SELECT child_id, parent_id AS root_id, 1 AS depth
    FROM HIER
    UNION ALL
    SELECT h.child_id, r.root_id, r.depth + 1
    FROM HIER h
    JOIN hier_rollup r ON h.parent_id = r.child_id
)
SELECT
    r.root_id,
    COUNT(a.assignment_id) AS total_assignments,
    MAX(r.depth) AS max_depth
FROM hier_rollup r
LEFT JOIN Assignment_A a ON a.responder_id = r.child_id
GROUP BY r.root_id
ORDER BY r.root_id;


-- ============================================================
-- SECTION B9 – Mini-Knowledge Base with Transitive Inference
-- ============================================================
---1. Create table TRIPLE(s VARCHAR2(64), p VARCHAR2(64), o VARCHAR2(64)).
-- ============================================================
-- STEP 1: Create TRIPLE Table
-- ============================================================
CREATE TABLE IF NOT EXISTS TRIPLE (
    s VARCHAR(64), -- Subject
    p VARCHAR(64), -- Predicate
    o VARCHAR(64)  -- Object
);


--2. Insert 8–10 domain facts relevant to your project (e.g., simple type hierarchy or rule implications).

-- ============================================================
-- STEP 2: Insert 8–10 Domain Facts
-- ============================================================

-- Example: Type hierarchy for vehicles and responders
INSERT INTO TRIPLE (s,p,o) VALUES
('Ambulance', 'isA', 'Vehicle'),
('FireTruck', 'isA', 'Vehicle'),
('PoliceCar', 'isA', 'Vehicle'),
('RescueVan', 'isA', 'Vehicle'),
('Paramedic', 'isA', 'Responder'),
('Firefighter', 'isA', 'Responder'),
('PoliceOfficer', 'isA', 'Responder'),
('RescueTeamLeader', 'isA', 'Responder');



--3. Write a recursive inference query implementing transitive isA*; 
--apply labels to base records and return up to 10 labeled rows.
-- ============================================================
-- STEP 3: Recursive Query for Transitive isA*
-- ============================================================

WITH RECURSIVE isa_closure(subject, object, depth) AS (
    -- Base case: direct 'isA' relations
    SELECT s, o, 1
    FROM TRIPLE
    WHERE p = 'isA'

    UNION ALL

    -- Recursive step: transitive closure
    SELECT t.s, c.object, c.depth + 1
    FROM TRIPLE t
    JOIN isa_closure c ON t.o = c.subject
    WHERE t.p = 'isA'
)
SELECT *
FROM isa_closure
ORDER BY subject, depth
LIMIT 10;  -- keep output ≤10 rows

-- ============================================================
-- B10: Business Limit Alert (Function + Trigger) – Row-budget safe
-- ============================================================
--   Enforce declarative business rules on Report/Incident tables.
--   Raise an error if inserting/updating violates thresholds defined in BUSINESS_LIMITS.
--   Ensure total committed rows across the project remain ≤10.
-- ============================================================

-- ============================================================
-- 1. Create BUSINESS_LIMITS(rule_key VARCHAR2(64), threshold NUMBER, active CHAR(1) CHECK(active IN('Y','N'))) 
---and seed exactly one active rule.
-- ============================================================
-- Stores active rules, thresholds, and status (Y/N)
CREATE TABLE BUSINESS_LIMITS (
    rule_key VARCHAR(64) PRIMARY KEY,     -- unique identifier for the rule
    threshold NUMERIC NOT NULL,           -- numeric threshold value
    active CHAR(1) NOT NULL CHECK (active IN ('Y','N'))  -- rule status
);

-- Seed exactly one active rule for demonstration:
-- MAX_ASSIGNMENTS_PER_RESPONDER = 5
INSERT INTO BUSINESS_LIMITS (rule_key, threshold, active)
VALUES ('MAX_ASSIGNMENTS_PER_RESPONDER', 5, 'Y');

SELECT * FROM BUSINESS_LIMITS; -- Verify seed data

-- ============================================================
-- 2. Implement function fn_should_alert(...) that reads BUSINESS_LIMITS 
--and inspects current data in Report or Incident to decide a violation (return 1/0).
-- ============================================================
-- Purpose: Check current Report/Incident data against active business rules
-- Returns 1 if any rule is violated, 0 otherwise
CREATE OR REPLACE FUNCTION fn_should_alert()
RETURNS INT AS $$
DECLARE
    v_rule_key VARCHAR(64);
    v_threshold NUMERIC;
    v_active CHAR(1);
    v_violation_count INT;
BEGIN
    -- Fetch one active rule
    SELECT rule_key, threshold, active
    INTO v_rule_key, v_threshold, v_active
    FROM BUSINESS_LIMITS
    WHERE active = 'Y'
    LIMIT 1;

    IF v_active = 'Y' THEN
        -- Count responders exceeding threshold reports
        SELECT COUNT(*)
        INTO v_violation_count
        FROM (
            SELECT a.responder_id, COUNT(r.report_id) AS total_reports
            FROM Assignment a
            JOIN Report r ON a.assignment_id = r.assignment_id
            GROUP BY a.responder_id
            HAVING COUNT(r.report_id) > v_threshold
        ) AS violations;

        IF v_violation_count > 0 THEN
            RETURN 1;  -- Violation detected
        ELSE
            RETURN 0;  -- No violation
        END IF;
    ELSE
        RETURN 0;      -- No active rule
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Quick test of the function
SELECT fn_should_alert();

-- ============================================================
-- 3. Create a BEFORE INSERT OR UPDATE trigger on Report (or relevant table)
--that raises an application error when fn_should_alert returns 1.
-- ============================================================
-- Trigger calls fn_should_alert and raises an error if a violation occurs
CREATE OR REPLACE FUNCTION trg_check_business_limits()
RETURNS TRIGGER AS $$
DECLARE
    v_alert INT;
BEGIN
    v_alert := fn_should_alert();  -- Call the business rule function

    IF v_alert = 1 THEN
        RAISE EXCEPTION 'Business rule violation: threshold exceeded in Report/Incident.';
    END IF;

    RETURN NEW; -- Allow operation if no violation
END;
$$ LANGUAGE plpgsql;

-- Add a helper column to Incident for total_reports tracking
ALTER TABLE Incident
ADD COLUMN total_reports INT DEFAULT 0;

-- Attach the trigger to the Report table
CREATE TRIGGER check_business_limits_trigger
BEFORE INSERT OR UPDATE ON Report
FOR EACH ROW
EXECUTE FUNCTION trg_check_business_limits();

-- ============================================================
-- 4. Demonstrate 2 failing and 2 passing DML cases; rollback 
---the failing ones so total committed rows remain within the ≤10 budget.
-- ============================================================

-- ---- Passing cases ----
-- Valid INSERT (assignment_id exists, rule not violated)
INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
VALUES (1, 1, 'Resolved successfully', INTERVAL '30 minutes', 'Success');

-- Valid UPDATE (existing report, still within rule)
UPDATE Report
SET outcome = 'Completed', duration = INTERVAL '40 minutes'
WHERE report_id = 1;

COMMIT;

-- ---- Failing cases ----
-- Invalid INSERT: foreign key violation (assignment_id does not exist)
DO $$
BEGIN
    BEGIN
        INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
        VALUES (99, 999, 'Invalid foreign key', INTERVAL '20 minutes', 'Fail');
    EXCEPTION
        WHEN foreign_key_violation THEN
            RAISE NOTICE 'Foreign key violation — rolling back this insert.';
            ROLLBACK;
    END;
END $$;

-- Invalid INSERT: violates business rule threshold
DO $$
BEGIN
    BEGIN
        INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
        VALUES (100, 1, 'Severe delay', INTERVAL '200 minutes', 'Timeout');
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Trigger alert fired — rolling back this insert.';
            ROLLBACK;
    END;
END $$;

-- ============================================================
-- Verify results
-- ============================================================
-- Only valid inserts/updates remain; total rows ≤10
SELECT * FROM Report;
SELECT * FROM Incident_AUDIT; -- If audit trigger is implemented

