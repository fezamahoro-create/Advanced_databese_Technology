CREATE DATABASE BranchDB_A

CREATE TABLE DispatchCenter (
    center_id INT PRIMARY KEY,  -- Unique ID for each dispatch center
    center_name VARCHAR(100) NOT NULL, -- Name of the dispatch center
	location VARCHAR(100) NOT NULL,    -- Physical location or address
    district VARCHAR(50) NOT NULL,      
	Contact VARCHAR(10) NOT NULL
	
);

CREATE TABLE Vehicle (
    vehicle_id INT PRIMARY KEY,
    plate_no VARCHAR(20) UNIQUE NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) CHECK (status IN ('Available', 'OnDuty', 'Maintenance')) NOT NULL,
    center_id INT,
    FOREIGN KEY (center_id) REFERENCES DispatchCenter(center_id)
);

CREATE TABLE Responder (
    responder_id INT PRIMARY KEY,
    responder_name VARCHAR(100) NOT NULL,
    role VARCHAR(50),
    phone VARCHAR(20),
	Availability VARCHAR(20) CHECK (Availability IN ('Available', 'OnDuty', 'Leave')) NOT NULL,
    center_id INT,
    FOREIGN KEY (center_id) REFERENCES DispatchCenter(center_id)
);

INSERT INTO DispatchCenter (center_id, center_name, location, district, contact)
VALUES
(1, 'Central Dispatch', 'Kigali City', 'Kigali', '0781234567'),
(2, 'Eastern Dispatch', 'Rwamagana Town', 'Eastern', '0782345678'),
(3, 'Western Dispatch', 'Rubavu City', 'Western', '0783456789');

INSERT INTO Responder (responder_id, responder_name, role, phone, availability, center_id)
VALUES
(1, 'Feza Mahoro', 'Paramedic', '0781111111', 'Available', 1),
(2, 'John Mugisha', 'Firefighter', '0782222222', 'OnDuty', 1),
(3, 'Grace Uwimana', 'Police Officer', '0783333333', 'Available', 1),
(4, 'Eric Niyonsenga', 'Paramedic', '0784444444', 'Available', 2),
(5, 'Ange Mukamana', 'Firefighter', '0785555555', 'OnDuty', 2),
(6, 'David Iradukunda', 'Police Officer', '0786666666', 'Leave', 2),
(7, 'Marie Uwera', 'Paramedic', '0787777777', 'Available', 3),
(8, 'Alex Hakizimana', 'Firefighter', '0788888888', 'Available', 3),
(9, 'Sifa Uwase', 'Police Officer', '0789999999', 'OnDuty', 3),
(10, 'Patrick Mugabo', 'Rescue Team Leader', '0781010101', 'Available', 1);

INSERT INTO Vehicle (vehicle_id, plate_no, vehicle_type, status, center_id)
VALUES
(1, 'RAB100A', 'Ambulance', 'OnDuty', 1),
(2, 'RAB101A', 'Fire Truck', 'OnDuty', 1),
(3, 'RAB102A', 'Police Car', 'Available', 1),
(4, 'RAB200B', 'Ambulance', 'Available', 2),
(5, 'RAB201B', 'Fire Truck', 'OnDuty', 2),
(6, 'RAB202B', 'Rescue Van', 'Available', 2),
(7, 'RAB300C', 'Ambulance', 'Available', 3),
(8, 'RAB301C', 'Fire Truck', 'OnDuty', 3),
(9, 'RAB302C', 'Police Car', 'Available', 3),
(10, 'RAB303C', 'Rescue Van', 'Available', 3);


-- Step 1: Enable extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
-- Step 2: Create the server connection to BranchDB_B
DROP SERVER IF EXISTS branch_b_server CASCADE;
CREATE SERVER branch_b_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'branchdb_b',
    port '5432'
);
-- Step 3: Create user mapping
CREATE USER MAPPING FOR postgres
SERVER branch_b_server
OPTIONS (
    user 'postgres',
    password 'postgres'
);
-- Step 4: Import tables from BranchDB_B
IMPORT FOREIGN SCHEMA public
LIMIT TO (Incident, Assignment, Report)
FROM SERVER branch_b_server
INTO public;

----Q3.Enable parallel query execution on a large table (e.g.,Transactions, Orders). Use /*+ PARALLEL(table, 8) */
--hint and compare serial vs parallel performance. Show EXPLAIN PLAN output and execution time.
-- Allow parallel execution

SHOW max_parallel_workers_per_gather;  -- default is 2
SET max_parallel_workers_per_gather = 8;  -- allow up to 8 workers





------------------------
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount NUMERIC
);

-- Insert a lot of dummy data
INSERT INTO orders(customer_id, order_date, amount)
SELECT
    (RANDOM()*1000)::INT,
    DATE '2024-01-01' + (RANDOM()*365)::INT,
    RANDOM()*1000
FROM generate_series(1, 1000000);  -- 1 million rows



SET max_parallel_workers_per_gather = 0;  -- force serial

EXPLAIN ANALYZE
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id;

SET max_parallel_workers_per_gather = 8;  -- allow parallel
EXPLAIN ANALYZE
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id;


----
 SELECT*FROM orders
--Q4.Write a PL/SQL block performing inserts on both nodes and committing once. Verify atomicity using
--DBA_2PC_PENDING. Provide SQL code and explanation of results.

SHOW max_prepared_transactions;

SELECT * FROM Assignment;
SELECT foreign_table_schema, foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables;

BEGIN;
INSERT INTO Dispatchcenter(center_id, center_name, location, contact, district)
VALUES (100, 'SimuCenter', 'Rwanda East', '0788888810', 'East');
INSERT INTO responder (responder_id, responder_name, role, phone, availability, center_id)
VALUES(34, 'Ange Mahoro', 'Paramedic', '0781111117', 'Available', 1);

PREPARE TRANSACTION 'tx_demo_e';

ROLLBACK PREPARED 'tx_demo_e';


--Q4.Write a PL/SQL block performing inserts on both nodes
---and committing once. Verify atomicity using
--DBA_2PC_PENDING. Provide SQL code and explanation
---of results.

DO
$$
DECLARE
    success_a BOOLEAN := FALSE;
    success_b BOOLEAN := FALSE;
BEGIN
    RAISE NOTICE '--- Starting Two-Phase Commit Simulation ---';

    -- Phase 1: Try local insert (Branch A)
    BEGIN
        INSERT INTO DispatchCenter(center_id, center_name, location, contact, district)
        VALUES (202, 'Simu Center', 'Kigali City','0788888810', 'East');
        success_a := TRUE;
        RAISE NOTICE 'Local insert (Branch A) successful.';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Local insert failed on Branch A: %', SQLERRM;
        success_a := FALSE;
    END;

    -- Phase 1: Try remote insert (Branch B via FDW)
    BEGIN
        INSERT INTO Incident(incident_id, incident_type, location, district, severity)
        VALUES (602, 'Earthquake', 'South District','East', 'Low');
        success_b := TRUE;
        RAISE NOTICE 'Remote insert (Branch B) successful.';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Remote insert failed on Branch B: %', SQLERRM;
        success_b := FALSE;
    END;

    -- Phase 2: Commit or Rollback both
    IF success_a AND success_b THEN
        RAISE NOTICE 'Both inserts succeeded. Committing...';
        COMMIT;
    ELSE
        RAISE NOTICE 'One or more inserts failed. Rolling back...';
        ROLLBACK;
    END IF;

    RAISE NOTICE '--- End of Two-Phase Commit Simulation ---';
END;
$$ LANGUAGE plpgsql;

select *from DispatchCenter

--Q5.Simulate a network failure during a distributed transaction. Check unresolved transactions and resolve
--them using ROLLBACK FORCE. Submit screenshots and brief explanation of recovery steps.
--SHOW max_prepared_transactions;

SELECT * FROM Assignment;

 --ON NODE 1--
INSERT INTO Responder (responder_id, responder_name, role, phone, availability, center_id)
VALUES(34, 'Ange Mahoro', 'Paramedic', '0781111117', 'Available', 1);
PREPARE TRANSACTION 'tx_demo_e';

ROLLBACK PREPARED 'tx_demo_e';



----Q6Demonstrate a lock conflict by running two sessions that
---update the same record from different nodes. Query
--DBA_LOCKS and interpret results.



BEGIN;

-- Lock one record by updating it (do not commit)
UPDATE DispatchCenter
SET contact = '0789999910'
WHERE center_id = 2;

COMMIT;


-- Keep the transaction open (don’t COMMIT or ROLLBACK yet)

---Q7.Perform parallel data aggregation or loading using
--PARALLEL DML. Compare runtime and document
--improvement in query cost and execution time.

-- ================================================
-- Q7: Parallel Data Loading / ETL Simulation
-- ================================================
-- Objective: Perform data aggregation in serial and parallel modes
--             and compare execution performance.
-- ================================================

-- Step 1: Create a test table
DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
    trans_id BIGSERIAL PRIMARY KEY,
    amount NUMERIC(10,2),
    category VARCHAR(20),
    trans_date DATE
);

-- Step 2: Load large random data (simulate ETL loading)
-- This will insert 100,000 rows
INSERT INTO transactions (amount, category, trans_date)
SELECT
    (random() * 1000)::NUMERIC(10,2),
    (ARRAY['Fuel','Medical','Rescue','Fire','Maintenance'])[ceil(random() * 5)],
    CURRENT_DATE - (random() * 365)::INT
FROM generate_series(1, 100000);

-- Confirm data loaded
SELECT COUNT(*) AS total_records FROM transactions;

-- ================================================
-- Step 3: SERIAL Execution (No parallelism)
-- ================================================
-- Disable parallel workers
SET max_parallel_workers_per_gather = 0;

-- Analyze query plan and time
EXPLAIN ANALYZE
SELECT category, SUM(amount)
FROM transactions
GROUP BY category;

-- ================================================

-- Step 4: PARALLEL Execution
-- Enable parallel workers
SET max_parallel_workers_per_gather = 8;
-- Force planner to use parallel workers (optional tuning)
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

-- Analyze query plan and time again
EXPLAIN ANALYZE
SELECT category, SUM(amount)
FROM transactions
GROUP BY category;

-- ================================================
-- Step 5: Show difference clearly
-- ================================================
-- You can record the "Execution Time" and "Total Cost" manually
-- from the EXPLAIN ANALYZE output for your report table like:
-- | Mode    | Workers | Query Cost | Execution Time (ms) |
-- |----------|----------|-------------|---------------------|
-- | Serial   | 0        | e.g. 15000 | e.g. 480 ms         |
-- | Parallel | 8        | e.g. 8000  | e.g. 220 ms         |

-- ================================================
-- Step 6: Clean up (Optional)
-- ================================================
-- DROP TABLE transactions;

----Q9 Distributed Query Optimization Use EXPLAIN PLAN and DBMS_XPLAN.DISPLAY 
--to analyze a distributed join. Discuss optimizer strategy and how data movement is minimized.

EXPLAIN ANALYZE
SELECT r.responder_name, i.incident_type, a.start_time, a.end_time
FROM Responder r
JOIN Assignment a ON r.responder_id = a.responder_id
JOIN Incident i ON a.incident_id = i.incident_id
WHERE i.severity = 'High';

--Q10. Run one complex query three ways – centralized,
--parallel, distributed. Measure time and I/O using
--AUTOTRACE. Write a half-page analysis on scalability
--and efficiency.

-- Centralized Query
--timing on
EXPLAIN (ANALYZE, BUFFERS)
SELECT dc.center_name, COUNT(i.incident_id) AS total_incidents
FROM DispatchCenter dc
JOIN Incident i ON dc.center_id = i.center_id
WHERE i.severity = 'High'
GROUP BY dc.center_name
ORDER BY total_incidents DESC;



-- CENTRALIZED QUERY (BranchDB_A only)
--timing on
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT dc.center_name,
       COUNT(r.responder_id) AS total_responders
FROM DispatchCenter dc
JOIN Responder r ON dc.center_id = r.center_id
GROUP BY dc.center_name
ORDER BY total_responders DESC;






