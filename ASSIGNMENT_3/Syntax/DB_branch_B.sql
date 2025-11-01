CREATE DATABASE BranchDB_B
CREATE TABLE Incident (
    incident_id INT PRIMARY KEY,
    incident_type VARCHAR(50),
    location VARCHAR(100),
    district VARCHAR(50),
    severity VARCHAR(20) CHECK (severity IN ('Low', 'Medium', 'High')),
    reported_time TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('Pending', 'Resolved')) DEFAULT 'Pending'
);

CREATE TABLE Assignment (
    assignment_id INT PRIMARY KEY,
    incident_id INT,
    responder_id INT,
    vehicle_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

CREATE TABLE Report (
    report_id INT PRIMARY KEY,
    assignment_id INT,
    Resolution TEXT,
    Duration INTERVAL,
    outcome VARCHAR(100)
);

INSERT INTO Incident (incident_id, incident_type, location, district, severity, reported_time, status)
VALUES
(1, 'Fire Outbreak', 'Nyamirambo', 'Kigali', 'High', '2025-10-01 08:30:00', 'Resolved'),
(2, 'Road Accident', 'Remera', 'Kigali', 'Medium', '2025-10-02 10:00:00', 'Resolved'),
(3, 'Flood', 'Gatsibo', 'Eastern', 'High', '2025-10-03 14:20:00', 'Pending'),
(4, 'Medical Emergency', 'Rwamagana', 'Eastern', 'Low', '2025-10-03 18:40:00', 'Resolved'),
(5, 'Building Collapse', 'Rubavu', 'Western', 'High', '2025-10-04 09:00:00', 'Resolved'),
(6, 'Traffic Accident', 'Musanze', 'Western', 'Medium', '2025-10-05 11:10:00', 'Resolved');

INSERT INTO Assignment (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES
(1, 1, 1, 1, '2025-10-01 08:35:00', '2025-10-01 09:00:00'),
(2, 1, 2, 2, '2025-10-01 08:40:00', '2025-10-01 09:15:00'),
(3, 2, 3, 3, '2025-10-02 10:10:00', '2025-10-02 10:45:00'),
(4, 3, 4, 4, '2025-10-03 14:25:00', '2025-10-03 15:00:00'),
(5, 4, 5, 5, '2025-10-03 18:45:00', '2025-10-03 19:05:00'),
(6, 5, 7, 6, '2025-10-04 09:05:00', '2025-10-04 09:50:00'),
(7, 5, 8, 7, '2025-10-04 09:10:00', '2025-10-04 09:40:00'),
(8, 6, 9, 8, '2025-10-05 11:15:00', '2025-10-05 11:55:00'),
(9, 2, 10, 9, '2025-10-02 10:05:00', '2025-10-02 10:25:00'),
(10, 4, 6, 10, '2025-10-03 18:50:00', '2025-10-03 19:25:00');

INSERT INTO Report (report_id, assignment_id, resolution, duration, outcome)
VALUES
(1, 1, 'Fire extinguished successfully', INTERVAL '1 hour 15 minutes', 'Resolved'),
(2, 2, 'Medical aid provided to injured person', INTERVAL '45 minutes', 'Resolved'),
(3, 3, 'Traffic accident cleared', INTERVAL '1 hour 30 minutes', 'Resolved'),
(4, 4, 'Flood area evacuated', INTERVAL '3 hours', 'Resolved'),
(5, 5, 'Power line fixed after storm', INTERVAL '2 hours', 'Resolved'),
(6, 6, 'Rescued trapped passengers', INTERVAL '1 hour 45 minutes', 'Resolved'),
(7, 7, 'False alarm, no action needed', INTERVAL '20 minutes', 'Closed'),
(8, 8, 'Chemical spill contained', INTERVAL '2 hours 20 minutes', 'Resolved'),
(9, 9, 'Fire under control, monitoring continues', INTERVAL '2 hours 10 minutes', 'Ongoing'),
(10, 10, 'Assisted police with roadblock', INTERVAL '1 hour', 'Completed');
-- Step 1: Enable FDW
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Step 2: Create the server connection to BranchDB_A
DROP SERVER IF EXISTS branch_a_server CASCADE;
CREATE SERVER branch_a_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'branchdb_a',
    port '5432'
);

-- Step 3: Create user mapping
CREATE USER MAPPING FOR postgres
SERVER branch_a_server
OPTIONS (
    user 'postgres',
    password 'postgres'
);

-- Step 4: Import tables from BranchDB_A
IMPORT FOREIGN SCHEMA public
LIMIT TO (Dispatchcenter, Vehicle, Responder)
FROM SERVER branch_a_server
INTO public;


SELECT foreign_table_schema, foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables;

SELECT *
FROM Responder r
JOIN Assignment a ON r.responder_id = a.responder_id
JOIN Incident i ON a.incident_id = i.incident_id;

SELECT * FROM Responder;
drop FOREIGN table  Vehicle
;

SHOW max_prepared_transactions;
BEGIN;
INSERT INTO Incident(incident_id, incident_type, location, district, severity)
VALUES (999, 'SimuIncident', 'East Kigali', 'East', 'Low');
PREPARE TRANSACTION 'tx_demo_b';
SELECT * FROM incident WHERE Incident_id = 999; 

COMMIT PREPARED 'tx_demo_b';

--Q5.Simulate a network failure during a distributed transaction. Check unresolved transactions and resolve
--them using ROLLBACK FORCE. Submit screenshots and brief explanation of recovery steps.
--SHOW max_prepared_transactions;
-- BEGIN and perform local work on node B

BEGIN;
INSERT INTO Incident(incident_id, incident_type, location, district, severity, reported_time, status)
VALUES (9001, 'RecoverySim', 'Rwamagana', 'Eastern', 'High', now(), 'Pending');

-- Prepare the transaction on node B
PREPARE TRANSACTION 'tx_recover_b';
-- After this line you should see: PREPARE TRANSACTION
ROLLBACK PREPARED 'tx_recover_b'


SELECT * FROM pg_prepared_xacts;

----Q6Demonstrate a lock conflict by running two sessions that
---update the same record from different nodes. Query
--DBA_LOCKS and interpret results.

BEGIN;

-- Try to update the same record
UPDATE DispatchCenter
SET contact = '0781111125'
WHERE center_id = 10;

COMMIT;



SELECT
    a.pid AS waiting_pid,
    a.usename AS waiting_user,
    a.query AS waiting_query,
    a.state AS waiting_state,
    l.locktype,
    l.mode AS lock_mode,
    l.granted AS lock_granted,
    t.relname AS table_name,
    b.pid AS blocking_pid,
    b.query AS blocking_query,
    b.usename AS blocking_user
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
LEFT JOIN pg_locks bl ON l.locktype = bl.locktype AND l.database IS NOT DISTINCT FROM bl.database
LEFT JOIN pg_stat_activity b ON bl.pid = b.pid AND bl.granted = true
LEFT JOIN pg_class t ON t.oid = l.relation
WHERE a.state = 'active'
  AND (l.granted = false OR l.granted IS NULL)
ORDER BY a.pid;