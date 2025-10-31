CREATE DATABASE NODE_B;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Step 2: Create the server connection to BranchDB_A
DROP SERVER IF EXISTS Main_server CASCADE;
CREATE SERVER Main_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'node_b',
    port '5432'
);

-- Step 3: Create user mapping
CREATE USER MAPPING FOR postgres
SERVER Main_server
OPTIONS (
    user 'postgres',
    password 'postgres'
);

-- Step 4: Import tables from BranchDB_A
IMPORT FOREIGN SCHEMA public
LIMIT TO (Dispatchcenter, Vehicle, Responder,Assignment,Incident,Report)
FROM SERVER Main_server
INTO public;


-- On Node_B
CREATE TABLE Assignment_B (
    assignment_id INT PRIMARY KEY,
    incident_id INT,
    responder_id INT,
    vehicle_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- Insert fragment data (assignment_id > 5)
INSERT INTO Assignment_B
SELECT *
FROM Assignment
WHERE assignment_id > 5;
SELECT*FROM Assignment_B
INSERT INTO Assignment_B (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES
(1, 1, 1, 1, '2025-10-01 08:35:00', '2025-10-01 09:00:00'),
(2, 1, 2, 2, '2025-10-01 08:40:00', '2025-10-01 09:15:00'),
(3, 2, 3, 3, '2025-10-02 10:10:00', '2025-10-02 10:45:00'),
(4, 3, 4, 4, '2025-10-03 14:25:00', '2025-10-03 15:00:00'),
(5, 4, 5, 5, '2025-10-03 18:45:00', '2025-10-03 19:05:00');



-- Step 2: Create the server connection to BranchDB_A
DROP SERVER IF EXISTS Main_server CASCADE;
CREATE SERVER Main_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'node_b',
    port '5432'
);

-- Step 3: Create user mapping
CREATE USER MAPPING FOR postgres
SERVER Main_server
OPTIONS (
    user 'postgres',
    password 'postgres'
);

-- Step 4: Import tables from BranchDB_A
IMPORT FOREIGN SCHEMA public
LIMIT TO (Dispatchcenter, Vehicle, Responder,Assignment,Incident,Report)
FROM SERVER Main_server
INTO public;


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

-- Step 4: Import tables from BranchDB_A
IMPORT FOREIGN SCHEMA public
LIMIT TO (Assignment_A)
FROM SERVER Proj_link_1
INTO public;
SELECT * FROM Assignment_A;




-- STEP 2: Simulate transaction preparation on NODE_B
-- ============================================================

BEGIN;
-- Insert a test record into Assignment_B (Node_B fragment)
INSERT INTO Assignment_B (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES (105, 3, 4, 4, '2025-10-27 09:00:00', '2025-10-27 09:30:00');

-- Prepare this transaction (this will store it in pg_prepared_xacts)
PREPARE TRANSACTION 'tx_recover_B';

COMMIT PREPARED 'tx_recover_B';

SELECT * FROM Assignment_B WHERE assignment_id = 105;

