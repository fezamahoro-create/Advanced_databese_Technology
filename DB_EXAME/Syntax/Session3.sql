BEGIN;
UPDATE Assignment_A
SET incident = 'P'
WHERE incident_id = 1;

SELECT * 
FROM pg_locks l
JOIN pg_class t ON l.relation = t.oid
WHERE t.relname = 'Incident';
