BEGIN;
UPDATE Assignment_A
SET incident_id = 31
WHERE responder_id = 1;
 
 
 
 
 SELECT * FROM Assignment_A WHERE incident_id = 31;
 
 
 
 rollback;