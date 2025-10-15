-- Create a new database for managing emergency response operations
CREATE DATABASE emergency_response;
-- Create a table to store details of dispatch centers
CREATE TABLE DispatchCenter (
    center_id INT PRIMARY KEY,  -- Unique ID for each dispatch center
    center_name VARCHAR(100) NOT NULL, -- Name of the dispatch center
	location VARCHAR(100) NOT NULL,    -- Physical location or address
    district VARCHAR(50) NOT NULL,     -- District where the center operates
	Contact VARCHAR(10) NOT NULL       -- Contact phone number for the center
	
);

-- Create a table to store information about emergency vehicles
CREATE TABLE Vehicle (
    vehicle_id INT PRIMARY KEY,                                  -- Unique ID for each vehicle
    plate_no VARCHAR(20) UNIQUE NOT NULL,                        -- Vehicle registration number (must be unique)
    vehicle_type VARCHAR(50) NOT NULL,                           -- Type of vehicle (e.g., Ambulance, Fire Truck, Police Car)
    status VARCHAR(20) CHECK (status IN ('Available', 'OnDuty', 'Maintenance')) NOT NULL,  -- Current operational status
    center_id INT,                                               -- Foreign key linking vehicle to its dispatch center
    FOREIGN KEY (center_id) REFERENCES DispatchCenter(center_id) -- Ensures each vehicle is assigned to a valid dispatch center
);

-- Create a table to store information about emergency responders
CREATE TABLE Responder (
    responder_id INT PRIMARY KEY,                                            -- Unique ID for each responder
    responder_name VARCHAR(100) NOT NULL,                                    -- Full name of the responder
    role VARCHAR(50),                                                        -- Role or position (e.g., Paramedic, Firefighter, Police Officer)
    phone VARCHAR(20),                                                       -- Contact phone number
    availability VARCHAR(20) CHECK (availability IN ('Available', 'OnDuty', 'Leave')) NOT NULL, -- Current availability status
    center_id INT,                                                           -- Foreign key linking the responder to a dispatch center
    FOREIGN KEY (center_id) REFERENCES DispatchCenter(center_id)             -- Ensures each responder belongs to a valid dispatch center
);

-- Create a table to store information about reported emergency incidents
CREATE TABLE Incident (
    incident_id INT PRIMARY KEY,                                      -- Unique ID for each incident
    incident_type VARCHAR(50),                                        -- Type of incident (e.g., Fire, Accident, Medical Emergency)
    location VARCHAR(100),                                            -- Specific location where the incident occurred
    district VARCHAR(50),                                             -- District in which the incident took place
    severity VARCHAR(20) CHECK (severity IN ('Low', 'Medium', 'High')), -- Severity level of the incident
    reported_time TIMESTAMP,                                          -- Date and time when the incident was reported
    status VARCHAR(20) CHECK (status IN ('Pending', 'Resolved')) 
        DEFAULT 'Pending'                                             -- Current status of the incident (defaults to 'Pending')
);

-- Create a table to record assignments of responders and vehicles to specific incidents
CREATE TABLE Assignment (
    assignment_id INT PRIMARY KEY,                                      -- Unique ID for each assignment record
    incident_id INT,                                                    -- References the incident being handled
    responder_id INT,                                                   -- References the responder assigned to the incident
    vehicle_id INT,                                                     -- References the vehicle used for the assignment
    start_time TIMESTAMP,                                               -- Date and time when the assignment started
    end_time TIMESTAMP,                                                 -- Date and time when the assignment ended
    FOREIGN KEY (incident_id) REFERENCES Incident(incident_id),         -- Ensures the incident exists in the Incident table
    FOREIGN KEY (responder_id) REFERENCES Responder(responder_id),      -- Ensures the responder exists in the Responder table
    FOREIGN KEY (vehicle_id) REFERENCES Vehicle(vehicle_id)             -- Ensures the vehicle exists in the Vehicle table
);

-- Create a table to store reports summarizing the outcome of each assignment
CREATE TABLE Report (
    report_id INT PRIMARY KEY,                                            -- Unique ID for each report
    assignment_id INT,                                                    -- References the related assignment record
    resolution TEXT,                                                      -- Description of how the incident was resolved
    duration INTERVAL,                                                    -- Time taken to complete the assignment
    outcome VARCHAR(100),                                                 -- Result or status after handling the incident
    FOREIGN KEY (assignment_id) REFERENCES Assignment(assignment_id) 
        ON DELETE CASCADE                                                 -- Automatically delete report if related assignment is deleted
);

--------------------------------------------------------------------------------------------------
-- Insert data into DispatchCenter table
-- Each record represents a dispatch center responsible for managing emergencies in its district
INSERT INTO DispatchCenter (center_id, center_name, location, district, contact)
VALUES
(1, 'Central Dispatch', 'Kigali City', 'Kigali', '0781234567'),
(2, 'Eastern Dispatch', 'Rwamagana Town', 'Eastern', '0782345678'),
(3, 'Western Dispatch', 'Rubavu City', 'Western', '0783456789');


-- Insert data into Responder table
-- Contains responder details: name, role, availability, and their dispatch center
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


-- Insert data into Incident table
-- Each record represents a reported emergency, including severity and status
INSERT INTO Incident (incident_id, incident_type, location, district, severity, reported_time, status)
VALUES
(1, 'Fire Outbreak', 'Nyamirambo', 'Kigali', 'High', '2025-10-01 08:30:00', 'Resolved'),
(2, 'Road Accident', 'Remera', 'Kigali', 'Medium', '2025-10-02 10:00:00', 'Resolved'),
(3, 'Flood', 'Gatsibo', 'Eastern', 'High', '2025-10-03 14:20:00', 'Pending'),
(4, 'Medical Emergency', 'Rwamagana', 'Eastern', 'Low', '2025-10-03 18:40:00', 'Resolved'),
(5, 'Building Collapse', 'Rubavu', 'Western', 'High', '2025-10-04 09:00:00', 'Resolved'),
(6, 'Traffic Accident', 'Musanze', 'Western', 'Medium', '2025-10-05 11:10:00', 'Resolved');


-- Insert data into Vehicle table
-- Each vehicle belongs to a dispatch center and has a type and operational status
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


-- Insert data into Assignment table
-- Records which responder and vehicle were assigned to each incident, with start and end times
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


-- Insert data into Report table
-- Summarizes the outcome and duration of each assignment
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

----------------------------------------------------------------------------------------------------------
-- Retrieve total number of incidents handled per district
-- This query counts how many incidents have been reported in each district
SELECT 
    district,                                -- The name of the district where the incident occurred
    COUNT(incident_id) AS total_incidents    -- The total number of incidents in that district
FROM 
    Incident                                 -- Source table containing incident records
GROUP BY 
    district                                 -- Group results by district to aggregate counts
ORDER BY 
    total_incidents DESC;                    -- Sort results from highest to lowest number of incidents
--------------------------------------------------------------------------------------------------------

-- Update vehicle status to 'Available' after the related incident has been resolved
UPDATE Vehicle
SET status = 'Available'                             -- Change the vehicle's current status to 'Available'
WHERE vehicle_id IN (                                -- Apply the update only to vehicles involved in resolved incidents
    SELECT a.vehicle_id                              -- Select vehicle IDs from the Assignment table
    FROM Assignment a
    JOIN Incident i ON a.incident_id = i.incident_id  -- Join with Incident table to check incident status
    WHERE i.status = 'Resolved'                       -- Only include vehicles linked to incidents that are resolved
);

-------------------------------------------------------------------------------------------------------
-- Retrieve responders with the fastest average response times
SELECT 
    r.responder_id,                                                        -- Unique ID of each responder
    r.responder_name,                                                      -- Name of the responder
    ROUND(AVG(EXTRACT(EPOCH FROM (a.end_time - a.start_time)) / 60), 2) AS avg_response_minutes  -- Average response duration in minutes (rounded to 2 decimals)
FROM 
    Assignment a                                                           -- Source table containing assignment records
JOIN 
    Responder r ON a.responder_id = r.responder_id                         -- Join to match each assignment with the corresponding responder
WHERE 
    a.end_time IS NOT NULL AND a.start_time IS NOT NULL                    -- Only include assignments with valid start and end times
GROUP BY 
    r.responder_id, r.responder_name                                       -- Group results by each responder
ORDER BY 
    avg_response_minutes ASC                                               -- Sort by average response time (fastest first)
LIMIT 7;    

------------------------------------------------------------------------------------------------

-- Display all responder and assignment details together
SELECT 
    r.responder_id,               -- Responder unique ID
    r.responder_name,             -- Name of the responder
    r.role,                       -- Responder’s role (e.g., Paramedic, Firefighter)
    --r.availability,               -- Current availability status
    --a.assignment_id,              -- Unique ID for each assignment
    --a.incident_id,                -- The incident handled by this assignment
    --a.vehicle_id,                 -- The vehicle used
    a.start_time,                 -- When the assignment started
    a.end_time,                   -- When the assignment ended
    (EXTRACT(EPOCH FROM (a.end_time - a.start_time)) / 60) AS duration_minutes  -- Duration of response in minutes
FROM 
    Assignment a
JOIN 
    Responder r ON a.responder_id = r.responder_id  -- Join the two tables using responder_id
ORDER BY 
     duration_minutes;               -- Sort the results by responder for clarity

---------------------------------------------------------------------------------------------------

-- Create or replace a view named 'IncidentSeveritySummary'
-- This view summarizes the total number of incidents for each severity level
CREATE OR REPLACE VIEW IncidentSeveritySummary AS
SELECT 
    severity,                     -- The severity level of the incident (e.g., High, Medium, Low)
    COUNT(incident_id) AS total_incidents  -- Count of incidents for that severity
FROM 
    Incident                       -- From the 'Incident' table
GROUP BY 
    severity                        -- Group results by severity to get totals per severity
ORDER BY 
    total_incidents DESC;           -- Order the results from highest to lowest total incidents

-- Select all records from the view to display the summarized data
SELECT * FROM IncidentSeveritySummary;
-----------------------------------------------------------------------------------------------------
-- ==============================================
-- Function: prevent_double_booking
-- Purpose: Prevents assigning a responder to overlapping assignments.
-- Trigger: Checks BEFORE INSERT or UPDATE on the Assignment table.
-- ==============================================

CREATE OR REPLACE FUNCTION prevent_double_booking()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the responder already has another assignment overlapping with the new one
    IF EXISTS (
        SELECT 1
        FROM Assignment
        WHERE responder_id = NEW.responder_id          -- Same responder
          AND assignment_id <> COALESCE(NEW.assignment_id, -1) -- Exclude current assignment for updates
          AND (
              (NEW.start_time, NEW.end_time) OVERLAPS (start_time, end_time) -- Time overlap check
          )
    ) THEN
        -- Raise an error to prevent double booking
        RAISE EXCEPTION 'Responder % is already assigned during this time!', NEW.responder_id;
    END IF;

    -- Allow the insert or update if no conflict is found
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==============================================
-- Trigger: trg_prevent_double_booking
-- Purpose: Executes the prevent_double_booking() function before every INSERT or UPDATE
-- on the Assignment table to ensure a responder is not double-booked.
-- ==============================================

CREATE TRIGGER trg_prevent_double_booking
BEFORE INSERT OR UPDATE ON Assignment
FOR EACH ROW
EXECUTE FUNCTION prevent_double_booking();

-- ==============================================
-- Example Inserts:
-- 1st insert (assignment_id=11) should succeed if no conflicts exist.
-- 2nd insert (assignment_id=12) will fail if it overlaps with any existing assignment
--    for responder_id=1 due to the trigger function.
-- ==============================================

INSERT INTO Assignment (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES (13, 6, 1, 2, '2025-10-06 10:00:00', '2025-10-06 11:00:00');

INSERT INTO Assignment (assignment_id, incident_id, responder_id, vehicle_id, start_time, end_time)
VALUES (16, 3, 1, 3, '2025-10-06 10:30:00', '2025-10-06 11:15:00');  -- This will raise an exception