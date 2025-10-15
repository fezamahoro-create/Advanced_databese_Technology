
# Emergency Response and Dispatch Management System

## Case Study Overview
This project implements an **Emergency Response Database** that tracks **incidents, dispatch centers, responders, vehicles, assignments, and reports**. The system is designed to help emergency agencies efficiently manage **dispatch operations** and **response times** across districts.

It is implemented in **PostgreSQL** with **strong primary and foreign key constraints, CHECK constraints, and cascade delete functionality** to ensure data integrity.

---

## Database Schema

### Tables

1. **DispatchCenter**
   - `CenterID` (Primary Key)
   - `Name`
   - `Location`
   - `Contact`
   - `District`

2. **Incident**
   - `IncidentID` (Primary Key)
   - `CenterID` (Foreign Key → DispatchCenter)
   - `Type`
   - `Location`
   - `DateReported`
   - `Severity`

3. **Responder**
   - `ResponderID` (Primary Key)
   - `FullName`
   - `Role`
   - `Contact`
   - `Availability`

4. **Vehicle**
   - `VehicleID` (Primary Key)
   - `Type`
   - `PlateNo`
   - `Status`
   - `CenterID` (Foreign Key → DispatchCenter)

5. **Assignment**
   - `AssignID` (Primary Key)
   - `IncidentID` (Foreign Key → Incident)
   - `ResponderID` (Foreign Key → Responder)
   - `VehicleID` (Foreign Key → Vehicle)
   - `DispatchTime`
   - `ArrivalTime`

6. **Report**
   - `ReportID` (Primary Key)
   - `AssignID` (Foreign Key → Assignment, `ON DELETE CASCADE`)
   - `Resolution`
   - `Duration`
   - `Remarks`

---

## Relationships
- **DispatchCenter → Incident** : One DispatchCenter handles many Incidents (1:N)  
- **DispatchCenter → Vehicle** : One DispatchCenter manages many Vehicles (1:N)  
- **Incident → Assignment** : One Incident may have multiple Assignments (1:N)  
- **Responder → Assignment** : One Responder may handle multiple Assignments (1:N)  
- **Assignment → Report** : Each Assignment has one Report (1:1, CASCADE DELETE)

---

## Key Features
- Tracks **dispatch centers, responders, vehicles, and incidents**
- Logs **assignment of responders and vehicles to incidents**
- Calculates **response duration**
- Automatically removes **reports when assignments are deleted** using CASCADE DELETE
- Ensures data integrity with **primary keys, foreign keys, and CHECK constraints**

---

