
-- Create a view to combine trip status with vehicle specifications
CREATE VIEW view_trip_details AS
SELECT 
    t.trip_id,
    t.trip_date,
    t.departure_time,
    t.route_id,
    t.status,
    t.available_seats,
    v.plate_number,
    v.model AS vehicle_model,
    v.manufacturer AS vehicle_manufacturer,
    v.capacity AS max_capacity
FROM trip t
-- Join trips with vehicles using the plate_number column
JOIN vehicle v ON t.plate_number = v.plate_number
-- Filter to show only active trips
WHERE t.status = 'Active';

select * from view_trip_details

-- Retrieve all active trips from a specific baseline date that still have available seats
SELECT trip_id, route_id, trip_date, departure_time, available_seats, vehicle_model
FROM view_trip_details
WHERE trip_date >= '2026-05-01'
  --AND available_seats > 0
ORDER BY trip_date ASC, departure_time ASC;



-- Analyze trip counts and average availability grouped by vehicle model
SELECT vehicle_model, COUNT(trip_id) AS total_trips, AVG(available_seats) AS avg_seats
FROM view_trip_details
GROUP BY vehicle_model;



-----------------------------------------------------------------------------------------------------------

-- Create a view to track driver assignments and calculate total workload
CREATE VIEW view_driver_workload AS
SELECT 
    d.driver_id,
    d.driver_fullname,
    d.licensetype,
    d.phone,
    -- Count total trips assigned to each driver
    COUNT(t.trip_id) AS total_assigned_trips
FROM driver d
-- Use LEFT JOIN so drivers with no trips yet will still appear in the list
LEFT JOIN trip t ON d.driver_id = t.driver_id
GROUP BY d.driver_id, d.driver_fullname, d.licensetype, d.phone;

select * from view_driver_workload

-- Find drivers who have been assigned to more than 5 trips
SELECT driver_id, driver_fullname, total_assigned_trips
FROM view_driver_workload
WHERE total_assigned_trips > 1
ORDER BY total_assigned_trips DESC;



-- Retrieve a list of drivers who currently have zero assigned trips
SELECT driver_id, driver_fullname, licensetype, phone
FROM view_driver_workload
WHERE total_assigned_trips = 0;


