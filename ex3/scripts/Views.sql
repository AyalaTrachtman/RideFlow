/* This view consolidates data from the trip, route, and driver
tables into a single readable format. It replaces IDs with actual names, 
allowing you to fetch complete trip details via one simple query.*/

CREATE VIEW trip_full_details_view AS
SELECT 
    t.trip_id,           -- The unique identifier of the trip
    t.trip_date,         -- The date of the trip
    r.route_name,        -- The name of the route (from route table)
    d.driver_fullname,   -- The name of the driver (from driver table)
    t.available_seats    -- Number of available seats
FROM 
    public.trip t
JOIN 
    public.route r ON t.route_id = r.route_id
JOIN 
    public.driver d ON t.driver_id = d.driver_id; -- Assuming driver_id exists in trip

--query 1 
-- This query calculates the total number of trips assigned to each driver.
SELECT 
    driver_fullname, 
    COUNT(*) AS total_trips -- Counting the total trips for every driver
FROM 
    trip_full_details_view
GROUP BY 
    driver_fullname;

select * from trip_full_details_view

--query 2
--This query lists the trips with the most available seats, helping to identify which routes have the most capacity.
SELECT 
    route_name, 
    trip_date, 
    available_seats
FROM 
    trip_full_details_view
ORDER BY 
    available_seats DESC; -- Sorting by availability in descending order to see the most empty trips first





------------------------------------------------------------------------------------------------------------------

-- Create a view that combines Trip, Driver, and Bus information
-- This provides a clear overview of active trips with descriptive names
CREATE OR REPLACE VIEW public.active_trip_details AS
SELECT 
    t.tripid,
    t.tripdate,
    d.fullname AS driver_name,
    b.licenseplate AS bus_license_plate,
    t.status
FROM public.trip t
JOIN public.driver d ON t.driverid = d.driverid
JOIN public.bus b ON t.busid = b.busid
WHERE t.status = 'Active'; -- Filtering only active trips

select * from public.active_trip_details

/* Query 1: Count active trips per driver
This query calculates how many active trips are currently assigned to each driver.
*/
SELECT 
    driver_name, 
    COUNT(tripid) AS active_trips_count -- Count the total number of trips associated with each driver
FROM public.active_trip_details
GROUP BY driver_name -- Group the results by driver name to aggregate the count
ORDER BY active_trips_count DESC; -- Sort by the number of trips in descending order to see the most active drivers

/* Query 2: List active buses and their assigned drivers
This query displays all currently active buses along with the name of the driver assigned to each one.
*/
SELECT 
    bus_license_plate, 
    driver_name,
    tripdate
FROM public.active_trip_details
ORDER BY tripdate ASC; -- Order the list chronologically by trip date
