-- ========================================
-- Rename all tables by adding "1" suffix
-- Database: MERGED
-- ========================================

-- Drop foreign keys first (to allow table renaming)
ALTER TABLE registration DROP CONSTRAINT registration_pass_id_fkey;
ALTER TABLE registration DROP CONSTRAINT registration_trip_id_fkey;
ALTER TABLE registration DROP CONSTRAINT registration_boarding_stop_id_fkey;
ALTER TABLE registration DROP CONSTRAINT registration_dropoff_stop_id_fkey;
ALTER TABLE includes DROP CONSTRAINT includes_route_id_fkey;
ALTER TABLE includes DROP CONSTRAINT includes_stop_id_fkey;
ALTER TABLE trip DROP CONSTRAINT trip_route_id_fkey;
ALTER TABLE trip DROP CONSTRAINT trip_driver_id_fkey;
ALTER TABLE trip DROP CONSTRAINT trip_plate_number_fkey;

-- Rename all tables by adding "1" to the name
ALTER TABLE stop RENAME TO stop1;
ALTER TABLE route RENAME TO route1;
ALTER TABLE driver RENAME TO driver1;
ALTER TABLE vehicle RENAME TO vehicle1;
ALTER TABLE trip RENAME TO trip1;
ALTER TABLE passenger RENAME TO passenger1;
ALTER TABLE registration RENAME TO registration1;
ALTER TABLE includes RENAME TO includes1;

-- Recreate foreign keys with new table names
ALTER TABLE registration1
ADD CONSTRAINT registration1_pass_id_fkey
FOREIGN KEY (pass_id) REFERENCES passenger1(pass_id);

ALTER TABLE registration1
ADD CONSTRAINT registration1_trip_id_fkey
FOREIGN KEY (trip_id) REFERENCES trip1(trip_id);

ALTER TABLE registration1
ADD CONSTRAINT registration1_boarding_stop_id_fkey
FOREIGN KEY (boarding_stop_id) REFERENCES stop1(stop_id);

ALTER TABLE registration1
ADD CONSTRAINT registration1_dropoff_stop_id_fkey
FOREIGN KEY (dropoff_stop_id) REFERENCES stop1(stop_id);

ALTER TABLE includes1
ADD CONSTRAINT includes1_route_id_fkey
FOREIGN KEY (route_id) REFERENCES route1(route_id);

ALTER TABLE includes1
ADD CONSTRAINT includes1_stop_id_fkey
FOREIGN KEY (stop_id) REFERENCES stop1(stop_id);

ALTER TABLE trip1
ADD CONSTRAINT trip1_route_id_fkey
FOREIGN KEY (route_id) REFERENCES route1(route_id);

ALTER TABLE trip1
ADD CONSTRAINT trip1_driver_id_fkey
FOREIGN KEY (driver_id) REFERENCES driver1(driver_id);

ALTER TABLE trip1
ADD CONSTRAINT trip1_plate_number_fkey
FOREIGN KEY (plate_number) REFERENCES vehicle1(plate_number);
