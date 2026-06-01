SELECT * FROM public.driver;

CREATE TABLE IF NOT EXISTS public.driver
(
    driver_id integer NOT NULL,
    driver_fullname character varying(100) NOT NULL,
    licensetype character varying(100) NOT NULL,
    phone character varying(20), -- מאפשר NULL עבור הנתונים מ-public1
    CONSTRAINT driver_pkey PRIMARY KEY (driver_id)
);

INSERT INTO public.driver (driver_id, driver_fullname, licensetype, phone)
SELECT 
    driver_id, 
    driver_fullname, 
    licensetype, 
    NULL -- אין שדה טלפון ב-public1, אז נכניס NULL
FROM public1.driver;

INSERT INTO public.driver (driver_id, driver_fullname, licensetype, phone)
SELECT 
    driverid + (SELECT COALESCE(MAX(driver_id), 0) FROM public1.driver) AS driver_id, 
    fullname AS driver_fullname, 
    licensetype, 
    phone
FROM public2.driver;

------------------------------------------------------------------------------

SELECT * FROM public.includes;

CREATE TABLE public.includes
(
    route_id integer NOT NULL,
    stop_id integer NOT NULL,
    CONSTRAINT includes_pkey PRIMARY KEY (route_id, stop_id),
    CONSTRAINT includes_route_fkey FOREIGN KEY (route_id) REFERENCES public.route (route_id),
    CONSTRAINT includes_stop_fkey FOREIGN KEY (stop_id) REFERENCES public.stop (stop_id)
);

ALTER TABLE public.includes OWNER to rideflow_user;

INSERT INTO public.includes (route_id, stop_id)
SELECT 
    route_id, 
    stop_id
FROM public1.includes;

------------------------------------------------------------------------------

SELECT * FROM public.passenger;

CREATE TABLE public.passenger
(
    pass_id integer NOT NULL,
    pass_fullname character varying(100) NOT NULL,
    email character varying(100),
    phone character varying(20) NOT NULL,
    sector character varying(50),
    CONSTRAINT passenger_pkey PRIMARY KEY (pass_id),
    CONSTRAINT passenger_email_key UNIQUE (email),
    CONSTRAINT passenger_phone_key UNIQUE (phone)
);

-- יצירת אינדקס חיפוש לפי שם הנוסע
CREATE INDEX IF NOT EXISTS idx_passenger_name
    ON public.passenger (pass_fullname ASC NULLS LAST);

ALTER TABLE public.passenger OWNER to rideflow_user;

INSERT INTO public.passenger (pass_id, pass_fullname, email, phone, sector)
SELECT 
    pass_id, 
    pass_fullname, 
    email, 
    phone, 
    sector
FROM public1.passenger;

------------------------------------------------------------------

SELECT * FROM public.route; ORDER BY route_id

SELECT * FROM public.route WHERE startlocation IS NOT NULL;

CREATE TABLE public.route
(
    route_id integer NOT NULL,
    route_name character varying(100) NOT NULL,
    startlocation character varying(50),      -- ללא NOT NULL כדי לאפשר מיזוג מ-public1
    endlocation character varying(50),        -- ללא NOT NULL כדי לאפשר מיזוג מ-public1
    estimatedduration integer,                 -- ללא NOT NULL כדי לאפשר מיזוג מ-public1
    CONSTRAINT route_pkey PRIMARY KEY (route_id),
    CONSTRAINT route_duration_chk CHECK (estimatedduration >= 1 AND estimatedduration <= 3000)
);

ALTER TABLE public.route OWNER to rideflow_user;

INSERT INTO public.route (route_id, route_name, startlocation, endlocation, estimatedduration)
SELECT 
    route_id, 
    route_name, 
    NULL, -- אין startlocation ב-public1
    NULL, -- אין endlocation ב-public1
    NULL  -- אין estimatedduration ב-public1
FROM public1.route;


INSERT INTO public.route (route_id, route_name, startlocation, endlocation, estimatedduration)
SELECT 
    routeid + (SELECT COALESCE(MAX(route_id), 0) FROM public1.route) AS route_id, 
    routename AS route_name, 
    startlocation, 
    endlocation, 
    estimatedduration
FROM public2.route;

-------------------------------------------------------------------

SELECT * FROM public.routestop 

CREATE TABLE public.routestop
(
    route_id integer NOT NULL,
    stop_id integer NOT NULL,
    stop_order integer NOT NULL,
    CONSTRAINT routestop_pkey PRIMARY KEY (route_id, stop_id),
    CONSTRAINT routestop_route_fkey FOREIGN KEY (route_id) REFERENCES public.route (route_id),
    CONSTRAINT routestop_stop_fkey FOREIGN KEY (stop_id) REFERENCES public.stop (stop_id)
);

ALTER TABLE public.routestop OWNER to rideflow_user;

INSERT INTO public.routestop (route_id, stop_id, stop_order)
SELECT 
    routeid + (SELECT COALESCE(MAX(route_id), 0) FROM public1.route) AS route_id, 
    stopid + (SELECT COALESCE(MAX(stop_id), 0) FROM public1.stop) AS stop_id, 
    stoporder AS stop_order
FROM public2.routestop;

-------------------------------------------------------------------------------

SELECT * FROM public.stop ORDER BY stop_id;

CREATE TABLE public.stop
(
    stop_id integer NOT NULL,
    stop_name character varying(100) NOT NULL,
    address character varying(100), -- ללא NOT NULL כדי לאפשר מיזוג
    latitude numeric(10,6),          -- ללא NOT NULL כדי לאפשר מיזוג
    longitude numeric(10,6),         -- ללא NOT NULL כדי לאפשר מיזוג
    CONSTRAINT stop_pkey PRIMARY KEY (stop_id)
);

ALTER TABLE public.stop OWNER to rideflow_user;

INSERT INTO public.stop (stop_id, stop_name, address, latitude, longitude)
SELECT 
    stop_id, 
    stop_name, 
    NULL, -- אין address ב-public1
    NULL, -- אין latitude ב-public1
    NULL  -- אין longitude ב-public1
FROM public1.stop;


INSERT INTO public.stop (stop_id, stop_name, address, latitude, longitude)
SELECT 
    stopid + (SELECT COALESCE(MAX(stop_id), 0) FROM public1.stop) AS stop_id, 
    stopname AS stop_name, 
    address, 
    latitude, 
    longitude
FROM public2.stop;

---------------------------------------------------------------------------------

SELECT * FROM public1.trip 

CREATE TABLE public.trip
(
    trip_id integer NOT NULL,
    trip_date date,
    departure_time character varying(5),           -- מאפשר NULL עבור public2
    available_seats integer,                        -- מאפשר NULL עבור public2
    route_id integer NOT NULL,
    driver_id integer NOT NULL,
    plate_number character varying(100) NOT NULL,
    status character varying(20) DEFAULT 'Active',  -- מאפשר NULL עבור public1
    CONSTRAINT trip_pkey PRIMARY KEY (trip_id),
    CONSTRAINT trip_driver_fkey FOREIGN KEY (driver_id) REFERENCES public.driver (driver_id),
    CONSTRAINT trip_route_fkey FOREIGN KEY (route_id) REFERENCES public.route (route_id),
    CONSTRAINT trip_vehicle_fkey FOREIGN KEY (plate_number) REFERENCES public.vehicle (plate_number),
    CONSTRAINT check_available_seats_non_negative CHECK (available_seats >= 0)
);

-- יצירת האינדקסים החשובים לביצועים
CREATE INDEX IF NOT EXISTS idx_trip_date ON public.trip (trip_date ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_trip_driverid ON public.trip (driver_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_trip_routeid ON public.trip (route_id ASC NULLS LAST);

ALTER TABLE public.trip OWNER to rideflow_user;


INSERT INTO public.trip (trip_id, trip_date, departure_time, available_seats, route_id, driver_id, plate_number, status)
SELECT 
    trip_id, 
    trip_date, 
    departure_time, 
    available_seats, 
    route_id, 
    driver_id, 
    plate_number,
    NULL -- אין סטטוס ב-public1
FROM public1.trip;

INSERT INTO public.trip (trip_id, trip_date, departure_time, available_seats, route_id, driver_id, plate_number, status)
SELECT 
    t2.tripid + (SELECT COALESCE(MAX(trip_id), 0) FROM public1.trip) AS trip_id, 
    t2.tripdate AS trip_date, 
    NULL AS departure_time, 
    NULL AS available_seats,
    t2.routeid + (SELECT COALESCE(MAX(route_id), 0) FROM public1.route) AS route_id, 
    t2.driverid + (SELECT COALESCE(MAX(driver_id), 0) FROM public1.driver) AS driver_id, 
    v.plate_number, -- מושך את מספר הלוחית המתאים מתוך הטבלה המאוחדת
    t2.status
FROM public2.trip t2
JOIN public.vehicle v ON t2.busid = v.bus_id;

--------------------------------------------------------------------------

SELECT * FROM public.vehicle;

SELECT * FROM public.vehicle WHERE manufacturer IS NOT NULL;


CREATE TABLE public.vehicle
(
    plate_number character varying(100) NOT NULL,
    bus_id integer,                                  -- מאפשר NULL עבור רכבי public1
    capacity integer NOT NULL,
    vehicle_type character varying(100),             -- מאפשר NULL עבור רכבי public2
    manufacturer character varying(50),              -- מאפשר NULL עבור רכבי public1
    model character varying(20),                     -- מאפשר NULL עבור רכבי public1
    year integer,                                    -- מאפשר NULL עבור רכבי public1
    CONSTRAINT vehicle_pkey PRIMARY KEY (plate_number),
    CONSTRAINT check_capacity_positive CHECK (capacity >= 1)
);

-- יצירת אינדקס על הקיבולת כפי שהיה ב-public1 לשיפור ביצועים
CREATE INDEX IF NOT EXISTS idx_vehicle_capacity
    ON public.vehicle (capacity ASC NULLS LAST);

ALTER TABLE public.vehicle OWNER to rideflow_user;

INSERT INTO public.vehicle (plate_number, bus_id, capacity, vehicle_type, manufacturer, model, year)
SELECT 
    plate_number, 
    NULL, -- אין bus_id ב-public1
    capacity, 
    vehicle_type, 
    NULL, -- אין manufacturer
    NULL, -- אין model
    NULL  -- אין year
FROM public1.vehicle;


INSERT INTO public.vehicle (plate_number, bus_id, capacity, vehicle_type, manufacturer, model, year)
SELECT 
    licenseplate::varchar AS plate_number, 
    busid AS bus_id, 
    capacity, 
    NULL, -- אין vehicle_type ב-public2
    manufacturer, 
    model, 
    year
FROM public2.vehicle;

