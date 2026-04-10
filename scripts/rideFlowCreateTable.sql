CREATE TABLE STOP
(
  stop_id INT NOT NULL,
  stop_name VARCHAR(100) NOT NULL,
  PRIMARY KEY (stop_id)
);

CREATE TABLE ROUTE
(
  route_name VARCHAR(100) NOT NULL,
  route_id INT NOT NULL,
  PRIMARY KEY (route_id)
);

CREATE TABLE DRIVER
(
  licenseType VARCHAR(100) NOT NULL,
  driver_fullname VARCHAR(50) NOT NULL,
  driver_id INT NOT NULL,
  PRIMARY KEY (driver_id)
);

CREATE TABLE VEHICLE
(
  capacity INT NOT NULL,
  vehicle_type VARCHAR(100) NOT NULL,
  plate_number VARCHAR(20) NOT NULL,
  PRIMARY KEY (plate_number)
);

CREATE TABLE TRIP
(
  trip_id INT NOT NULL,
  trip_date DATE NOT NULL,
  departure_Time VARCHAR(5) NOT NULL,
  available_Seats INT NOT NULL,
  route_id INT NOT NULL,
  driver_id INT NOT NULL,
  plate_number VARCHAR(20) NOT NULL, -- תיקון: שונה מ-INT ל-VARCHAR כדי להתאים ל-VEHICLE
  PRIMARY KEY (trip_id),
  FOREIGN KEY (route_id) REFERENCES ROUTE(route_id),
  FOREIGN KEY (driver_id) REFERENCES DRIVER(driver_id),
  FOREIGN KEY (plate_number) REFERENCES VEHICLE(plate_number)
);

CREATE TABLE PASSENGER
(
  email VARCHAR(100),
  phone VARCHAR(20) NOT NULL,
  pass_fullname VARCHAR(100) NOT NULL,
  pass_id INT NOT NULL,
  sector VARCHAR(50),
  PRIMARY KEY (pass_id),
  UNIQUE (email),
  UNIQUE (phone)
);

CREATE TABLE REGISTRATION
(
  reg_id INT NOT NULL,
  status VARCHAR(20) NOT NULL,
  pass_id INT NOT NULL,
  trip_id INT NOT NULL,
  boarding_stop_id INT NOT NULL,
  dropoff_stop_id INT NOT NULL, 
  PRIMARY KEY (reg_id, pass_id),
  FOREIGN KEY (pass_id) REFERENCES PASSENGER(pass_id),
  FOREIGN KEY (trip_id) REFERENCES TRIP(trip_id),
  FOREIGN KEY (boarding_stop_id) REFERENCES STOP(stop_id),
  FOREIGN KEY (dropoff_stop_id) REFERENCES STOP(stop_id)
);

CREATE TABLE INCLUDES
(
  route_id INT NOT NULL,
  stop_id INT NOT NULL,
  PRIMARY KEY (route_id, stop_id),
  FOREIGN KEY (route_id) REFERENCES ROUTE(route_id),
  FOREIGN KEY (stop_id) REFERENCES STOP(stop_id)
);