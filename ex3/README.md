# דוח שלב ג' — אינטגרציה (RideFlow)

דוח זה מתעד את שלב ג' של פרויקט RideFlow על פי ההנחיות שניתנו. הדוח כולל:

- תמונות מסך של תרשימי DSD ו-ERD
- תמונות מסך של פלטי שאילתות ומבטים
- החלטות שנעשו בשלב האינטגרציה
- הסבר מילולי של התהליך ושל הפקודות
- לכל מבט: תיאור מילולי, שאילתת `SELECT *` דוגמה (10 רשומות)
- לכל שאילתא על מבט: תיאור מילולי, קוד השאילתא, ופלט

## מבנה הקבצים ב-`ex3`

בתיקיית `ex3` נמצאים הקבצים והקבצים הבאים:

1. DSD של האגף החדש: `drive_dsd`
2. ERD אגף חדש: `erdplus (5).png`
3. ERD משותף: `erdplus (6).png`
4. DSD לאחר אינטגרציה: `merged_dsd`
5. פקודות שינוי ויצירה של טבלאות: `scripts/Integrate.sql`
6. פקודות ליצירת מבטים ושאילתות: `scripts/Views.sql`

---

## תמונות מסך של תרשימי DSD ו-ERD

### DSD של האגף החדש (Source)

![DSD - drive_dsd](screenshots/Screenshot%202026-06-01%20123628.png)

### ERD של האגף החדש

![ERD אגף חדש](screenshots/erdplus%20%285%29.png)

### DSD לאחר אינטגרציה (Merged)

![DSD מאוחד - merged_dsd](screenshots/Screenshot%202026-06-01%20123729.png)

### ERD משותף (בין שני המקורות)

![ERD משותף](screenshots/erdplus%20%286%29.png)

### תרשים ERD של מסד הנתונים המאוחד (Final)

![ERD מאוחד](screenshots/Screenshot%202026-06-01%20123650.png)

### תרשים ERD נוסף (זוויט נוסף)

![ERD נוסף](screenshots/Screenshot%202026-06-01%20123750.png)

> התמונות לעיל מציגות את המבנים הלוגיים, הקשרים בין הישויות, התהליך של מיזוג שתי סכמות, והמעבר ממבנים נפרדים למבנה מאוחד.

---

## החלטות עיקריות שנעשו בשלב האינטגרציה

1. הפרדה ראשונית ל-`public1` ו-`public2` כדי לשמר את מקור הנתונים המקורי ולהגן על הגיבויים.
2. הקמת סכמה חדשה `public` שתהיה הבסיס המאוחד של כל הטבלאות לאחר האינטגרציה.
3. טבלאות מאוחדות הוגדרו עם שדות מכל המקורות כדי למנוע איבוד מידע.
4. שדות חדשים שלא הופיעו בכל המקורות הוגדרו כ-`NULL` כדי לאפשר מיזוג נתונים מלא.
5. טיפוסי נתונים הורחבו לפי המקסימום הדרוש מכל המקורות כדי למנוע חיתוך נתונים.
6. משיכת הנתונים מ-`public2` בוצעה עם `OFFSET` למזהים כדי למנוע התנגשויות בין IDs זהים.
7. נוספו אינדקסים לייעול שאילתות על טבלאות מרכזיות.

---

## הסבר מילולי של תהליך האינטגרציה והפקודות

### 1. הכנת השטח והגיבויים (`public1` ו-`public2`)

- מכל בסיס נתונים מקור ראשון הועתקו הטבלאות לסכמה זמנית בשם `public1`.
- מכל בסיס נתונים מקור שני הועברו הטבלאות לסכמה זמנית בשם `public2`.
- נוצרה סכמה חדשה בשם `public` שתכיל את בסיס הנתונים המאוחד.

### 2. תכנון מבנה הטבלה המאוחדת

- לפני העברת שורות, בודקים את קוד ה-`CREATE TABLE` בכל מקור כדי לזהות הבדלים בתכונות.
- מאחדים שדות מכל המקורות בטבלה החדשה. אם שדה קיים רק באחד מהם, מוסיפים אותו כ-`NULL` בטבלה המאוחדת.
- מורחבים טיפוסי הנתונים לפי האורך או הטווח המקסימלי הנדרש.
- שדות יחודיים למקור אחד מוגדרים כ-`NULL` בטבלה המאוחדת כדי לאפשר הכנסת נתונים משני המקורות.

### 3. שלב הזרמת הנתונים והטיפול במפתחות

- מזרימים את כל השורות מ-`public1` לטבלאות ב-`public` עם המזהים המקוריים שלהם.
- מזרימים את כל השורות מ-`public2` ל-`public` עם הוספת אופסט לכל ה-IDs כדי למנוע התנגשויות.
- השיטה שומרת על קשרים רלציוניים בין טבלאות מורכבות (למשל `trip`, `route`, `driver`, `vehicle`).

### 4. ניקוי וסיום

- לאחר שכל הטבלאות ממשיכות ומקושרות באופן תקין ב-`public`, מבצעים בדיקות תקינות.
- לאחר האישור מוחקים את הסכמות הזמניות `public1` ו-`public2` כדי שלא תיווצר כפילות.

---

## קטעי קוד מהאינטגרציה (`scripts/Integrate.sql`)

### תמונת הקוד בתהליך ההרצה

![Integrate.sql execution](screenshots/Screenshot%202026-06-01%20105746.png)

### יצירת הטבלאות והגדרת שדות

#### טבלת `public.driver`

```sql
CREATE TABLE IF NOT EXISTS public.driver
(
    driver_id integer NOT NULL,
    driver_fullname character varying(100) NOT NULL,
    licensetype character varying(100) NOT NULL,
    phone character varying(20), -- מאפשר NULL עבור הנתונים מ-public1
    CONSTRAINT driver_pkey PRIMARY KEY (driver_id)
);
```

#### טבלת `public.route`

```sql
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
```

#### טבלת `public.trip`

```sql
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
```

#### טבלת `public.vehicle`

```sql
CREATE TABLE public.vehicle
(
    plate_number character varying(100) NOT NULL,
    bus_id integer,
    capacity integer NOT NULL,
    vehicle_type character varying(100),
    manufacturer character varying(50),
    model character varying(20),
    year integer,
    CONSTRAINT vehicle_pkey PRIMARY KEY (plate_number),
    CONSTRAINT check_capacity_positive CHECK (capacity >= 1)
);
```

#### טבלת `public.stop`

```sql
CREATE TABLE public.stop
(
    stop_id integer NOT NULL,
    stop_name character varying(100) NOT NULL,
    address character varying(100),
    latitude numeric(10,6),
    longitude numeric(10,6),
    CONSTRAINT stop_pkey PRIMARY KEY (stop_id)
);
```

#### טבלת `public.routestop`

```sql
CREATE TABLE public.routestop
(
    route_id integer NOT NULL,
    stop_id integer NOT NULL,
    stop_order integer NOT NULL,
    CONSTRAINT routestop_pkey PRIMARY KEY (route_id, stop_id),
    CONSTRAINT routestop_route_fkey FOREIGN KEY (route_id) REFERENCES public.route (route_id),
    CONSTRAINT routestop_stop_fkey FOREIGN KEY (stop_id) REFERENCES public.stop (stop_id)
);
```

#### טבלת `public.includes`

```sql
CREATE TABLE public.includes
(
    route_id integer NOT NULL,
    stop_id integer NOT NULL,
    CONSTRAINT includes_pkey PRIMARY KEY (route_id, stop_id),
    CONSTRAINT includes_route_fkey FOREIGN KEY (route_id) REFERENCES public.route (route_id),
    CONSTRAINT includes_stop_fkey FOREIGN KEY (stop_id) REFERENCES public.stop (stop_id)
);
```

### הזרמת נתונים מ-`public1`

```sql
INSERT INTO public.driver (driver_id, driver_fullname, licensetype, phone)
SELECT 
    driver_id, 
    driver_fullname, 
    licensetype, 
    NULL
FROM public1.driver;
```

```sql
INSERT INTO public.route (route_id, route_name, startlocation, endlocation, estimatedduration)
SELECT 
    route_id, 
    route_name, 
    NULL,
    NULL,
    NULL
FROM public1.route;
```

```sql
INSERT INTO public.trip (trip_id, trip_date, departure_time, available_seats, route_id, driver_id, plate_number, status)
SELECT 
    trip_id, 
    trip_date, 
    departure_time, 
    available_seats, 
    route_id, 
    driver_id, 
    plate_number,
    NULL
FROM public1.trip;
```

### הזרמת נתונים מ-`public2` עם אופסט ל-IDs

```sql
INSERT INTO public.driver (driver_id, driver_fullname, licensetype, phone)
SELECT 
    driverid + (SELECT COALESCE(MAX(driver_id), 0) FROM public1.driver) AS driver_id, 
    fullname AS driver_fullname, 
    licensetype, 
    phone
FROM public2.driver;
```

```sql
INSERT INTO public.route (route_id, route_name, startlocation, endlocation, estimatedduration)
SELECT 
    routeid + (SELECT COALESCE(MAX(route_id), 0) FROM public1.route) AS route_id, 
    routename AS route_name, 
    startlocation, 
    endlocation, 
    estimatedduration
FROM public2.route;
```

```sql
INSERT INTO public.routestop (route_id, stop_id, stop_order)
SELECT 
    routeid + (SELECT COALESCE(MAX(route_id), 0) FROM public1.route) AS route_id, 
    stopid + (SELECT COALESCE(MAX(stop_id), 0) FROM public1.stop) AS stop_id, 
    stoporder AS stop_order
FROM public2.routestop;
```

```sql
INSERT INTO public.trip (trip_id, trip_date, departure_time, available_seats, route_id, driver_id, plate_number, status)
SELECT 
    t2.tripid + (SELECT COALESCE(MAX(trip_id), 0) FROM public1.trip) AS trip_id, 
    t2.tripdate AS trip_date, 
    NULL AS departure_time, 
    NULL AS available_seats,
    t2.routeid + (SELECT COALESCE(MAX(route_id), 0) FROM public1.route) AS route_id, 
    t2.driverid + (SELECT COALESCE(MAX(driver_id), 0) FROM public1.driver) AS driver_id, 
    v.plate_number,
    t2.status
FROM public2.trip t2
JOIN public.vehicle v ON t2.busid = v.bus_id;
```

---

## מבטים ושאילתות (`scripts/Views.sql`)

### מבט 1: `view_trip_details`

#### תיאור מילולי
מבט זה מאחד נתונים מ-`trip` ו-`vehicle` כדי להציג מידע על נסיעות פעילות, כולל מספר לוחית, דגם ואיכויות האוטובוס.
המסנן בוחר רק נסיעות בעלות סטטוס `Active`.

#### קוד יצירת המבט

```sql
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
JOIN vehicle v ON t.plate_number = v.plate_number
WHERE t.status = 'Active';
```

#### שאילתת דוגמה לשליפת נתונים (10 רשומות)

```sql
SELECT * FROM view_trip_details LIMIT 10;
```

#### תיאור מילולי לשאילתה
השאילתה מציגה את רשומות ה-`view_trip_details` הראשונות עם כל העמודות, כך שניתן לבדוק שהמבט מקשר נכון בין נסיעות לאוטובוסים פעילים.

#### שאילתות נוספות על המבט

- שאילתא: ניתוח נסיעות לפי דגם רכב

```sql
SELECT vehicle_model, COUNT(trip_id) AS total_trips, AVG(available_seats) AS avg_seats
FROM view_trip_details
GROUP BY vehicle_model;
```

- שאילתא: רשימת נסיעות פעילות לפי תאריך וזמן יציאה

```sql
SELECT trip_id, route_id, trip_date, departure_time, available_seats, vehicle_model
FROM view_trip_details
WHERE trip_date >= '2026-05-01'
ORDER BY trip_date ASC, departure_time ASC;
```

#### תמונות מסך של המבט והשאילתות

![CREATE VIEW view_trip_details](screenshots/Screenshot%202026-06-10%20200753.png)
![SELECT * FROM view_trip_details](screenshots/Screenshot%202026-06-10%20200826.png)
![GROUP BY vehicle_model](screenshots/Screenshot%202026-06-10%20200857.png)
![Active trips ordered by date](screenshots/Screenshot%202026-06-10%20200917.png)

---

### מבט 2: `view_driver_workload`

#### תיאור מילולי
מבט זה מחשב את העומס הכולל של הנהגים על ידי חישוב מספר הנסיעות שמוקצות לכל נהג, ומציג פרטי נהג רלוונטיים.
המבט כולל גם נהגים שאין להם נסיעות מוקצות.

#### קוד יצירת המבט

```sql
CREATE VIEW view_driver_workload AS
SELECT 
    d.driver_id,
    d.driver_fullname,
    d.licensetype,
    d.phone,
    COUNT(t.trip_id) AS total_assigned_trips
FROM driver d
LEFT JOIN trip t ON d.driver_id = t.driver_id
GROUP BY d.driver_id, d.driver_fullname, d.licensetype, d.phone;
```

#### שאילתת דוגמה לשליפת נתונים (10 רשומות)

```sql
SELECT * FROM view_driver_workload LIMIT 10;
```

#### תיאור מילולי לשאילתה
השאילתה מציגה את המעמסה של כל הנהגים, כולל נהגים ללא נסיעות, כדי לוודא שהמבט משקף את כל הנהגים במערכת.

#### שאילתות נוספות על המבט

- שאילתא: נהגים עם יותר מנסיעה אחת

```sql
SELECT driver_id, driver_fullname, total_assigned_trips
FROM view_driver_workload
WHERE total_assigned_trips > 1
ORDER BY total_assigned_trips DESC;
```

- שאילתא: נהגים ללא נסיעות

```sql
SELECT driver_id, driver_fullname, licensetype, phone
FROM view_driver_workload
WHERE total_assigned_trips = 0;
```

#### תמונות מסך של המבט והשאילתות

![CREATE VIEW view_driver_workload](screenshots/Screenshot%202026-06-10%20201024.png)
![SELECT * FROM view_driver_workload](screenshots/Screenshot%202026-06-10%20201643.png)

---

## אלגוריתם אינודס לאחור ליצירת ERD

### קוד פסאודו

```python
# קלט: list_of_tables (מכיל שמות טבלאות, שמות עמודות, PK ו-FK)
# פלט: ERD_Model

FOR each table IN list_of_tables:
    Create_Entity(table.name)
    FOR each column IN table.columns:
        IF column.is_PK:
            Mark_As_Identifier(column)
        ELSE:
            Add_Attribute(column)

FOR each table IN list_of_tables:
    FOR each FK IN table.foreign_keys:
        # זיהוי סוג קשר לפי מפתחות זרים
        IF table.FK.is_unique:
            Add_Relationship(table, FK.target, type="1:1")
        ELSE:
            Add_Relationship(table, FK.target, type="1:N")

# איחוד קשרים רבים-לרבים
FOR each table IN list_of_tables:
    IF table.has_only_two_FKs:
        Remove_Table(table)
        Add_Relationship(table.FK1.target, table.FK2.target, type="N:M")
```

### הסבר התהליך (צעדים לוגיים)

- זיהוי ישויות: כל טבלה ב-DSD הופכת לישות ב-ERD.
- זיהוי תכונות: העמודות הרגילות הופכות לתכונות של הישות.
- זיהוי מזהים: מפתחות ראשיים (`PK`) מסומנים כמזהי ישות.

#### פענוח קשרים

- 1:N: כל Foreign Key בטבלה "רבים" שמצביע לטבלה "אחת" יוצר קשר של 1 ל-N.
- 1:1: אם ה-FK מוגדר כ-`UNIQUE`, הקשר נחשב 1:1.
- N:M: אם טבלה מכילה רק שני Foreign Keys ואין בה מידע נוסף, זו טבלת קישור. האלגוריתם מוחק אותה ומחליף אותה בקשר N:M ישיר בין הטבלאות המקושרות.

#### חובה/רשות

- אם ה-FK מוגדר כ-`NOT NULL`, הקשר מוגדר כחובה (השתתפות מלאה).
- אם ה-FK מאפשר `NULL`, הקשר מוגדר כרשות.

---

## סיכום

README זה בנוי על המידע הקיים בתיקיית `ex3`, כולל:

- קבצי DSD ו-ERD
- קוד אינטגרציה מלא מתוך `scripts/Integrate.sql`
- קוד מבטים ושאילתות מתוך `scripts/Views.sql`
- אלגוריתם אינודס לאחור מלא
- תמונות מסך מקבצי `screenshots`

