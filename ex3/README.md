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

### מבט 1: `trip_full_details_view`

#### תיאור מילולי
מבט זה מאחד את נתוני הנסיעות מתוך `trip` עם נתוני המסלול מתוך `route` ונתוני הנהג מתוך `driver`, כדי להציג כתוצאה מידע קריא יותר לטובת דוחות.

#### קוד יצירת המבט

```sql
CREATE VIEW trip_full_details_view AS
SELECT 
    t.trip_id,
    t.trip_date,
    r.route_name,
    d.driver_fullname,
    t.available_seats
FROM 
    public.trip t
JOIN 
    public.route r ON t.route_id = r.route_id
JOIN 
    public.driver d ON t.driver_id = d.driver_id;
```

#### שאילתה דוגמה לשליפת נתונים (10 רשומות)

```sql
SELECT * FROM trip_full_details_view LIMIT 10;
```

#### שאילתות נוספות על המבט

- שאילתא: סיכום נסיעות לפי נהג

```sql
SELECT 
    driver_fullname, 
    COUNT(*) AS total_trips
FROM 
    trip_full_details_view
GROUP BY 
    driver_fullname
ORDER BY 
    total_trips DESC;
```

- שאילתא: נסיעות עם הכי הרבה מקומות פנויים

```sql
SELECT 
    route_name, 
    trip_date, 
    available_seats
FROM 
    trip_full_details_view
ORDER BY 
    available_seats DESC;
```

#### תמונות מסך של השאילתות

![CREATE VIEW trip_full_details_view](screenshots/Screenshot%202026-06-01%20105159.png)
![SELECT * FROM trip_full_details_view](screenshots/Screenshot%202026-06-01%20113005.png)
![COUNT trips per driver](screenshots/Screenshot%202026-06-01%20105612.png)
![Trips with most available seats](screenshots/Screenshot%202026-06-01%20105950.png)

---

### מבט 2: `public.active_trip_details`

#### תיאור מילולי
מבט זה מציג את הנסיעות הפעילות בלבד (`status = 'Active'`) יחד עם שם הנהג ומספר הלוחית של האוטובוס.

#### קוד יצירת המבט

```sql
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
WHERE t.status = 'Active';
```

#### שאילתה דוגמה לשליפת נתונים (10 רשומות)

```sql
SELECT * FROM public.active_trip_details LIMIT 10;
```

#### שאילתות נוספות על המבט

- שאילתא: ספירת נסיעות פעילות לכל נהג

```sql
SELECT 
    driver_name, 
    COUNT(tripid) AS active_trips_count
FROM public.active_trip_details
GROUP BY driver_name
ORDER BY active_trips_count DESC;
```

- שאילתא: רשימת אוטובוסים פעילים ונהגיהם

```sql
SELECT 
    bus_license_plate, 
    driver_name,
    tripdate
FROM public.active_trip_details
ORDER BY tripdate ASC;
```

#### תמונות מסך של השאילתות

![CREATE VIEW active_trip_details](screenshots/Screenshot%202026-06-01%20111004.png)
![COUNT active trips per driver](screenshots/Screenshot%202026-06-01%20111237.png)
![Active buses and their drivers](screenshots/Screenshot%202026-06-01%20111254.png)

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

