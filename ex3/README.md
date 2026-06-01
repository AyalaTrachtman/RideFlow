# דוח — שלב ג' (אינטגרציה)

זהו דוח שלב ג' עבור פרויקט RideFlow. הדוח משלב תמונות DSD ו-ERD, החלטות אינטגרציה, תיאור התהליך, קטעי SQL אמיתיים מתוך הקבצים ואלגוריתם אינוד לאחור.

## מה הדוח כולל

- תמונות מסך של תרשימי DSD ו-ERD שנמצאו ב-`ex3/screenshots`
- החלטות שנעשו בשלב האינטגרציה
- הסבר מילולי של התהליך ושל הפקודות שבוצעו
- לכל מבט: תיאור מילולי, `SELECT *` לדוגמה (10 רשומות), ופלט
- לכל שאילתה על מבט: תיאור מילולי, קוד השאילתה, ופלט

## קבצים מרכזיים לעבודה

- DSD של האגף החדש: `ex3/drive_dsd`
- ERD אגף חדש: `ex3/erdplus (5).png` ו-`ex3/erdplus (6).png`
- ERD משותף: `ex3/merged_dsd`
- DSD לאחר אינטגרציה: `ex3/merged_dsd`
- פקודות שינוי ויצירה: `ex3/scripts/Integrate.sql`
- פקודות מבטים ושאילתות: `ex3/scripts/Views.sql`
- קבצים נוספים שנמצאים בתיקיה: `ex3/27.05 (1).2026`

## תמונות מסך נבחרות

- תרשים ERD משולב מהאינטגרציה:

  ![ERD משולב](ex3/screenshots/Screenshot%202026-06-01%20123650.png)
  קובץ: `ex3/screenshots/Screenshot 2026-06-01 123650.png`

- יצירת המבט `trip_full_details_view` ו-`CREATE VIEW` עבור הנתונים הממוזגים:

  ![יצירת מבט trip_full_details_view](ex3/screenshots/Screenshot%202026-06-01%20105159.png)
  קובץ: `ex3/screenshots/Screenshot 2026-06-01 105159.png`

- פלט שאילתה 1: סיכום נסיעות לפי נהג מתוך `trip_full_details_view`:

  ![פלט סיכום נסיעות לפי נהג](ex3/screenshots/Screenshot%202026-06-01%20105612.png)
  קובץ: `ex3/screenshots/Screenshot 2026-06-01 105612.png`

- פלט שאילתה 2: נסיעות פעילות לפי נהגים מתוך `public.active_trip_details`:

  ![פלט נסיעות פעילות לפי נהגים](ex3/screenshots/Screenshot%202026-06-01%20111237.png)
  קובץ: `ex3/screenshots/Screenshot 2026-06-01 111237.png`

> שים לב: התמונות בתיקייה `ex3/screenshots` משמשות להמחשה של שלבי יצירת ה-views, אימות הקוד וטעינת התוצאות.

## החלטות עיקריות שנעשו במהלך האינטגרציה

1. שימור הנתונים המקוריים באמצעות סכמות ביניים:
   - כל טבלאות מקור ראשון הועתקו ל-`public1`
   - כל טבלאות מקור שני הועברו ל-`public2`
   - הטבלאות הסופיות נבנו ב-`public`

2. איחוד שדות בטבלאות המאוחדות:
   - שדות ייחודיים למקור אחד הוגדרו כ-`NULL` בטבלאות המאוחדות
   - טיפוסי נתונים הורחבו לפי הערך המקסימלי הנדרש כדי לשמור על כל המידע
   - שדות חובה במקור אחד הועברו ל-`NULL` כאשר אינם קיימים במקור השני

3. טיפול ב-offset עבור מזהים:
   - הועברו מזהים מ-`public1` כפי שהם
   - עבור `public2` הוסיפו את ה-`MAX(ID)` של `public1` כדי למנוע התנגשויות
   - שמרו על קשרים רלציוניים בין טבלאות באמצעות עדכון מזהים בטבלאות התלויות

4. הקמת אינדקסים ומשמעותם:
   - נוספו אינדקסים על שדות חיפוש מרחיבים (כגון שם נוסע, תאריך נסיעה וקיבולת רכב)
   - אינדקסים אלו תורמים לשיפור ביצועים בשאילתות על מבטים וטבלאות גדולות

5. שמירה על מבנה קשרים תקני:
   - טבלאות רבים-לרבים נשמרו כתיבות קישור (כגון `includes`, `routestop`)
   - טבלאות חד-לרבים נשמרו עם מפתחות זרים ויחסי `FOREIGN KEY`

## הסבר מילולי של התהליך והפקודות

השלב המרכזי באינטגרציה היה ליצור טבלאות `public` מאוחדות על בסיס הקבצים ב-`public1` ו-`public2`, ואז להזין אותן עם נתונים בהתאם למפתחות הייחודיים והקשרים.

### 1. יצירת הטבלאות המאוחדות וניתוח מבנה

להלן קטעים מתוך `ex3/scripts/Integrate.sql` המייצגים את המבנה המאוחד:

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

### 2. הזרמת נתונים מהסכמות הזמניות

```sql
INSERT INTO public.driver (driver_id, driver_fullname, licensetype, phone)
SELECT 
    driver_id, 
    driver_fullname, 
    licensetype, 
    NULL -- אין שדה טלפון ב-public1, אז נכניס NULL
FROM public1.driver;
```

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
    route_id, 
    route_name, 
    NULL, -- אין startlocation ב-public1
    NULL, -- אין endlocation ב-public1
    NULL  -- אין estimatedduration ב-public1
FROM public1.route;
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
    v.plate_number, -- מושך את מספר הלוחית המתאים מתוך הטבלה המאוחדת
    t2.status
FROM public2.trip t2
JOIN public.vehicle v ON t2.busid = v.bus_id;
```

## אלגוריתם אינוד לאחור

האלגוריתם עובר על הטבלאות ומזהה את הקשרים הנסתרים בתוך ה-Foreign Keys. הוא מייצר ERD ישירות מתוך מידע על טבלאות, מפתחות ראשיים וזרות.

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

- זיהוי ישויות: כל טבלה ב-DSD הופכת לישות ב-ERD. העמודות הרגילות הופכות לתכונות, וה-PK הופך למזהה הישות.
- פענוח קשרים:
  1. 1:N: כל Foreign Key בטבלה "רבים" שמצביע לטבלה "אחת" יוצר קשר של 1 ל-N.
  2. 1:1: נוצר רק אם ה-FK מוגדר כ-`UNIQUE`.
- זיהוי N:M: אם טבלה מכילה רק שני Foreign Keys ואין בה שום עמודות מידע אחרות, זו טבלת קישור. האלגוריתם מוחק אותה מהתרשים ומחליף אותה בקשר N:M בין שתי הישויות.
- חובה/רשות: אם ה-FK הוא `NOT NULL`, הקשר מוגדר כחובה (השתתפות מלאה). אם הוא מאפשר `NULL`, הקשר מוגדר כרשות.

## קטעים מתוך `ex3/scripts/Views.sql`

### יצירת המבט `trip_full_details_view`

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

### שאילתות על `trip_full_details_view`

```sql
SELECT 
    driver_fullname, 
    COUNT(*) AS total_trips
FROM 
    trip_full_details_view
GROUP BY 
    driver_fullname;
```

- פלט הסתכלות: הוצג בתמונת המסך `ex3/screenshots/Screenshot 2026-06-01 105612.png`.

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

- פלט הסתכלות: הוצג בתמונת המסך `ex3/screenshots/Screenshot 2026-06-01 105612.png`.

### יצירת המבט `public.active_trip_details`

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

### שאילתות על `public.active_trip_details`

```sql
SELECT 
    driver_name, 
    COUNT(tripid) AS active_trips_count
FROM public.active_trip_details
GROUP BY driver_name
ORDER BY active_trips_count DESC;
```

- פלט הסתכלות: הוצג בתמונת המסך `ex3/screenshots/Screenshot 2026-06-01 111237.png`.

```sql
SELECT 
    bus_license_plate, 
    driver_name,
    tripdate
FROM public.active_trip_details
ORDER BY tripdate ASC;
```

- פלט הסתכלות: הוצג בתמונת המסך `ex3/screenshots/Screenshot 2026-06-01 111237.png`.

## הוראות להשלמת הדוח

- לכל מבט: כתוב תיאור מילולי מלא, החלף את `view_name` בשם המבט, וציין את הפלט הקיים בתמונות המסך.
- לכל שאילתה על מבט: הצג את מטרת השאילתה, הדבק את קוד השאילתה ואת הפלט כפי שמופיע בתמונת המסך.
- אם יש קובץ גיבוי עדכני בשם `backup3`, יש להוסיף סעיף מתועד על מקורו ועל תכולת הגיבוי.
- אם תרצה, ניתן להוסיף גם תיאור של הקבצים החדשים שנוצרו בתהליך המיזוג ומשמעותם.

## דוגמאות של מבטים ודוח מבוסס שאילתות

### תיאור מילולי למבט `trip_full_details_view`

מבט זה מאחד נתונים מטבלת `trip`, `route` ו-`driver` כדי להראות פרטי נסיעה מלאים עם שם מסלול ושם נהג במקום מזהים.

- שאילתה לשליפת דוגמה (10 רשומות):

```sql
SELECT * FROM trip_full_details_view LIMIT 10;
```

- תיאור קוד השאילתה:
  שאילתא זו קוראת את כל העמודות של המבט ומציגה את 10 הרשומות הראשונות כדי לבדוק את תקינות הנתונים המאוחדים.

- פלט:

```
-- פלט: כפי שמופיע בתמונת המסך המתאימה, לא צריך להריץ SQL חדש.
```

### תיאור מילולי למבט `public.active_trip_details`

מבט זה מציג רק נסיעות במצב `Active`, כולל שם הנהג ומספר לוחית הרכב, מתוך טבלאות `trip`, `driver` ו-`bus`.

- שאילתה לשליפת דוגמה (10 רשומות):

```sql
SELECT * FROM public.active_trip_details LIMIT 10;
```

- תיאור קוד השאילתה:
  שאילתא זו בודקת את המידע הפעיל על הנסיעות המאוחדות ומוודאת שהקשרים בין הטבלאות `trip`, `driver` ו-`bus` תקינים.

- פלט:

```
-- פלט: כפי שמופיע בתמונת המסך המתאימה, לא צריך להריץ SQL חדש.
```
