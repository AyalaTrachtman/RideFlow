-- שאילתא 1: נסיעות עם כמות הרשמות מעל הממוצע
-- צורה א' (JOIN)
SELECT t.trip_id,
       COUNT(r.reg_id) AS registration_count
FROM public.trip t
LEFT JOIN public.registration r ON t.trip_id = r.trip_id
GROUP BY t.trip_id
HAVING COUNT(r.reg_id) > (
    SELECT AVG(cnt)
    FROM (
        SELECT t2.trip_id, COUNT(r2.reg_id) AS cnt
        FROM public.trip t2
        LEFT JOIN public.registration r2 ON t2.trip_id = r2.trip_id
        GROUP BY t2.trip_id
    ) AS sub
)
ORDER BY t.trip_id; 

-- צורה ב' (Sub-query בלבד) 
SELECT t.trip_id,
       (SELECT COUNT(*) 
        FROM public.registration r 
        WHERE r.trip_id = t.trip_id) AS registration_count
FROM public.trip t
WHERE (SELECT COUNT(*) 
       FROM public.registration r 
       WHERE r.trip_id = t.trip_id) > (
    SELECT AVG(reg_count)
    FROM (
        SELECT (SELECT COUNT(*) 
                FROM public.registration r2 
                WHERE r2.trip_id = t3.trip_id) AS reg_count
        FROM public.trip t3
    ) AS sub
)
ORDER BY t.trip_id;

-- שאילתא 2: נהגים שביצעו יותר מ-3 נסיעות
-- צורה א' שימוש ב-GROUP BY + HAVING

SELECT d.driver_fullname,
       COUNT(t.trip_id) AS trip_count
FROM public.driver d
JOIN public.trip t ON d.driver_id = t.driver_id
GROUP BY d.driver_fullname
HAVING COUNT(t.trip_id) > 3;

-- צורה ב' (Sub-query בלבד)

SELECT d.driver_fullname,
       (
           SELECT COUNT(*)
           FROM public.trip t
           WHERE t.driver_id = d.driver_id
       ) AS trip_count
FROM public.driver d
WHERE (
    SELECT COUNT(*)
    FROM public.trip t
    WHERE t.driver_id = d.driver_id
) > 3;


-- שאילתה 3: נהגים שנוהגים ברכבים גדולים (קיבולת מעל 10)
-- צורה א' (JOIN)
SELECT DISTINCT d.driver_fullname 
FROM public.driver d 
JOIN public.trip t ON d.driver_id = t.driver_id 
JOIN public.vehicle v ON t.plate_number = v.plate_number 
WHERE v.capacity > 10
ORDER BY d.driver_fullname;

-- צורה ב' (Sub-query ב-WHERE)
SELECT driver_fullname 
FROM public.driver 
WHERE driver_id IN (
    SELECT driver_id 
    FROM public.trip 
    WHERE plate_number IN (
        SELECT plate_number 
        FROM public.vehicle 
        WHERE capacity > 10
    )
)
ORDER BY driver_fullname; 

-- שאילתה 4: כמות הרשמות לכל מסלול
-- צורה א' (Group By)
SELECT r.route_name, COUNT(reg.reg_id) as total_registrations
FROM public.route r 
LEFT JOIN public.trip t ON r.route_id = t.route_id 
LEFT JOIN public.registration reg ON t.trip_id = reg.trip_id 
GROUP BY r.route_id, r.route_name 
ORDER BY r.route_name, r.route_id;

-- צורה ב' (Sub-query ב-SELECT)
SELECT r.route_name, 
       (SELECT COUNT(*) 
        FROM public.registration reg 
        WHERE reg.trip_id IN (
            SELECT t.trip_id 
            FROM public.trip t 
            WHERE t.route_id = r.route_id
        )) as total_registrations
FROM public.route r
ORDER BY r.route_name, r.route_id;

-- 5. ספירת נסיעות לפי יום בשבוע (0=ראשון, 6=שבת)
SELECT EXTRACT(DOW FROM trip_date) as day_of_week, 
       COUNT(*) as trip_count
FROM public.trip 
GROUP BY day_of_week;

-- 6. איתור נוסעים "מתמידים" עם מעל 5 הרשמות
SELECT p.pass_fullname, 
       COUNT(r.reg_id) as registration_count
FROM public.passenger p 
JOIN public.registration r ON p.pass_id = r.pass_id 
GROUP BY p.pass_fullname 
HAVING COUNT(r.reg_id) > 5;

-- 7. הצגת כל הנסיעות המתוכננות במערכת כולל שם המסלול ומספר הרכב
SELECT t.trip_id, 
       r.route_name, 
       t.plate_number,
       t.available_seats
FROM public.trip t 
JOIN public.route r ON t.route_id = r.route_id
ORDER BY t.trip_id ASC;

-- 8. רשימת כל התחנות הקיימות במערכת והמסלולים שבהם הן עוברות
SELECT s.stop_name, 
       r.route_name
FROM public.stop s 
JOIN public.includes i ON s.stop_id = i.stop_id 
JOIN public.route r ON i.route_id = r.route_id
ORDER BY r.route_name ASC;

-- עדכון 1: סימון הרשמות כ"הושלמו" עבור נסיעות מהעבר
-- מלל: עדכון סטטוס ההרשמה ל'Completed' עבור כל הנסיעות שתאריכן עבר.
-- לפני השינוי:
SELECT * FROM public.registration;

UPDATE public.registration 
SET status = 'Completed' 
WHERE trip_id IN (
    SELECT trip_id 
    FROM public.trip 
    WHERE trip_date < CURRENT_DATE
);

-- אחרי השינוי :
SELECT * FROM public.registration;


-- עדכון 2: הורדת מקום פנוי בנסיעה ספציפית
-- מלל: הפחתת מקום פנוי אחד בנסיעה מספר 101 בעקבות רישום ידני.
-- לפני השינוי :
SELECT * FROM public.trip WHERE trip_id = 101;

UPDATE public.trip 
SET available_seats = available_seats - 1 
WHERE trip_id = 101;

-- אחרי השינוי :
SELECT * FROM public.trip WHERE trip_id = 101;


-- עדכון 3: עדכון סוג רישיון לנהג ספציפי
-- מלל: שינוי דרגת הרישיון של הנהג כדי לשקף שדרוג בדרגת ההסמכה.

-- 1. לפני השינוי
SELECT * FROM public.driver WHERE driver_fullname = 'ישראל ישראלי';

-- 2. פקודת העדכון 
UPDATE public.driver 
SET licensetype = 'C1' 
WHERE driver_fullname = 'ישראל ישראלי';

-- 3. אחרי השינוי
SELECT * FROM public.driver WHERE driver_fullname = 'ישראל ישראלי';

-- מחיקה 1
-- 1. הצגת כל טבלת הנהגים לפני השינוי
SELECT * FROM public.driver 
ORDER BY driver_id ASC;

-- 2. הרצת המחיקה
-- מחיקת נהג שאינו מופיע בטבלת הנסיעות (Trip)
DELETE FROM public.driver 
WHERE driver_id NOT IN (SELECT DISTINCT driver_id FROM public.trip)
AND driver_id = (
    SELECT driver_id 
    FROM public.driver 
    WHERE driver_id NOT IN (SELECT DISTINCT driver_id FROM public.trip)
    LIMIT 1
);

-- 3. הצגת כל טבלת הנהגים אחרי השינוי
SELECT * FROM public.driver 
ORDER BY driver_id ASC;

-- מחיקה 2: מחיקת נסיעות ישנות (לפני שנת 2026)
-- 1. לפני השינוי:
SELECT * FROM public.trip WHERE trip_date < '2026-02-02';

-- שלב ביניים: מחיקת ההרשמות המקושרות לנסיעות הישנות כדי למנוע שגיאת Foreign Key
DELETE FROM public.registration 
WHERE trip_id IN (SELECT trip_id FROM public.trip WHERE trip_date < '2026-02-02');

-- 2. הרצת המחיקה של הנסיעות עצמן:
DELETE FROM public.trip 
WHERE trip_date < '2026-02-02';

-- 3. אחרי השינוי (לוודא שנמחקו):
SELECT * FROM public.trip;

-- מחיקה 3: מחיקת רכבים שאינם בשימוש
-- מלל: מחיקת רכבים מהמערכת שמעולם לא שובצו לנסיעה כלשהי.
-- לפני השינוי:
SELECT * FROM public.vehicle;

DELETE FROM public.vehicle
WHERE plate_number NOT IN (
    SELECT DISTINCT plate_number 
    FROM public.trip
);

-- אחרי השינוי :
SELECT * FROM public.vehicle;