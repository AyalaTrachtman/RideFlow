# דוח פרויקט מסדי נתונים - שלב ב'

## מבוא
**הסבר כללי על הצורות:**
- **צורה א' (JOIN / GROUP BY):** עושה הצטרפות של טבלאות ומבצעת חישוב קבוצתי אחד, מה שמאפשר לנצל אופטימיזציית מסד הנתונים (Hash/Sort Aggregates) ומבצע חיפוש רק פעם אחת.
- **צורה ב' (Sub‑query):** כוללת תת‑שאילתות תלויות (Correlated) או קביעת ערכים בתוך SELECT/WHERE; עבור כל שורה הראשית מתבצעת הרצה של תת‑שאילתה, מה שיכול לגרום ל‑O(N²) וביצועים משמעותיים פחותים בטבלאות גדולות.
- **בדרך כלל, במקרים שבהם יש צורך באגרגציה או סינון על בסיס ערכי טבלאות מרובות, צורה א' עדיפה מבחינת זמן ריצה וניצול משאבי DB.**

---

## 1. שאילתות SELECT כפולות (שתי צורות כתיבה)

עבור כל שאילתה מוצגות שתי גרסאות: צורה א' (לרוב שימוש ב-JOIN או GROUP BY) וצורה ב' (שימוש ב-Sub-query).

### שאילתה 1: נסיעות עם כמות הרשמות מעל הממוצע
**תיאור:** איתור נסיעות שמספר הנוסעים שנרשמו אליהן גבוה מהממוצע הכללי של הרשמות לנסיעה במערכת.

**צורה א' (JOIN):**
```sql
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
```
**צילום הרצה ותוצאה - צורה א':**
![הרצה - צורה א'](screenshots/Screenshot%202026-05-06%20083734.png)
![תוצאה - צורה א'](screenshots/Screenshot%202026-05-06%20084137.png)

**צורה ב' (Sub-query):**
```sql
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
```
**צילום הרצה ותוצאה - צורה ב':**
![הרצה - צורה ב'](screenshots/Screenshot%202026-05-06%20083746.png)
![תוצאה - צורה ב'](screenshots/Screenshot%202026-05-06%20084146.png)

**הסבר טכני והשוואת יעילות:**
צורה א' משתמשת ב-`JOIN` ו-`GROUP BY` המאפשרים למנוע מסד הנתונים לבצע את החישוב בצורה אגרגטיבית ויעילה יותר על ידי סריקה מרוכזת של הטבלאות. צורה ב' משתמשת ב-`Correlated Sub-query` בתוך ה-SELECT וה-WHERE, מה שעלול לגרום להרצת תת-השאילתה עבור כל שורה בטבלה הראשית (O(N^2) במקרים מסוימים). לכן, צורה א' יעילה יותר בבסיסי נתונים גדולים.

---

### שאילתה 2: נהגים שביצעו יותר מ-3 נסיעות
**תיאור:** שליפת שמות הנהגים שרשומים על שמם יותר מ-3 נסיעות במערכת.

**צורה א' (GROUP BY + HAVING):**
```sql
SELECT d.driver_fullname,
       COUNT(t.trip_id) AS trip_count
FROM public.driver d
JOIN public.trip t ON d.driver_id = t.driver_id
GROUP BY d.driver_fullname
HAVING COUNT(t.trip_id) > 3;
```
**צילום הרצה ותוצאה - צורה א':**
![הרצה - צורה א'](screenshots/Screenshot%202026-05-06%20081907.png)
![תוצאה - צורה א'](screenshots/Screenshot%202026-05-06%20082131.png)

**צורה ב' (Sub-query):**
```sql
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
```
**צילום הרצה ותוצאה - צורה ב':**
![הרצה - צורה ב'](screenshots/Screenshot%202026-05-06%20081937.png)
![תוצאה - צורה ב'](screenshots/Screenshot%202026-05-06%20082316.png)

**הסבר טכני והשוואת יעילות:**
צורה א' מנצלת את מנגנון ה-Hash Aggregate או Sort Aggregate של מסד הנתונים לאחר ה-Join, מה שחוסך זמן ריצה. צורה ב' מחשבת את ה-Count פעמיים (פעם אחת להצגה ופעם אחת לסינון), מה שגורם לכפילות מיותרת בעבודה.

---

### שאילתה 3: נהגים שנוהגים ברכבים גדולים (קיבולת מעל 10)
**תיאור:** הצגת שמות הנהגים הייחודיים ששובצו לנסיעה עם רכב שהקיבולת שלו גדולה מ-10.

**צורה א' (JOIN):**
```sql
SELECT DISTINCT d.driver_fullname 
FROM public.driver d 
JOIN public.trip t ON d.driver_id = t.driver_id 
JOIN public.vehicle v ON t.plate_number = v.plate_number 
WHERE v.capacity > 10
ORDER BY d.driver_fullname;
```
**צילום הרצה ותוצאה - צורה א':**
![הרצה - צורה א'](screenshots/Screenshot%202026-05-06%20083933.png)
![תוצאה - צורה א'](screenshots/Screenshot%202026-05-06%20082631.png)

**צורה ב' (Sub-query):**
```sql
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
```
**צילום הרצה ותוצאה - צורה ב':**
![הרצה - צורה ב'](screenshots/Screenshot%202026-05-06%20083946.png)
![תוצאה - צורה ב'](screenshots/Screenshot%202026-05-06%20082802.png)

**הסבר טכני והשוואת יעילות:**
בשאילתה זו, ה-`JOIN` (צורה א') לרוב עדיף כיוון שאופטימייזרים מודרניים יודעים לבצע `Nested Loops` או `Hash Join` בצורה מיטבית. עם זאת, בשימוש ב-`IN` (צורה ב'), אם רשימת הלוחיות הפנימית קטנה מאוד, הביצועים עשויים להיות דומים. בדרך כלל נעדיף JOIN לקריאות ויעילות.

---

### שאילתה 4: כמות הרשמות לכל מסלול
**תיאור:** ספירת סך כל ההרשמות שבוצעו עבור כל מסלול נסיעה קיים.

**צורה א' (Group By):**
```sql
SELECT r.route_name, COUNT(reg.reg_id) as total_registrations
FROM public.route r 
LEFT JOIN public.trip t ON r.route_id = t.route_id 
LEFT JOIN public.registration reg ON t.trip_id = reg.trip_id 
GROUP BY r.route_id, r.route_name 
ORDER BY r.route_name, r.route_id;
```
**צילום הרצה ותוצאה - צורה א':**
![הרצה - צורה א'](screenshots/Screenshot%202026-05-06%20091529.png)
![תוצאה - צורה א'](screenshots/Screenshot%202026-05-06%20090209.png)

**צורה ב' (Sub-query ב-SELECT):**
```sql
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
```
**צילום הרצה ותוצאה - צורה ב':**
![הרצה - צורה ב'](screenshots/Screenshot%202026-05-06%20091537.png)
![תוצאה - צורה ב'](screenshots/Screenshot%202026-05-06%20090359.png)

**הסבר טכני והשוואת יעילות:**
צורה א' משתמשת ב-`LEFT JOIN` המבטיח שגם מסלולים ללא הרשמות יופיעו (עם ערך 0). צורה ב' מבצעת תת-שאילתה מקוננת פעמיים (אחת בתוך השנייה) לכל שורה בטבלת המסלולים, מה שיוצר עומס כבד מאוד על המעבד ככל שכמות הנתונים גדלה.

---

## 2. שאילתות SELECT נוספות

### שאילתה 5: ספירת נסיעות לפי יום בשבוע
**תיאור:** חלוקת הנסיעות לימי השבוע (0-6) וספירת כמות הנסיעות בכל יום.
```sql
SELECT EXTRACT(DOW FROM trip_date) as day_of_week, 
       COUNT(*) as trip_count
FROM public.trip 
GROUP BY day_of_week;
```
**צילום הרצה ותוצאה:**
![שאילתה 5 הרצה](screenshots/Screenshot%202026-05-06%20091512.png)
![שאילתה 5 תוצאה](screenshots/Screenshot%202026-05-06%20083237.png)

### שאילתה 6: איתור נוסעים "מתמידים" (מעל 5 הרשמות)
**תיאור:** רשימת נוסעים שנרשמו ליותר מ-5 נסיעות שונות.
```sql
SELECT p.pass_fullname, 
       COUNT(r.reg_id) as registration_count
FROM public.passenger p 
JOIN public.registration r ON p.pass_id = r.pass_id 
GROUP BY p.pass_fullname 
HAVING COUNT(r.reg_id) > 5;
```
**צילום הרצה ותוצאה:**
![שאילתה 6 הרצה](screenshots/Screenshot%202026-05-06%20005012.png)
![שאילתה 6 תוצאה](screenshots/Screenshot%202026-05-06%20005140.png)

### שאילתה 7: נסיעות עם מעט מקומות פנויים
**תיאור:** הצגת נסיעות שנותרו בהן פחות מ-2 מקומות פנויים.
```sql
SELECT t.trip_id, 
       r.route_name, 
       t.available_seats
FROM public.trip t 
JOIN public.route r ON t.route_id = r.route_id
WHERE t.available_seats < 2;
```
**צילום הרצה ותוצאה:**
![שאילתה 7 הרצה](screenshots/Screenshot%202026-05-06%20005044.png)
![שאילתה 7 תוצאה](screenshots/Screenshot%202026-05-06%20005146.png)

### שאילתה 8: רשימת תחנות ומסלוליהן
**תיאור:** הצגת כל התחנות והמסלולים שבהן הן עוברות, ממוין לפי שם מסלול.
```sql
SELECT s.stop_name, 
       r.route_name
FROM public.stop s 
JOIN public.includes i ON s.stop_id = i.stop_id 
JOIN public.route r ON i.route_id = r.route_id
ORDER BY r.route_name ASC;
```
**צילום הרצה ותוצאה:**
![שאילתה 8 הרצה](screenshots/Screenshot%202026-05-06%20005255.png)
![שאילתה 8 תוצאה](screenshots/Screenshot%202026-05-06%20005325.png)

---

## 3. פעולות עדכון (UPDATE) ומחיקה (DELETE)

### עדכון 1: עדכון סטטוס הרשמות עבר
**תיאור:** עדכון סטטוס ההרשמה ל'Completed' עבור כל הנסיעות שתאריכן עבר.
**לפני השינוי:**
![לפני העדכון 1](screenshots/Screenshot%202026-05-06%20005314.png)
**קוד ההרצה:**
```sql
UPDATE public.registration 
SET status = 'Completed' 
WHERE trip_id IN (
    SELECT trip_id 
    FROM public.trip 
    WHERE trip_date < CURRENT_DATE
);
```
**אחרי השינוי:**
![אחרי העדכון 1](screenshots/Screenshot%202026-05-06%20005335.png)

### עדכון 2: הורדת מקום פנוי בנסיעה ספציפית
**תיאור:** הפחתת מקום פנוי אחד בנסיעה מספר 101 בעקבות רישום ידני.
**לפני השינוי:**
![לפני העדכון 2](screenshots/Screenshot%202026-05-06%20005346.png)
**קוד ההרצה:**
```sql
UPDATE public.trip 
SET available_seats = available_seats - 1 
WHERE trip_id = 101;
```
**אחרי השינוי:**
![אחרי העדכון 2](screenshots/Screenshot%202026-05-06%20005358.png)

### עדכון 3: עדכון סוג רישיון לנהג ספציפי
**תיאור:** שינוי דרגת הרישיון של הנהג 'ישראל ישראלי' ל-'C1' כדי לשקף שדרוג בהסמכה.
**לפני השינוי:**
![לפני העדכון 3](screenshots/Screenshot%202026-05-06%20005414.png)
**קוד ההרצה:**
```sql
UPDATE public.driver 
SET licensetype = 'C1' 
WHERE driver_fullname = 'ישראל ישראלי';
```
**אחרי השינוי:**
![אחרי העדכון 3](screenshots/Screenshot%202026-05-06%20005515.png)

---

### מחיקה 1: מחיקת נהגים ללא שיבוץ
**תיאור:** הסרת נהגים מהמערכת שמעולם לא שובצו לנסיעה.
**לפני השינוי:**
![לפני מחיקה 1](screenshots/Screenshot%202026-05-06%20011442.png)
**קוד ההרצה:**
```sql
DELETE FROM public.driver 
WHERE driver_id NOT IN (SELECT DISTINCT driver_id FROM public.trip)
AND driver_id = (
    SELECT driver_id 
    FROM public.driver 
    WHERE driver_id NOT IN (SELECT DISTINCT driver_id FROM public.trip)
    LIMIT 1
);
```
**אחרי השינוי:**
![אחרי מחיקה 1](screenshots/Screenshot%202026-05-06%20011454.png)

### מחיקה 2: מחיקת נסיעות ישנות (לפני 2026-02-02)
**תיאור:** מחיקת נסיעות ישנות. תחילה מוחקים הרשמות מקושרות, ואז את הנסיעות עצמן.
**לפני השינוי:**
![לפני מחיקה 2](screenshots/Screenshot%202026-05-06%20011833.png)
**קוד ההרצה:**
```sql
DELETE FROM public.registration 
WHERE trip_id IN (SELECT trip_id FROM public.trip WHERE trip_date < '2026-02-02');

DELETE FROM public.trip 
WHERE trip_date < '2026-02-02';
```
**אחרי השינוי:**
![אחרי מחיקה 2](screenshots/Screenshot%202026-05-06%20011844.png)

### מחיקה 3: מחיקת רכבים שאינם בשימוש
**תיאור:** מחיקת רכבים מהמערכת שמעולם לא שובצו לנסיעה כלשהי.
**לפני השינוי:**
![לפני מחיקה 3](screenshots/Screenshot%202026-05-06%20011911.png)
**קוד ההרצה:**
```sql
DELETE FROM public.vehicle
WHERE plate_number NOT IN (
    SELECT DISTINCT plate_number 
    FROM public.trip
);
```
**אחרי השינוי:**
![אחרי מחיקה 3](screenshots/Screenshot%202026-05-06%20012037.png)

---

## 4. אילוצי שלמות (Constraints)

### אילוץ 1: קיבולת רכב חיובית
**תיאור:** הוספת אילוץ המבטיח שקיבולת הרכב תהיה גדולה מ-0.
```sql
ALTER TABLE public.vehicle 
ADD CONSTRAINT check_capacity_positive 
CHECK (capacity > 0);
```
**ניסיון הפרה (הכנסת ערך 0):**
![שגיאת אילוץ קיבולת](screenshots/Screenshot%202026-05-06%20012304.png)

### אילוץ 2: מקומות פנויים לא שליליים
**תיאור:** מניעת מצב של רישום יתר (Overbooking).
```sql
ALTER TABLE public.trip 
ADD CONSTRAINT check_available_seats_non_negative 
CHECK (available_seats >= 0);
```
**ניסיון הפרה (הכנסת ערך שלילי):**
![שגיאת אילוץ מקומות פנויים](screenshots/Screenshot%202026-05-06%20012313.png)

---

## 5. טרנזקציות (Rollback & Commit)

### הדגמת Rollback
**תהליך:** פתיחת טרנזקציה, שינוי סוג רישיון לנהג, בדיקת השינוי הזמני, וביצוע ביטול (Rollback).
**מצב בסיס הנתונים במהלך טרנזקציה (לפני ביטול):**
![מהלך הטרנזקציה](screenshots/Screenshot%202026-05-06%20022638.png)
**אחרי Rollback:**
![אחרי Rollback](screenshots/Screenshot%202026-05-06%20022645.png)

**הסבר:** לאחר הביטול (Rollback) הנתונים חזרו למצב הקודם ללא שינוי.

### הדגמת Commit
**תהליך:** עדכון קיבולת רכב בתוך טרנזקציה ואישור השינוי לצמיתות.
**לפני ה-Commit:**
![לפני ה-Commit](screenshots/Screenshot%202026-05-06%20023000.png)
**אחרי ה-Commit:**
![אחרי ה-Commit](screenshots/Screenshot%202026-05-06%20023009.png)

---

## 6. אינדקסים (Indexes)

עבור כל אינדקס מוצגים קודם כל זמני הריצה מתוך EXPLAIN ANALYZE **לפני** בניית האינדקס (זמן ריצה ארוך), ולאחר מכן **אחרי** בניית האינדקס (זמן קצר יותר).

### אינדקס 1: אינדקס על תאריך נסיעה (`trip_date`) בטבלת `trip`
**הסבר:** האצת חיפושים וסינונים לפי תאריכים. האינדקס משפר את הביצועים על ידי יצירת מבנה המאפשר חיפוש מהיר במקום סריקה ליניארית מלאה (Full Table Scan).

**לפני האינדקס (זמן ריצה ארוך, למשל 11.234 ms):**
![לפני אינדקס תאריך](screenshots/Screenshot%202026-05-06%20024128.png)

**בניית האינדקס:**
```sql
CREATE INDEX idx_trip_date ON public.trip(trip_date);
```

**אחרי האינדקס (זמן ריצה קצר, למשל 0.034 ms):**
![אחרי אינדקס תאריך](screenshots/Screenshot%202026-05-06%20024138.png)

---

### אינדקס 2: אינדקס על שם נוסע (`pass_fullname`) בטבלת `passenger`
**הסבר:** שיפור מהירות החיפוש עבור שמות נוסעים (חיפוש טקסטואלי).

**לפני האינדקס (זמן ריצה ארוך, למשל 1.523 ms):**
![לפני אינדקס שם](screenshots/Screenshot%202026-05-06%20024204.png)

**בניית האינדקס:**
```sql
CREATE INDEX idx_passenger_name ON public.passenger(pass_fullname);
```

**אחרי האינדקס (זמן ריצה קצר, למשל 0.045 ms):**
![אחרי אינדקס שם](screenshots/Screenshot%202026-05-06%20024218.png)

---

### אינדקס 3: אינדקס על קיבולת רכב (`capacity`) בטבלת `vehicle`
**הסבר:** ייעול שליפת רכבים לפי מספר המושבים הפנויים לצורך התאמת נסיעה.

**לפני האינדקס (זמן ריצה ארוך, למשל 0.840 ms):**
![לפני אינדקס קיבולת](screenshots/Screenshot%202026-05-06%20024654.png)

**בניית האינדקס:**
```sql
CREATE INDEX idx_vehicle_capacity ON public.vehicle(capacity);
```

**אחרי האינדקס (זמן ריצה קצר, למשל 0.021 ms):**
![אחרי אינדקס קיבולת](screenshots/Screenshot%202026-05-06%20024754.png)
