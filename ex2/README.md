# דוח פרויקט מסדי נתונים - שלב ב'

## מבוא
דוח זה מציג את עבודת הפיתוח שבוצעה בשלב ב' של הפרויקט. השלב כולל כתיבת שאילתות מורכבות, פעולות עדכון ומחיקה, הגדרת אילוצי שלמות, ניהול טרנזקציות ושימוש באינדקסים לשיפור ביצועים.

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

**צילומי מסך:**
![שאילתה 1 צורה א'](screenshots/Screenshot%202026-05-06%20083734.png)
![שאילתה 1 צורה ב'](screenshots/Screenshot%202026-05-06%20083746.png)

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

**צילומי מסך:**
![שאילתה 2 צורה א'](screenshots/Screenshot%202026-05-06%20081907.png)
![שאילתה 2 צורה ב'](screenshots/Screenshot%202026-05-06%20081937.png)

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

**צילומי מסך:**
![שאילתה 3 צורה א'](screenshots/Screenshot%202026-05-06%20083933.png)
![שאילתה 3 צורה ב'](screenshots/Screenshot%202026-05-06%20083946.png)

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

**צילומי מסך:**
![שאילתה 4 צורה א'](screenshots/Screenshot%202026-05-06%20091529.png)
![שאילתה 4 צורה ב'](screenshots/Screenshot%202026-05-06%20091537.png)

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
![שאילתה 5](screenshots/Screenshot%202026-05-06%20091512.png)

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
![שאילתה 6](screenshots/Screenshot%202026-05-06%20005012.png)

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
![שאילתה 7](screenshots/Screenshot%202026-05-06%20005044.png)

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
![שאילתה 8](screenshots/Screenshot%202026-05-06%20005255.png)

---

## 3. פעולות עדכון (UPDATE) ומחיקה (DELETE)

### עדכון 1: עדכון סטטוס הרשמות עבר
**תיאור:** שינוי סטטוס ל-'Completed' עבור כל הנסיעות שתאריכן חלף.
```sql
UPDATE public.registration 
SET status = 'Completed' 
WHERE trip_id IN (
    SELECT trip_id 
    FROM public.trip 
    WHERE trip_date < CURRENT_DATE
);
```
![ביצוע העדכון](screenshots/Screenshot%202026-05-06%20005314.png)

### מחיקה 1: מחיקת נהגים ללא שיבוץ
**תיאור:** הסרת נהגים מהמערכת שמעולם לא שובצו לנסיעה.
```sql
DELETE FROM public.driver 
WHERE driver_id NOT IN (SELECT DISTINCT driver_id FROM public.trip);
```
![לפני המחיקה](screenshots/Screenshot%202026-05-06%20011442.png)
![אחרי המחיקה](screenshots/Screenshot%202026-05-06%20011454.png)

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
![מהלך הטרנזקציה](screenshots/Screenshot%202026-05-06%20022638.png)
![אחרי Rollback](screenshots/Screenshot%202026-05-06%20022645.png)

### הדגמת Commit
**תהליך:** עדכון קיבולת רכב בתוך טרנזקציה ואישור השינוי לצמיתות.
![לפני ה-Commit](screenshots/Screenshot%202026-05-06%20023000.png)
![אחרי ה-Commit](screenshots/Screenshot%202026-05-06%20023009.png)

---

## 6. אינדקסים (Indexes)

### אינדקס על תאריך נסיעה (`trip_date`)
**לפני האינדקס:** זמן ריצה ארוך עקב סריקה מלאה של הטבלה.
**אחרי האינדקס:** זמן הריצה התקצר משמעותית.
![ביצועי אינדקס תאריך](screenshots/Screenshot%202026-05-06%20024138.png)
 העדכון](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20005314.png)

### מחיקה 1: מחיקת נהגים ללא שיבוץ
**תיאור:** הסרת נהגים מהמערכת שמעולם לא שובצו לנסיעה.
```sql
DELETE FROM public.driver 
WHERE driver_id NOT IN (SELECT DISTINCT driver_id FROM public.trip);
```
![לפני המחיקה](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20011442.png)
![אחרי המחיקה](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20011454.png)

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
![שגיאת אילוץ קיבולת](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20012304.png)

### אילוץ 2: מקומות פנויים לא שליליים
**תיאור:** מניעת מצב של רישום יתר (Overbooking).
```sql
ALTER TABLE public.trip 
ADD CONSTRAINT check_available_seats_non_negative 
CHECK (available_seats >= 0);
```
**ניסיון הפרה (הכנסת ערך שלילי):**
![שגיאת אילוץ מקומות פנויים](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20012313.png)

---

## 5. טרנזקציות (Rollback & Commit)

### הדגמת Rollback
**תהליך:** פתיחת טרנזקציה, שינוי סוג רישיון לנהג, בדיקת השינוי הזמני, וביצוע ביטול (Rollback).
![מהלך הטרנזקציה](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20022638.png)
![אחרי Rollback](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20022645.png)

### הדגמת Commit
**תהליך:** עדכון קיבולת רכב בתוך טרנזקציה ואישור השינוי לצמיתות.
![לפני ה-Commit](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20023000.png)
![אחרי ה-Commit](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20023009.png)

---

## 6. אינדקסים (Indexes)

### אינדקס על תאריך נסיעה (`trip_date`)
**לפני האינדקס:** זמן ריצה ארוך עקב סריקה מלאה של הטבלה.
**אחרי האינדקס:** זמן הריצה התקצר משמעותית.
![ביצועי אינדקס תאריך](file:///c:/screenshotsEx2/Screenshot%202026-05-06%20024138.png)

**הסבר:** האינדקס משפר את ביצועי השאילתות על ידי יצירת מבנה נתונים מסודר המאפשר חיפוש לוגריתמי במקום ליניארי.
