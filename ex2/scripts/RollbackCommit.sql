-- הדגמת Rollback (ביטול שינוי)
-- המטרה: להראות ששינוי שבוצע בתוך טרנזקציה לא נשמר אם מבצעים ביטול (אטומיות).

-- 1. מצב לפני: בדיקת הערך המקורי
SELECT driver_id, licensetype 
FROM public.driver WHERE driver_id = 2;

BEGIN;

-- 2. ביצוע השינוי הזמני
UPDATE public.driver SET licensetype = 'TEST_ERROR' WHERE driver_id = 2;

-- 3. בדיקה תוך כדי טרנזקציה: כאן נראה שהערך השתנה ל-TEST_ERROR
SELECT driver_id, licensetype 
FROM public.driver WHERE driver_id = 2;

ROLLBACK;

-- 4. מצב אחרי: בדיקה שהערך חזר לקדמותו בזכות הביטול
SELECT driver_id, licensetype 
FROM public.driver WHERE driver_id = 2;

-- הדגמת Commit (שמירת שינוי)
-- המטרה: להראות ששינוי שבוצע בתוך טרנזקציה נשמר לצמיתות לאחר אישור.

-- 1. מצב לפני: בדיקת קיבולת הרכב
SELECT plate_number, capacity FROM public.vehicle WHERE plate_number = '100-00-001';

BEGIN;

-- 2. ביצוע השינוי (העלאת הקיבולת ב-1)
UPDATE public.vehicle SET capacity = capacity + 1 WHERE plate_number = '100-00-001';

-- 3. בדיקה תוך כדי טרנזקציה (הערך כבר מעודכן בזיכרון הזמני)
SELECT plate_number, capacity FROM public.vehicle WHERE plate_number = '100-00-001';

COMMIT;

-- 4. מצב אחרי: בדיקה שהשינוי ננעל בבסיס הנתונים
SELECT plate_number, capacity FROM public.vehicle WHERE plate_number = '100-00-001';