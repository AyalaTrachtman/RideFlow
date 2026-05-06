-- 1. אינדקס על תאריך נסיעה (trip_date) בטבלת trip
-- הסבר: האצת חיפושים וסינונים לפי תאריכים.

-- א. בדיקה לפני: סריקה מלאה של הטבלה
SELECT * FROM public.trip WHERE trip_date = '2026-05-01';

-- ב. יצירת האינדקס
CREATE INDEX idx_trip_date ON public.trip(trip_date);

-- ג. בדיקה אחרי: שימוש באינדקס לקיצור זמן הריצה
SELECT * FROM public.trip WHERE trip_date = '2026-05-01';


-- 2. אינדקס על שם נוסע (pass_fullname) בטבלת passenger
-- הסבר: שיפור מהירות החיפוש עבור שמות נוסעים (חיפוש טקסטואלי).

-- א. בדיקה לפני: חיפוש ללא אינדקס 
SELECT * FROM public.passenger WHERE pass_fullname = 'ישראל ישראלי';

-- ב. יצירת האינדקס
CREATE INDEX idx_passenger_name ON public.passenger(pass_fullname);

-- ג. בדיקה אחרי: איתור מהיר דרך מבנה ה-Index
SELECT * FROM public.passenger WHERE pass_fullname = 'ישראל ישראלי';


-- 3. אינדקס על קיבולת רכב (capacity) בטבלת vehicle
-- הסבר: ייעול שליפת רכבים לפי מספר המושבים הפנויים לצורך התאמת נסיעה.

-- א. בדיקה לפני: חיפוש רכבים עם קיבולת גדולה (למשל מעל 30 מקומות)
SELECT * FROM public.vehicle WHERE capacity > 30;

-- ב. יצירת האינדקס
CREATE INDEX idx_vehicle_capacity ON public.vehicle(capacity);

-- ג. בדיקה אחרי: אופטימיזציה של החיפוש לפי קיבולת 
SELECT * FROM public.vehicle WHERE capacity > 30;