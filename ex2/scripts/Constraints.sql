-- אילוץ 1: הבטחה שקיבולת הרכב היא תמיד מספר חיובי
-- תיאור השינוי: הוספת אילוץ CHECK לטבלת vehicle המבטיח שבעמודת הקיבולת (capacity) 
-- יופיעו רק מספרים הגדולים מאפס. פעולה זו מונעת הזנת רכבים ללא מקומות ישיבה.

ALTER TABLE public.vehicle 
ADD CONSTRAINT check_capacity_positive 
CHECK (capacity > 0);

-- ניסוי הכנסה לא תקין (אמור להחזיר שגיאה):
INSERT INTO public.vehicle (plate_number, vehicle_type, capacity)
VALUES ('99-888-77', 'Bus', 0);


-- אילוץ 2: הבטחה שמספר המקומות הפנויים בנסיעה לא יהיה שלילי
-- תיאור השינוי: הוספת אילוץ CHECK לטבלת trip המוודא שערך המקומות הפנויים (available_seats) 
-- הוא תמיד אפס ומעלה. אילוץ זה מונע מצב של רישום יתר (Overbooking).

ALTER TABLE public.trip 
ADD CONSTRAINT check_available_seats_non_negative 
CHECK (available_seats >= 0);

-- ניסוי הכנסה לא תקין (אמור להחזיר שגיאה):
INSERT INTO public.trip (trip_id, driver_id, plate_number, route_id, trip_date, available_seats)
VALUES (999, 1, '100-00-001', 1, '2026-06-01', -5);


-- ==========================================
-- אילוץ 1: הבטחה שקיבולת הרכב היא תמיד מספר חיובי
-- ==========================================
-- תיאור השינוי: הוספת אילוץ CHECK לטבלת vehicle המבטיח שבעמודת הקיבולת (capacity) 
-- יופיעו רק מספרים הגדולים מאפס. פעולה זו מונעת הזנת רכבים ללא מקומות ישיבה.

ALTER TABLE public.vehicle DROP CONSTRAINT IF EXISTS check_capacity_positive;
ALTER TABLE public.vehicle 
ADD CONSTRAINT check_capacity_positive 
CHECK (capacity > 0);

-- ניסוי הכנסה לא תקין (אמור להחזיר שגיאה):
INSERT INTO public.vehicle (plate_number, vehicle_type, capacity)
VALUES ('99-888-77', 'Bus', 0);


-- ==========================================
-- אילוץ 2: הבטחה שמספר המקומות הפנויים בנסיעה לא יהיה שלילי
-- ==========================================
-- תיאור השינוי: הוספת אילוץ CHECK לטבלת trip המוודא שערך המקומות הפנויים (available_seats) 
-- הוא תמיד אפס ומעלה. אילוץ זה מונע מצב של רישום יתר (Overbooking).

ALTER TABLE public.trip DROP CONSTRAINT IF EXISTS check_available_seats_non_negative;
ALTER TABLE public.trip 
ADD CONSTRAINT check_available_seats_non_negative 
CHECK (available_seats >= 0);

-- ניסוי הכנסה לא תקין (אמור להחזיר שגיאה):
INSERT INTO public.trip (trip_id, driver_id, plate_number, route_id, trip_date, available_seats)
VALUES (999, 1, '100-00-001', 1, '2026-06-01', -5);


-- אילוץ 3: וידוא פורמט לוחית זיהוי (אורך מינימלי)
-- תיאור השינוי: הוספת אילוץ בטבלת vehicle המבטיח שלוחית הזיהוי (plate_number) 
-- תכיל לפחות 7 תווים. אילוץ זה עוזר במניעת טעויות הקלדה והזנת לוחיות חסרות.

ALTER TABLE public.vehicle DROP CONSTRAINT IF EXISTS check_plate_length;
ALTER TABLE public.vehicle 
ADD CONSTRAINT check_plate_length 
CHECK (LENGTH(plate_number) >= 7);

-- ניסוי הכנסה לא תקין (אמור להחזיר שגיאה):
-- ניסיון להזין רכב עם לוחית קצרה מדי ("123").
INSERT INTO public.vehicle (plate_number, vehicle_type, capacity)
VALUES ('123', 'Van', 10);