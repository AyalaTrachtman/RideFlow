**דו"ח שלב ד – תכנות**

תיאור כללי
- **מטרה:** להציג את הפונקציות, הפרוצדורות, הטריגרים והתוכניות הראשיות שנבנו עבור מסד הנתונים של פרויקט RideFlow, כולל תיאור, קוד והוכחות הרצה (תמונות).
- **מיקום קוד:** כל קבצי ה-SQL נמצאים בתיקיה [ex4/scripts](ex4/scripts)
- **תמונות:** כל התמונות משולבות בהמשך מתוך [ex4/screenshots](ex4/screenshots)

**הערה חשובה לגבי הגשה**
- חובה ליצור TAG בגיט עבור השלב הזה. דוגמה לפקודות:

```powershell
git add .
git commit -m "שלב ד - תכנות: הוספת קבצי SQL ודו"ח"
git tag -a v4 -m "שלב ד - תכנות"
git push origin main --tags
```

---

**פונקציה 1 – `get_route_available_seats_function`**
- **תיאור:** פונקציה המחזירה את מספר המקומות הזמינים בנתיב/נסיעה מסוים. כוללת בדיקות, לולאות והחזרת ערך מספרי.
- **קובץ מקור:** [ex4/scripts/get_route_available_seats_function](ex4/scripts/get_route_available_seats_function)
- **קוד:** בצעו פתיחה של הקובץ לעיון (הקוד נשמר בקובץ המצויין).
- **הוכחות הרצה:**

יצירת הפונקציה:
![יצירת פונקציה1](screenshots/create_function1.png)

הרצה תקינה (תוצאה מוצגת):
![הרצה תקינה פונקציה1](screenshots/good_run_functon1.png)

הרצה שגויה (פרמטר שגוי וזריקת חריגה):
![הרצה שגויה פונקציה1](screenshots/wring_run_function1.png)

---

**פונקציה 2 – `function2_get_passenger_active_trips`**
- **תיאור:** פונקציה שמחזירה רשימה/מספר של נסיעות פעילות עבור נוסע נתון (שימוש ב-REF CURSOR ואיפוס/לולאות פנימיות).
- **קובץ מקור:** [ex4/scripts/function2_get_passenger_active_trips](ex4/scripts/function2_get_passenger_active_trips)
- **הוכחות הרצה:**

יצירת הפונקציה:
![יצירת פונקציה2](screenshots/create_function2.png)

הרצה תקינה:
![הרצה תקינה פונקציה2](screenshots/good_run_function2.png)

הרצה שגויה (פרמטר שגוי):
![הרצה שגויה פונקציה2](screenshots/wrong_run_function2.png)

---

**פרוצדורה 1 – `register_passenger_to_trip_procedure`**
- **תיאור:** פרוצדורה שמרשמת נוסע לנסיעה, מבצעת בדיקות על זמינות מושבים, עדכוני DML (INSERT/UPDATE) ומשתמשת ב-EXCEPTION לטיפול בשגיאות.
- **קובץ מקור:** [ex4/scripts/register_passenger_to_trip_procedure](ex4/scripts/register_passenger_to_trip_procedure)
- **הוכחות הרצה:**

יצירת הפרוצדורה:
![יצירת פרוצדורה](screenshots/create_procedore1.png)

הרצה לפני עדכון בבסיס הנתונים (תצוגת טבלאות):
![מצב לפני הרצה](screenshots/run_database_befoe_rinprocdure1.png)

הרצה מוצלחת והעדכון בבסיס:
![הרצה מוצלחת פרוצדורה](screenshots/Screenshot 2026-06-14 025254.png)

הרצה עם פרמטר שגוי (זריקת חריגה):
![הרצה שגויה פרוצדורה](screenshots/Screenshot 2026-06-14 025306.png)

---

**פרוצדורה 2 – `cancel_trip_and_notify_procedure`**
- **תיאור:** פרוצדורה שמבטלת נסיעה, מעדכנת מושבים פנויים ושולחת הודעה לדוגמא (הדמה באמצעות טבלה/לוג). כוללת שימוש ב-TRANSACTION ובחריגות.
- **קובץ מקור:** [ex4/scripts/cancel_trip_and_notify_procedure](ex4/scripts/cancel_trip_and_notify_procedure)
- **הוכחות הרצה:**

יצירת הפרוצדורה והרצה:
![הרצה פרוצדורה2](screenshots/Screenshot 2026-06-14 025315.png)

בדיקה שהבסיס עודכן לאחר ביטול:
![בדיקת עדכון](screenshots/Screenshot 2026-06-14 025332.png)

הרצה שגויה להדגמת חריגה:
![הרצה שגויה פרוצדורה2](screenshots/Screenshot 2026-06-14 025514.png)

---

**טריגר 1 – `prevent_negative_seats_trigger` (ON UPDATE / INSERT)**
- **תיאור:** טריגר שבודק שאחרי עדכון הכמויות לא יווצרו ערכים שליליים במושבים; במידה וזה קורה הטריגר מונע את הפעולה וזורק חריגה.
- **קובץ מקור:** [ex4/scripts/prevent_negative_seats_trigger](ex4/scripts/prevent_negative_seats_trigger)
- **הוכחות הרצה:**

יצירת הטריגר:
![יצירת טריגר1](screenshots/Screenshot 2026-06-14 024817.png)

הכנסת נתונים שגורמים לטריגר לפעול (נעצר):
![טריגר פועל1](screenshots/Screenshot 2026-06-14 024835.png)

הדגמת חריגה מונעת עדכון:
![תוצאה טריגר1](screenshots/Screenshot 2026-06-14 024843.png)

---

**טריגר 2 – `validate_registration_stops_trigger` (ON UPDATE / INSERT)**
- **תיאור:** בודק תקינות נקודות עצירה בהרשמת נוסע (מניעת הרשמה לנקודות לא קיימות), זורק חריגה אם יש אי-התאמה.
- **קובץ מקור:** [ex4/scripts/validate_registration_stops_trigger](ex4/scripts/validate_registration_stops_trigger)
- **הוכחות הרצה:**

יצירת הטריגר והוכחה:
![יצירת טריגר2](screenshots/Screenshot 2026-06-14 024858.png)

בדיקה שהטריגר חוסם הרשמה לא תקינה:
![טריגר פועל2](screenshots/Screenshot 2026-06-14 025001.png)

---

**Main 1 – `main_program1`**
- **תיאור:** תוכנית ראשית שקוראת לפונקציה אחת ולפרוצדורה אחת (תסריט להפעלת תרחיש מלא: בדיקות, עדכונים והדפסות לוג).
- **קובץ מקור:** [ex4/scripts/main_program1](ex4/scripts/main_program1)
- **הוכחות הרצה:**

הרצת ה-MAIN והדפסות/עדכונים:
![MAIN1 הרצה](screenshots/Screenshot 2026-06-14 030201.png)
![MAIN1 בדיקה נוספת](screenshots/Screenshot 2026-06-14 030215.png)

---

**Main 2 – `main_program2`**
- **תיאור:** תסריט בדיקה משולב נוסף שקורא לפרוצדורות ובודק זריקות חריגות ו-rollbacks.
- **קובץ מקור:** [ex4/scripts/main_program2](ex4/scripts/main_program2)
- **הוכחות הרצה:**

הרצת ה-MAIN השני:
![MAIN2 הרצה](screenshots/Screenshot 2026-06-14 030225.png)
![MAIN2 בדיקה נוספת](screenshots/Screenshot 2026-06-14 030240.png)

---

**קבצים נוספים בתיקיה**
- קובץ גיבוי מעודכן: [ex4/scripts/backup4](ex4/scripts/backup4)

**סיכום ומסקנות**
- כל הפונקציות, הפרוצדורות והטריגרים נשמרו בקבצי SQL תחת [ex4/scripts](ex4/scripts). התמונות המצורפות מוכיחות יצירת פריטים, הרצות תקינות והרצות שגויות שמייצרות חריגות.
- אם חסרות תמונות ספציפיות עבור פריט מסוים, אנא הודיעו לי ואוסיף אותן ישירות ל-README כפי שנדרש.
