// RideFlow Management System - Frontend JavaScript

// =============================================
// GLOBAL STATE
// =============================================
let currentTable = 'driver';
let tableData = [];
let optionsLoaded = false; // lazy-load flag
let dbOptions = {
    driver: [],
    passenger: [],
    route: [],
    stop: [],
    vehicle: [],
    trip: []
};

// Cached text per row for fast filtering (no DOM traversal)
let tableDataText = [];

// Hebrew column name translations
const columnTranslations = {
    'driver_id': 'מזהה נהג',
    'driver_fullname': 'שם נהג',
    'licensetype': 'סוג רישיון',
    'pass_id': 'מזהה נוסע',
    'pass_fullname': 'שם נוסע',
    'email': 'אימייל',
    'phone': 'טלפון',
    'sector': 'מגזר',
    'plate_number': 'מספר רישוי',
    'capacity': 'קיבולת',
    'vehicle_type': 'סוג רכב',
    'route_id': 'מזהה מסלול',
    'route_name': 'שם מסלול',
    'stop_id': 'מזהה תחנה',
    'stop_name': 'שם תחנה',
    'stop_order': 'סדר תחנה',
    'trip_id': 'מזהה נסיעה',
    'trip_date': 'תאריך',
    'departure_time': 'שעת יציאה',
    'available_seats': 'מקומות פנויים',
    'reg_id': 'מזהה הרשמה',
    'status': 'סטטוס',
    'boarding_stop_name': 'תחנת עלייה',
    'dropoff_stop_name': 'תחנת הורדה',
    'boarding_stop_id': 'תחנת עלייה (ID)',
    'dropoff_stop_id': 'תחנת הורדה (ID)'
};

// Hidden columns per table
const hiddenColumnsPerTable = {
    'trip': ['route_id', 'driver_id'],
    'registration': ['pass_id', 'trip_id', 'boarding_stop_id', 'dropoff_stop_id'],
    'includes': ['route_id', 'stop_id'],
    'routestop': ['route_id', 'stop_id']
};

// Primary keys per table
const primaryKeys = {
    'driver': ['driver_id'],
    'passenger': ['pass_id'],
    'vehicle': ['plate_number'],
    'route': ['route_id'],
    'stop': ['stop_id'],
    'includes': ['route_id', 'stop_id'],
    'routestop': ['route_id', 'stop_id'],
    'trip': ['trip_id'],
    'registration': ['reg_id', 'pass_id']
};

// =============================================
// INIT — only load stats + first table. NO options on startup.
// =============================================
document.addEventListener('DOMContentLoaded', () => {
    loadDashboardStats();
    // Do NOT call loadAllOptions() here — it fetches tens of thousands
    // of rows for every dropdown. Load lazily only when needed.
});

// =============================================
// 1. NAVIGATION
// =============================================
function switchTab(tabId, subTable = null) {
    document.querySelectorAll('.tab-section').forEach(s => s.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));

    const section = document.getElementById(`tab-${tabId}`);
    if (section) section.classList.add('active');
    const navItem = document.getElementById(`nav-${tabId}`);
    if (navItem) navItem.classList.add('active');

    const title = document.getElementById('current-page-title');
    const subtitle = document.getElementById('current-page-subtitle');

    if (tabId === 'dashboard') {
        title.innerText = 'לוח בקרה';
        subtitle.innerText = 'מבט על סטטיסטיקות בסיס הנתונים';
        loadDashboardStats();

    } else if (tabId === 'crud') {
        title.innerText = 'ניהול נתונים (CRUD)';
        subtitle.innerText = 'צפייה, הוספה, עדכון ומחיקה של רשומות';
        if (subTable) {
            const sel = document.getElementById('table-selector');
            if (sel) sel.value = subTable;
        }
        onTableChange();

    } else if (tabId === 'queries') {
        title.innerText = 'שאילתות מורכבות';
        subtitle.innerText = 'שאילתות מתקדמות ומנתחות';

    } else if (tabId === 'routines') {
        title.innerText = 'פונקציות ופרוצדורות';
        subtitle.innerText = 'קריאות לתהליכים המאוחסנים בבסיס הנתונים';
        // Load options lazily — only when the user actually visits this tab
        loadAllOptions();
    }
}

// =============================================
// 2. DASHBOARD STATS
// =============================================
async function loadDashboardStats() {
    try {
        const res = await fetch('/api/stats');
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        ['driver', 'passenger', 'trip', 'registration', 'route', 'stop'].forEach(key => {
            const el = document.getElementById(`stat-${key}`);
            if (el) el.innerText = data[key] !== undefined ? data[key] : '-';
        });
    } catch (err) {
        console.error('Stats error:', err);
    }
}

// =============================================
// 3. OPTIONS — lazy, called only when needed
// =============================================
async function loadAllOptions() {
    // Only fetch once per session unless forced
    if (optionsLoaded) {
        populateRoutineDropdowns();
        return;
    }
    const tables = ['driver', 'passenger', 'route', 'stop', 'vehicle', 'trip'];
    // Fetch all in parallel but with LIMIT (enforced server-side)
    const promises = tables.map(t =>
        fetch(`/api/options/${t}`)
            .then(r => r.ok ? r.json() : Promise.resolve({ error: true }))
            .then(data => { if (!data.error) dbOptions[t] = data; })
            .catch(e => console.error(`Options error ${t}:`, e))
    );
    await Promise.all(promises);
    optionsLoaded = true;
    populateRoutineDropdowns();
}

function buildSelectOptions(items, valKey, textKey) {
    // Use array join — much faster than string concatenation in a loop
    const parts = ['<option value="">-- בחר --</option>'];
    items.forEach(opt => {
        parts.push(`<option value="${opt[valKey]}">${opt[textKey] || opt[valKey]}</option>`);
    });
    return parts.join('');
}

function populateRoutineDropdowns() {
    const tripHtml = (() => {
        const parts = ['<option value="">-- בחר נסיעה --</option>'];
        dbOptions.trip.forEach(t => {
            const d = t.trip_date ? new Date(t.trip_date).toLocaleDateString('he-IL') : '';
            parts.push(`<option value="${t.trip_id}">נסיעה ${t.trip_id} - ${t.route_name} (${d})</option>`);
        });
        return parts.join('');
    })();

    const passHtml = (() => {
        const parts = ['<option value="">-- בחר נוסע --</option>'];
        dbOptions.passenger.forEach(p => {
            parts.push(`<option value="${p.pass_id}">${p.pass_fullname} (${p.pass_id})</option>`);
        });
        return parts.join('');
    })();

    const stopHtml = (() => {
        const parts = ['<option value="">-- בחר תחנה --</option>'];
        dbOptions.stop.forEach(s => {
            parts.push(`<option value="${s.stop_id}">${s.stop_name} (${s.stop_id})</option>`);
        });
        return parts.join('');
    })();

    const routeHtml = (() => {
        const parts = ['<option value="">-- בחר מסלול --</option>'];
        dbOptions.route.forEach(r => {
            parts.push(`<option value="${r.route_id}">${r.route_name} (${r.route_id})</option>`);
        });
        return parts.join('');
    })();

    // Set all at once using innerHTML — single DOM write each
    const set = (id, html) => { const el = document.getElementById(id); if (el) el.innerHTML = html; };
    set('routine-trip-select', tripHtml);
    set('routine-trip-reg-select', tripHtml);
    set('routine-pass-select', passHtml);
    set('routine-pass-lookup', passHtml);
    set('routine-board-select', stopHtml);
    set('routine-drop-select', stopHtml);
    set('routine-route-lookup', routeHtml);
}

// =============================================
// 4. CRUD — load table data
// =============================================
async function onTableChange() {
    const sel = document.getElementById('table-selector');
    if (!sel) return;
    currentTable = sel.value;

    const rowCountEl = document.getElementById('row-count');
    const thead = document.querySelector('#data-table thead');
    const tbody = document.querySelector('#data-table tbody');

    if (rowCountEl) rowCountEl.innerText = 'טוען נתונים...';
    if (thead) thead.innerHTML = '';
    if (tbody) tbody.innerHTML = '<tr><td colspan="20" style="text-align:center;padding:30px;color:#aaa;">⏳ טוען נתונים...</td></tr>';

    try {
        const res = await fetch(`/api/data/${currentTable}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);

        tableData = data;
        // Pre-build search text cache — avoids DOM reflow in filterTable
        tableDataText = tableData.map(row =>
            Object.values(row).map(v => (v === null || v === undefined) ? '' : String(v)).join(' ').toLowerCase()
        );
        renderTable();
    } catch (err) {
        if (tbody) tbody.innerHTML = `<tr><td colspan="20" style="text-align:center;color:#e74c3c;padding:30px;">שגיאה: ${err.message}</td></tr>`;
        if (rowCountEl) rowCountEl.innerText = 'שגיאה';
        console.error('onTableChange:', err);
    }
}

// =============================================
// 5. RENDER TABLE — single DOM write, no per-row reflow
// =============================================
function renderTable() {
    const table = document.getElementById('data-table');
    if (!table) return;
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    const rowCountEl = document.getElementById('row-count');

    if (!tableData || tableData.length === 0) {
        thead.innerHTML = '';
        tbody.innerHTML = '<tr><td colspan="20" style="text-align:center;padding:30px;">לא נמצאו נתונים בטבלה זו</td></tr>';
        if (rowCountEl) rowCountEl.innerText = '0 רשומות';
        return;
    }

    const allKeys = Object.keys(tableData[0]);
    const hidden = hiddenColumnsPerTable[currentTable] || [];
    const visibleKeys = allKeys.filter(k => !hidden.includes(k));
    const pks = primaryKeys[currentTable] || [];

    // Build header — one write
    const headerParts = ['<tr>'];
    visibleKeys.forEach(k => headerParts.push(`<th>${columnTranslations[k] || k}</th>`));
    headerParts.push('<th style="text-align:center;min-width:130px;">פעולות</th></tr>');
    thead.innerHTML = headerParts.join('');

    // Build ALL rows as one big string — single innerHTML write
    const rowParts = [];
    tableData.forEach((row, idx) => {
        rowParts.push(`<tr data-idx="${idx}">`);
        visibleKeys.forEach(k => {
            let val = row[k];
            if (k === 'trip_date' && val) val = new Date(val).toLocaleDateString('he-IL');
            rowParts.push(`<td>${(val === null || val === undefined) ? '<span style="color:#bbb">—</span>' : val}</td>`);
        });
        const deleteParams = pks.map(pk => `${pk}=${encodeURIComponent(row[pk])}`).join('&');
        rowParts.push(`
            <td style="text-align:center;white-space:nowrap;">
                <button class="btn btn-warning btn-sm" style="padding:3px 9px;font-size:12px;margin-left:3px" onclick="openUpdateModal(${idx})">✏️</button>
                <button class="btn btn-danger btn-sm" style="padding:3px 9px;font-size:12px" onclick="deleteRecord('${deleteParams}')">🗑️</button>
            </td>
        </tr>`);
    });

    tbody.innerHTML = rowParts.join('');
    if (rowCountEl) rowCountEl.innerText = `${tableData.length} רשומות (מוצגות ${tableData.length})`;
}

// =============================================
// 6. FILTER — uses pre-cached text, no DOM reflow
// =============================================
function filterTable() {
    const query = document.getElementById('table-search').value.toLowerCase().trim();
    const tbody = document.querySelector('#data-table tbody');
    const rows = tbody ? tbody.querySelectorAll('tr') : [];
    const rowCountEl = document.getElementById('row-count');
    let visible = 0;

    if (!query) {
        // No filter — show all rows fast
        rows.forEach(r => { r.style.display = ''; });
        visible = tableData.length;
    } else {
        rows.forEach((row, i) => {
            const match = tableDataText[i] && tableDataText[i].includes(query);
            row.style.display = match ? '' : 'none';
            if (match) visible++;
        });
    }

    if (rowCountEl) rowCountEl.innerText = `${visible} מתוך ${tableData.length} רשומות`;
}

// =============================================
// 7. DELETE
// =============================================
async function deleteRecord(queryParams) {
    if (!confirm('האם אתה בטוח שברצונך למחוק רשומה זו?')) return;
    try {
        const res = await fetch(`/api/data/${currentTable}?${queryParams}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        alert('הרשומה נמחקה בהצלחה!');
        onTableChange();
    } catch (err) {
        alert('שגיאה במחיקה: ' + err.message);
    }
}

// =============================================
// 8. ADD MODAL — loads options lazily before opening
// =============================================
async function openAddModal() {
    // Ensure options are loaded before showing form
    if (!optionsLoaded) await loadAllOptions();

    const modal = document.getElementById('add-modal');
    const fieldsContainer = document.getElementById('add-form-fields');
    if (!modal || !fieldsContainer) return;

    document.getElementById('add-modal-title').innerText = `הוספת רשומה — טבלת ${currentTable}`;
    let fieldsHtml = '';

    if (currentTable === 'driver') {
        fieldsHtml += generateTextInput('driver_fullname', 'שם מלא של הנהג', true);
        fieldsHtml += generateTextInput('licensetype', 'דרגת רישיון (B, C, C1...)', true);
    } else if (currentTable === 'passenger') {
        fieldsHtml += generateTextInput('pass_fullname', 'שם מלא של הנוסע', true);
        fieldsHtml += generateTextInput('email', 'כתובת אימייל', false, 'email');
        fieldsHtml += generateTextInput('phone', 'מספר טלפון', true, 'tel');
        fieldsHtml += generateTextInput('sector', 'מגזר (סטודנט, מבוגר...)', false);
    } else if (currentTable === 'vehicle') {
        fieldsHtml += generateTextInput('plate_number', 'מספר רישוי', true);
        fieldsHtml += generateNumberInput('capacity', 'קיבולת נוסעים', 1, true);
        fieldsHtml += generateTextInput('vehicle_type', 'סוג הרכב', false);
    } else if (currentTable === 'route') {
        fieldsHtml += generateTextInput('route_name', 'שם המסלול', true);
    } else if (currentTable === 'stop') {
        fieldsHtml += generateTextInput('stop_name', 'שם התחנה', true);
    } else if (currentTable === 'includes') {
        fieldsHtml += generateSelectInput('route_id', 'בחר מסלול', dbOptions.route, 'route_id', 'route_name', true);
        fieldsHtml += generateSelectInput('stop_id', 'בחר תחנה', dbOptions.stop, 'stop_id', 'stop_name', true);
    } else if (currentTable === 'routestop') {
        fieldsHtml += generateSelectInput('route_id', 'בחר מסלול', dbOptions.route, 'route_id', 'route_name', true);
        fieldsHtml += generateSelectInput('stop_id', 'בחר תחנה', dbOptions.stop, 'stop_id', 'stop_name', true);
        fieldsHtml += generateNumberInput('stop_order', 'מיקום תחנה (סדר)', 1, true);
    } else if (currentTable === 'trip') {
        fieldsHtml += generateDateInput('trip_date', 'תאריך נסיעה', true);
        fieldsHtml += generateTextInput('departure_time', 'שעת יציאה (08:30)', true);
        fieldsHtml += generateNumberInput('available_seats', 'מושבים פנויים', 1, true);
        fieldsHtml += generateSelectInput('route_id', 'בחר מסלול', dbOptions.route, 'route_id', 'route_name', true);
        fieldsHtml += generateSelectInput('driver_id', 'בחר נהג', dbOptions.driver, 'driver_id', 'driver_fullname', true);
        fieldsHtml += generateSelectInput('plate_number', 'בחר רכב', dbOptions.vehicle, 'plate_number', 'plate_number', true);
    } else if (currentTable === 'registration') {
        fieldsHtml += generateSelectInput('pass_id', 'בחר נוסע', dbOptions.passenger, 'pass_id', 'pass_fullname', true);
        fieldsHtml += generateSelectInput('trip_id', 'בחר נסיעה', dbOptions.trip, 'trip_id', 'route_name', true);
        fieldsHtml += generateSelectInput('boarding_stop_id', 'תחנת עלייה', dbOptions.stop, 'stop_id', 'stop_name', true);
        fieldsHtml += generateSelectInput('dropoff_stop_id', 'תחנת הורדה', dbOptions.stop, 'stop_id', 'stop_name', true);
        fieldsHtml += generateSelectInput('status', 'סטטוס', [
            { val: 'Confirmed', label: 'Confirmed' },
            { val: 'Cancelled', label: 'Cancelled' },
            { val: 'Completed', label: 'Completed' }
        ], 'val', 'label', true);
    } else {
        fieldsHtml = '<p>אין תמיכה בהוספה לטבלה זו</p>';
    }

    fieldsContainer.innerHTML = fieldsHtml;
    modal.style.display = 'flex';
}

async function submitAddForm(event) {
    event.preventDefault();
    const formData = new FormData(document.getElementById('add-form'));
    const payload = {};
    const intFields = ['capacity', 'available_seats', 'route_id', 'stop_id', 'driver_id', 'pass_id', 'trip_id', 'boarding_stop_id', 'dropoff_stop_id', 'stop_order'];
    formData.forEach((val, key) => {
        payload[key] = intFields.includes(key) ? (val ? parseInt(val) : null) : (val || null);
    });
    try {
        const res = await fetch(`/api/data/${currentTable}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        alert('הרשומה נוספה בהצלחה!');
        closeModal('add-modal');
        onTableChange();
    } catch (err) {
        alert('שגיאה בהוספה: ' + err.message);
    }
}

// =============================================
// 9. UPDATE MODAL — loads options lazily
//    rowIdx: if provided, opens edit form directly for that row (no selection step)
// =============================================
async function openUpdateModal(rowIdx = null) {
    if (!optionsLoaded) await loadAllOptions();

    const modal = document.getElementById('update-modal');
    const fetchStage = document.getElementById('fetch-row-stage');
    const updateForm = document.getElementById('update-form');
    const keyContainer = document.getElementById('fetch-key-inputs');
    if (!modal) return;

    document.getElementById('update-modal-title').innerText = `עדכון רשומה — טבלת ${currentTable}`;

    // ── DIRECT MODE: row index passed from the table button ──
    if (rowIdx !== null && tableData[rowIdx]) {
        fetchStage.style.display = 'none';
        updateForm.style.display = 'none';
        keyContainer.innerHTML = '';
        modal.style.display = 'flex';
        renderUpdateFormFields(tableData[rowIdx]); // skip the fetch step
        return;
    }

    // ── MANUAL MODE: header button — user picks from dropdown ──
    fetchStage.style.display = 'block';
    updateForm.style.display = 'none';
    keyContainer.innerHTML = '';

    let keysHtml = '';
    if (currentTable === 'driver')         keysHtml = generateSelectInput('id', 'בחר נהג', dbOptions.driver, 'driver_id', 'driver_fullname', true);
    else if (currentTable === 'passenger') keysHtml = generateSelectInput('id', 'בחר נוסע', dbOptions.passenger, 'pass_id', 'pass_fullname', true);
    else if (currentTable === 'vehicle')   keysHtml = generateSelectInput('id', 'בחר רכב', dbOptions.vehicle, 'plate_number', 'plate_number', true);
    else if (currentTable === 'route')     keysHtml = generateSelectInput('id', 'בחר מסלול', dbOptions.route, 'route_id', 'route_name', true);
    else if (currentTable === 'stop')      keysHtml = generateSelectInput('id', 'בחר תחנה', dbOptions.stop, 'stop_id', 'stop_name', true);
    else if (currentTable === 'includes') {
        keysHtml = generateSelectInput('route_id', 'מסלול', dbOptions.route, 'route_id', 'route_name', true);
        keysHtml += generateSelectInput('stop_id', 'תחנה', dbOptions.stop, 'stop_id', 'stop_name', true);
    }
    else if (currentTable === 'routestop') {
        keysHtml = generateSelectInput('route_id', 'מסלול', dbOptions.route, 'route_id', 'route_name', true);
        keysHtml += generateSelectInput('stop_id', 'תחנה', dbOptions.stop, 'stop_id', 'stop_name', true);
    }
    else if (currentTable === 'trip')         keysHtml = generateSelectInput('id', 'בחר נסיעה', dbOptions.trip, 'trip_id', 'route_name', true);
    else if (currentTable === 'registration') {
        keysHtml = generateNumberInput('reg_id', 'מזהה הרשמה (reg_id)', 1, true);
        keysHtml += generateSelectInput('pass_id', 'בחר נוסע', dbOptions.passenger, 'pass_id', 'pass_fullname', true);
    }

    keyContainer.innerHTML = keysHtml;
    modal.style.display = 'flex';
}

async function fetchRowForUpdate() {
    const inputs = document.getElementById('fetch-key-inputs').querySelectorAll('input, select');
    const params = inputs.length === 1
        ? `id=${inputs[0].value}`
        : Array.from(inputs).map(i => `${i.name}=${i.value}`).join('&');
    try {
        const res = await fetch(`/api/data/${currentTable}/fetch?${params}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        renderUpdateFormFields(data);
    } catch (err) {
        alert('שגיאה: הרשומה לא נמצאה — ' + err.message);
    }
}

function renderUpdateFormFields(rowData) {
    const fetchStage = document.getElementById('fetch-row-stage');
    const updateForm = document.getElementById('update-form');
    const fieldsContainer = document.getElementById('update-form-fields');
    let html = '';

    if (currentTable === 'driver') {
        html += `<input type="hidden" name="driver_id" value="${rowData.driver_id}">`;
        html += `<div class="form-group"><label>מזהה נהג (נעול)</label><input class="form-control" type="text" value="${rowData.driver_id}" disabled></div>`;
        html += generateTextInput('driver_fullname', 'שם מלא', true, 'text', rowData.driver_fullname || '');
        html += generateTextInput('licensetype', 'דרגת רישיון', true, 'text', rowData.licensetype || '');
    } else if (currentTable === 'passenger') {
        html += `<input type="hidden" name="pass_id" value="${rowData.pass_id}">`;
        html += `<div class="form-group"><label>מזהה נוסע (נעול)</label><input class="form-control" type="text" value="${rowData.pass_id}" disabled></div>`;
        html += generateTextInput('pass_fullname', 'שם מלא', true, 'text', rowData.pass_fullname || '');
        html += generateTextInput('email', 'אימייל', false, 'email', rowData.email || '');
        html += generateTextInput('phone', 'טלפון', true, 'tel', rowData.phone || '');
        html += generateTextInput('sector', 'מגזר', false, 'text', rowData.sector || '');
    } else if (currentTable === 'vehicle') {
        html += `<input type="hidden" name="plate_number" value="${rowData.plate_number}">`;
        html += `<div class="form-group"><label>מספר רישוי (נעול)</label><input class="form-control" type="text" value="${rowData.plate_number}" disabled></div>`;
        html += generateNumberInput('capacity', 'קיבולת', 1, true, rowData.capacity);
        html += generateTextInput('vehicle_type', 'סוג הרכב', false, 'text', rowData.vehicle_type || '');
    } else if (currentTable === 'route') {
        html += `<input type="hidden" name="route_id" value="${rowData.route_id}">`;
        html += `<div class="form-group"><label>מזהה מסלול (נעול)</label><input class="form-control" type="text" value="${rowData.route_id}" disabled></div>`;
        html += generateTextInput('route_name', 'שם המסלול', true, 'text', rowData.route_name || '');
    } else if (currentTable === 'stop') {
        html += `<input type="hidden" name="stop_id" value="${rowData.stop_id}">`;
        html += `<div class="form-group"><label>מזהה תחנה (נעול)</label><input class="form-control" type="text" value="${rowData.stop_id}" disabled></div>`;
        html += generateTextInput('stop_name', 'שם התחנה', true, 'text', rowData.stop_name || '');
    } else if (currentTable === 'routestop') {
        html += `<input type="hidden" name="route_id" value="${rowData.route_id}">`;
        html += `<input type="hidden" name="stop_id" value="${rowData.stop_id}">`;
        html += `<div class="form-group"><label>מסלול: ${rowData.route_name || rowData.route_id} / תחנה: ${rowData.stop_name || rowData.stop_id} (נעול)</label></div>`;
        html += generateNumberInput('stop_order', 'מיקום תחנה (סדר)', 1, true, rowData.stop_order);
    } else if (currentTable === 'trip') {
        html += `<input type="hidden" name="trip_id" value="${rowData.trip_id}">`;
        html += `<div class="form-group"><label>מזהה נסיעה (נעול)</label><input class="form-control" type="text" value="${rowData.trip_id}" disabled></div>`;
        const d = rowData.trip_date ? new Date(rowData.trip_date).toISOString().split('T')[0] : '';
        html += generateDateInput('trip_date', 'תאריך', true, d);
        html += generateTextInput('departure_time', 'שעת יציאה', true, 'text', rowData.departure_time || '');
        html += generateNumberInput('available_seats', 'מושבים פנויים', 0, true, rowData.available_seats);
        html += generateSelectInput('route_id', 'מסלול', dbOptions.route, 'route_id', 'route_name', true, rowData.route_id);
        html += generateSelectInput('driver_id', 'נהג', dbOptions.driver, 'driver_id', 'driver_fullname', true, rowData.driver_id);
        html += generateSelectInput('plate_number', 'רכב', dbOptions.vehicle, 'plate_number', 'plate_number', true, rowData.plate_number);
    } else if (currentTable === 'registration') {
        html += `<input type="hidden" name="reg_id" value="${rowData.reg_id}">`;
        html += `<input type="hidden" name="pass_id" value="${rowData.pass_id}">`;
        html += `<div class="form-group"><label>הרשמה ${rowData.reg_id} / נוסע ${rowData.pass_id} (נעול)</label></div>`;
        html += generateSelectInput('boarding_stop_id', 'תחנת עלייה', dbOptions.stop, 'stop_id', 'stop_name', true, rowData.boarding_stop_id);
        html += generateSelectInput('dropoff_stop_id', 'תחנת הורדה', dbOptions.stop, 'stop_id', 'stop_name', true, rowData.dropoff_stop_id);
        html += generateSelectInput('status', 'סטטוס', [
            { val: 'Confirmed', label: 'Confirmed' },
            { val: 'Cancelled', label: 'Cancelled' },
            { val: 'Completed', label: 'Completed' }
        ], 'val', 'label', true, rowData.status);
    }

    fieldsContainer.innerHTML = html;
    fetchStage.style.display = 'none';
    updateForm.style.display = 'block';
}

async function submitUpdateForm(event) {
    event.preventDefault();
    const formData = new FormData(document.getElementById('update-form'));
    const payload = {};
    const intFields = ['capacity', 'available_seats', 'route_id', 'stop_id', 'driver_id', 'pass_id', 'trip_id', 'boarding_stop_id', 'dropoff_stop_id', 'reg_id', 'stop_order'];
    formData.forEach((val, key) => {
        payload[key] = intFields.includes(key) ? (val ? parseInt(val) : null) : (val || null);
    });
    try {
        const res = await fetch(`/api/data/${currentTable}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        alert('הרשומה עודכנה בהצלחה!');
        closeModal('update-modal');
        onTableChange();
    } catch (err) {
        alert('שגיאה בעדכון: ' + err.message);
    }
}

// =============================================
// 10. QUERIES
// =============================================
async function runCustomQuery(queryId) {
    const resultsContainer = document.getElementById('query-results-section');
    const table = document.getElementById('query-results-table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    const titles = {
        '1': 'נסיעות עם כמות הרשמות מעל הממוצע',
        '2': 'נהגים שביצעו נסיעות',
        '3': 'כמות הרשמות לכל מסלול'
    };
    document.getElementById('query-title-display').innerText = titles[queryId] || 'תוצאות שאילתה';
    thead.innerHTML = '';
    tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:20px;">⏳ מריץ שאילתה...</td></tr>';
    resultsContainer.style.display = 'block';

    try {
        const res = await fetch(`/api/query/${queryId}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);

        if (data.length === 0) {
            thead.innerHTML = '<tr><th>אין נתונים</th></tr>';
            tbody.innerHTML = '<tr><td>השאילתה החזירה 0 שורות.</td></tr>';
            return;
        }

        const keys = Object.keys(data[0]);
        thead.innerHTML = '<tr>' + keys.map(k => `<th>${columnTranslations[k] || k}</th>`).join('') + '</tr>';

        const rowParts = [];
        data.forEach(row => {
            rowParts.push('<tr>');
            keys.forEach(k => {
                let val = row[k];
                if (k === 'trip_date' && val) val = new Date(val).toLocaleDateString('he-IL');
                rowParts.push(`<td>${val === null ? 'N/A' : val}</td>`);
            });
            rowParts.push('</tr>');
        });
        tbody.innerHTML = rowParts.join('');
        resultsContainer.scrollIntoView({ behavior: 'smooth' });
    } catch (err) {
        tbody.innerHTML = `<tr><td colspan="10" style="color:#e74c3c;text-align:center;padding:20px;">שגיאה: ${err.message}</td></tr>`;
    }
}

// =============================================
// 11. ROUTINES
// =============================================
async function executeCancelTrip(event) {
    event.preventDefault();
    const tripId = document.getElementById('routine-trip-select').value;
    const feedback = document.getElementById('feedback-cancel-trip');
    if (!tripId) { alert('אנא בחר נסיעה'); return; }

    feedback.className = 'routine-feedback';
    feedback.style.display = 'block';
    feedback.innerText = '⏳ מבצע ביטול נסיעה...';

    try {
        const res = await fetch('/api/routine/cancel_trip', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ trip_id: parseInt(tripId) })
        });
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        feedback.classList.add('success');
        let html = `<strong>${data.message}</strong>`;
        if (data.notices && data.notices.length > 0) {
            html += '<div class="notice-log"><p class="help-text">הודעות מ-PostgreSQL:</p>';
            data.notices.forEach(n => { html += `<div class="notice-item">💬 ${n}</div>`; });
            html += '</div>';
        }
        feedback.innerHTML = html;
        loadDashboardStats();
    } catch (err) {
        feedback.classList.add('error');
        feedback.innerText = 'שגיאה: ' + err.message;
    }
}

async function executeRegisterPassenger(event) {
    event.preventDefault();
    const passId = document.getElementById('routine-pass-select').value;
    const tripId = document.getElementById('routine-trip-reg-select').value;
    const boardId = document.getElementById('routine-board-select').value;
    const dropId = document.getElementById('routine-drop-select').value;
    const feedback = document.getElementById('feedback-register-passenger');

    if (!passId || !tripId || !boardId || !dropId) { alert('אנא מלא את כל השדות'); return; }
    feedback.className = 'routine-feedback';
    feedback.style.display = 'block';
    feedback.innerText = '⏳ רושם נוסע לנסיעה...';

    try {
        const res = await fetch('/api/routine/register_passenger', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pass_id: parseInt(passId), trip_id: parseInt(tripId), boarding_stop_id: parseInt(boardId), dropoff_stop_id: parseInt(dropId) })
        });
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        feedback.classList.add('success');
        let html = `<strong>${data.message}</strong>`;
        if (data.notices && data.notices.length > 0) {
            html += '<div class="notice-log"><p class="help-text">הודעות מ-PostgreSQL:</p>';
            data.notices.forEach(n => { html += `<div class="notice-item">💬 ${n}</div>`; });
            html += '</div>';
        }
        feedback.innerHTML = html;
        loadDashboardStats();
    } catch (err) {
        feedback.classList.add('error');
        feedback.innerText = 'שגיאה: ' + err.message;
    }
}

async function executeGetPassengerTrips(event) {
    event.preventDefault();
    const passId = document.getElementById('routine-pass-lookup').value;
    const feedback = document.getElementById('feedback-passenger-trips');
    const tableDiv = feedback.querySelector('.table-responsive');
    const tbody = document.querySelector('#tbl-passenger-trips tbody');

    if (!passId) { alert('אנא בחר נוסע'); return; }
    feedback.className = 'routine-feedback';
    feedback.style.display = 'block';
    feedback.innerText = '⏳ שולף נסיעות...';
    if (tableDiv) tableDiv.style.display = 'none';

    try {
        const res = await fetch(`/api/routine/passenger_trips?pass_id=${passId}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);

        if (data.length === 0) {
            feedback.innerHTML = '<span style="color:#999">לא נמצאו נסיעות פעילות עבור נוסע זה.</span>';
            return;
        }
        feedback.innerHTML = '';
        if (tableDiv) { feedback.appendChild(tableDiv); tableDiv.style.display = 'block'; }
        const parts = [];
        data.forEach(row => {
            const d = row.trip_date ? new Date(row.trip_date).toLocaleDateString('he-IL') : '';
            parts.push(`<tr><td>${row.reg_id||''}</td><td>${row.registration_status||''}</td><td>${row.trip_id||''}</td><td>${d}</td><td>${row.departure_time||'N/A'}</td><td>${row.trip_status||'Active'}</td><td>${row.route_name||''}</td></tr>`);
        });
        if (tbody) tbody.innerHTML = parts.join('');
    } catch (err) {
        feedback.classList.add('error');
        feedback.innerText = 'שגיאה: ' + err.message;
    }
}

async function executeGetRouteSeats(event) {
    event.preventDefault();
    const routeId = document.getElementById('routine-route-lookup').value;
    const feedback = document.getElementById('feedback-route-seats');
    if (!routeId) { alert('אנא בחר מסלול'); return; }
    feedback.className = 'routine-feedback';
    feedback.style.display = 'block';
    feedback.innerText = '⏳ מחשב מקומות פנויים...';
    try {
        const res = await fetch(`/api/routine/route_seats?route_id=${routeId}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        feedback.classList.add('success');
        feedback.innerHTML = `🏁 סך המקומות הפנויים: <strong>${data.available_seats}</strong> מקומות.`;
    } catch (err) {
        feedback.classList.add('error');
        feedback.innerText = 'שגיאה: ' + err.message;
    }
}

// =============================================
// HELPERS — form field generators
// =============================================
function generateTextInput(name, label, required = false, type = 'text', val = '') {
    return `<div class="form-group">
        <label for="f-${name}">${label}${required ? ' <span style="color:#e74c3c">*</span>' : ''}</label>
        <input type="${type}" id="f-${name}" name="${name}" class="form-control" value="${val}" ${required ? 'required' : ''}>
    </div>`;
}
function generateNumberInput(name, label, min = 0, required = false, val = '') {
    return `<div class="form-group">
        <label for="f-${name}">${label}${required ? ' <span style="color:#e74c3c">*</span>' : ''}</label>
        <input type="number" id="f-${name}" name="${name}" class="form-control" min="${min}" value="${(val !== null && val !== undefined) ? val : ''}" ${required ? 'required' : ''}>
    </div>`;
}
function generateDateInput(name, label, required = false, val = '') {
    return `<div class="form-group">
        <label for="f-${name}">${label}${required ? ' <span style="color:#e74c3c">*</span>' : ''}</label>
        <input type="date" id="f-${name}" name="${name}" class="form-control" value="${val}" ${required ? 'required' : ''}>
    </div>`;
}
function generateSelectInput(name, label, options, valKey, textKey, required = false, selectedVal = null) {
    const parts = [`<option value="">-- בחר ${label} --</option>`];
    options.forEach(opt => {
        const sel = (selectedVal !== null && selectedVal !== undefined &&
            opt[valKey] !== null && opt[valKey] !== undefined &&
            String(opt[valKey]) === String(selectedVal)) ? 'selected' : '';

        // For trip dropdowns: show "נסיעה X - [route] (date)" so trip number is clear
        let displayVal;
        if (name === 'trip_id' || (name === 'id' && opt.trip_id !== undefined)) {
            const dateStr = opt.trip_date ? new Date(opt.trip_date).toLocaleDateString('he-IL') : '';
            displayVal = `נסיעה ${opt.trip_id}${opt.route_name ? ' — ' + opt.route_name : ''}${dateStr ? ' (' + dateStr + ')' : ''}`;
        } else {
            displayVal = opt[textKey] || opt[valKey];
        }

        parts.push(`<option value="${opt[valKey]}" ${sel}>${displayVal}</option>`);
    });
    return `<div class="form-group">
        <label for="f-${name}">${label}${required ? ' <span style="color:#e74c3c">*</span>' : ''}</label>
        <select id="f-${name}" name="${name}" class="form-control" ${required ? 'required' : ''}>${parts.join('')}</select>
    </div>`;
}

// =============================================
// MODAL
// =============================================
function closeModal(id) {
    const m = document.getElementById(id);
    if (m) m.style.display = 'none';
}
