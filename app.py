from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
from flask_login import (
    LoginManager, UserMixin,
    login_user, login_required,
    logout_user, current_user
)
import datetime
from functools import wraps
import os
import math
import secrets
import smtplib
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv; load_dotenv()
from email.message import EmailMessage




# DB config
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:abc123@localhost:5432/bloodbridge"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static")  # for static folder
)

secret = os.getenv("SECRET_KEY")
if not secret:
    raise RuntimeError("SECRET_KEY env var is required")
app.secret_key = secret

app.config['TEMPLATES_AUTO_RELOAD'] = True

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Email config
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "1") == "1"

def send_email(to_email: str, subject: str, body: str):
    """
    Minimal SMTP email sender using config from environment.
    Safe no-op if email config or recipient is missing.
    """
    if not to_email or not EMAIL_USER or not EMAIL_PASSWORD:
        return

    msg = EmailMessage()
    msg["From"] = EMAIL_USER
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        # if smtp not responsive, this will fail in 2 seconds
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=2.0) as server:
            if EMAIL_USE_TLS:
                server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
    except Exception as e:
        # Log error but do not crash the app
        print(f"Email failed: {e}")



def get_db():
    """Open a PostgreSQL connection and set search_path."""
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO core, ops, public;")
    return conn

def queue_donor_notification(db, donor_id: str, request_id: str, email_to: str, preferred_contact: str = "Email"):
    """
    Create or update a row in ops.notifications for this (donor, request).
    just records the emails.
    """
    if not donor_id or not request_id or not email_to:
        return None

    row = db.execute("""
        SELECT notification_id
          FROM ops.notifications
         WHERE donor_id = %s
           AND request_id = %s;
    """, (donor_id, request_id)).fetchone()

    if row:
        nid = row["notification_id"]
        db.execute("""
            UPDATE ops.notifications
               SET email_to = %s,
                   preferred_contact = %s
             WHERE notification_id = %s;
        """, (email_to, preferred_contact, nid))
        return nid

    nid_row = db.execute("""
        INSERT INTO ops.notifications (
            donor_id, request_id, preferred_contact, email_to
        )
        VALUES (%s, %s, %s, %s)
        RETURNING notification_id;
    """, (donor_id, request_id, preferred_contact, email_to)).fetchone()

    return nid_row["notification_id"] if nid_row else None

def send_pending_notifications(db, limit: int = 50):
    """
    Simple worker: send up to `limit` pending email notifications.
    Call this from a protected route or a cron job.

    Additionally, to avoid long page loads and rate issues with Gmail,
    this function enforces an absolute cap of MAX_EMAILS_PER_RUN actual
    sends per call, regardless of the `limit` parameter.
    """
    # We are sending 7 emails per run just for the demo purpose, it will take time and cost if run loosely without any cap.
    MAX_EMAILS_PER_RUN = 7
    emails_sent = 0

    pending = db.execute("""
        SELECT notification_id, donor_id, request_id,
               preferred_contact, email_to, attempt_count
          FROM ops.notifications
         WHERE status = 'PENDING'
           AND email_to IS NOT NULL
           AND (preferred_contact ILIKE 'email' OR preferred_contact ILIKE 'both')
         ORDER BY created_at ASC
         LIMIT %s;
    """, (limit,)).fetchall()

    for n in pending:
        # Stop sending if we hit the cap
        if emails_sent >= MAX_EMAILS_PER_RUN:
            break

        try:
            subject = f"BloodBridge – New request {n['request_id']}"
            body = (
                f"Dear donor,\n\n"
                f"You have a new request on your BloodBridge dashboard.\n"
                f"Request ID: {n['request_id']}\n\n"
                f"Please log in to view full details and respond.\n\n"
                f"- BloodBridge"
            )
            send_email(n["email_to"], subject, body)
            emails_sent += 1

            db.execute("""
                UPDATE ops.notifications
                   SET status       = 'SENT',
                       sent_at      = NOW(),
                       attempt_count = attempt_count + 1,
                       error_message = NULL
                 WHERE notification_id = %s;
            """, (n["notification_id"],))
        except Exception as e:
            db.execute("""
                UPDATE ops.notifications
                SET status = 'FAILED',
                    attempt_count = attempt_count + 1,
                    error_message = %s
                WHERE notification_id = %s;
            """, (str(e), n["notification_id"]))

    db.commit()

# Helpers functions for distance, IDs, statuses, parsing & inventory
def haversine_distance(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return 999999.0
    R = 3958.8  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Request status helpers
REQUEST_STATUS_OPEN       = "OPEN"
REQUEST_STATUS_CLAIMED    = "CLAIMED"      
REQUEST_STATUS_PARTIAL    = "PARTIAL"     
REQUEST_STATUS_FULFILLED  = "FULFILLED"
REQUEST_STATUS_REJECTED   = "REJECTED"
REQUEST_STATUS_EXPIRED    = "EXPIRED"

VALID_REQUEST_STATUSES = {
    REQUEST_STATUS_OPEN,
    REQUEST_STATUS_CLAIMED,
    REQUEST_STATUS_PARTIAL,
    REQUEST_STATUS_FULFILLED,
    REQUEST_STATUS_REJECTED,
    REQUEST_STATUS_EXPIRED,
}

# Normalization and level parsing over the blood components, sends the request for only those components to different entities.
_COMPONENT_MAP = {
    "rbc": "RBC",
    "plasma": "Plasma",
    "platelets": "Platelets",
    "whole": "Whole",
}
_LEVEL_MAP = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}

def _norm_bt(s: str) -> str:
    """Normalize blood type to uppercase (e.g., 'A-')."""
    return (s or "").strip().upper()

def norm_component_for_db(s: str) -> str:
    """
    Return exact DB value for component ('RBC', 'Plasma', 'Platelets', 'Whole')
    or raise ValueError on invalid input. Accepts canonical names or lower keys.
    """
    key = (s or "").strip().lower()
    if key in _COMPONENT_MAP:
        return _COMPONENT_MAP[key]
    if s in _COMPONENT_MAP.values():
        return s
    raise ValueError(f"Invalid component '{s}'. Must be one of {list(_COMPONENT_MAP.values())}.")

def parse_level(value):
    """
    Return int level 1–3 or None. Accepts 'LOW|MEDIUM|HIGH' or '1|2|3'.
    """
    if value in (None, ""):
        return None
    s = str(value).strip()
    if s.isdigit():
        return int(s)
    s_up = s.upper()
    if s_up in _LEVEL_MAP:
        return _LEVEL_MAP[s_up]
    raise ValueError("Invalid level. Use LOW/MEDIUM/HIGH or 1/2/3.")

# ID generation
def gen_transaction_id(db, entity_id: str) -> str:
    """
    Generate a unique transaction_id: '<ENTITY_ID>-<6 hex>'.
    Uniqueness is enforced against ops.transaction_logs.
    """
    while True:
        suffix = secrets.token_hex(3)  # 6 hex chars
        tx_id = f"{entity_id}-{suffix}"
        exists = db.execute(
            "SELECT 1 FROM ops.transaction_logs WHERE transaction_id = %s LIMIT 1;",
            (tx_id,)
        ).fetchone()
        if not exists:
            return tx_id

def next_request_id(db, origin_prefix: str) -> str:
    """
    Return next request id as '<prefix><NNNN>' based on the count of DISTINCT request_ids
    with that prefix. Use 'hops-' for hospital-origin requests and 'bank-' for bank-origin.
    """
    row = db.execute(
        "SELECT COALESCE(COUNT(DISTINCT request_id), 0) AS c "
        "FROM ops.transaction_logs WHERE request_id LIKE %s;",
        (origin_prefix + "%",)
    ).fetchone()
    n = int(row["c"]) + 1
    return f"{origin_prefix}{n:04d}"


# Inventory helpers functions, works on the basis of time series.
def get_bank_stock(db, bank_org_id: str, blood_type: str, component: str) -> int:
    """
    Return the latest known units for a bank/org for a blood_type + component.
    """
    bt = _norm_bt(blood_type)
    comp = norm_component_for_db(component)
    row = db.execute("""
        SELECT COALESCE(units, 0) AS units
          FROM ops.inventory
         WHERE org_id = %s AND blood_type = %s AND component = %s
         ORDER BY updated_at DESC
         LIMIT 1;
    """, (bank_org_id, bt, comp)).fetchone()
    return int(row["units"] if row else 0)

def get_inventory_units(db, org_id: str, blood_type: str, component: str) -> int:
    """
    Return the latest known units for any org for a blood_type + component.
    """
    row = db.execute("""
        SELECT COALESCE(units, 0) AS units
          FROM ops.inventory
         WHERE org_id = %s AND blood_type = %s AND component = %s
         ORDER BY updated_at DESC
         LIMIT 1;
    """, (org_id, _norm_bt(blood_type), norm_component_for_db(component))).fetchone()
    return int(row["units"] if row else 0)

def upsert_inventory(db, org_id: str, blood_type: str, component: str, delta_units: int):
    """
    Append a new inventory version row (Timeseries style):
    new_units = max(0, latest_units + delta_units).
    """
    bt = _norm_bt(blood_type)
    comp = norm_component_for_db(component)

    prev = db.execute("""
        SELECT units
          FROM ops.inventory
         WHERE org_id = %s AND blood_type = %s AND component = %s
         ORDER BY updated_at DESC
         LIMIT 1;
    """, (org_id, bt, comp)).fetchone()

    prev_units = int(prev["units"]) if prev and prev["units"] is not None else 0
    new_units  = max(0, prev_units + int(delta_units))

    db.execute("""
        INSERT INTO ops.inventory (org_id, blood_type, component, units, updated_at)
        VALUES (%s, %s, %s, %s, NOW());
    """, (org_id, bt, comp, new_units))

# Optional utility
def find_bank_with_stock(db, blood_type: str, component: str, min_units: int):
    """
    Utility to query a bank with enough stock (latest snapshot).
    Keep for diagnostics or manual selection UIs.
    NOTE: The app should NOT auto-fulfill on create; fulfillment is done by manual accept on the dashboard.
    """
    bt = _norm_bt(blood_type)
    comp = norm_component_for_db(component)

    return db.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (i.org_id)
                   i.org_id, i.units
              FROM ops.inventory i
              JOIN core.organizations o ON o.org_id = i.org_id
             WHERE o.org_type = 'BloodBank'
               AND i.blood_type = %s
               AND  ORDER BY i.org_id, i.updated_at DESC
        )
        SELECT org_id AS bank_id, COALESCE(units, 0) AS units
          FROM latest
         WHERE COALESCE(units, 0) >= %s
         ORDER BY units DESC, org_id
         LIMIT 1;
    """, (bt, comp, int(min_units))).fetchone()



def notify_donors_for_request(db, request_id: str, donors) -> None:
    """
    For a given request_id and an iterable of donor rows, log and send email notifications.

    `donors` is expected to be an iterable of dict-like rows with at least:
      - donor_id      (or DonorId)
      - email         (or Email)
      - preferred_contact (optional, defaults to 'Email')
      - firstname / lastname (or FirstName / LastName) — optional, used in greeting

    Behavior:
      - For each donor, ensures exactly ONE row in ops.notifications per (donor_id, request_id)
        thanks to the UNIQUE (donor_id, request_id) constraint.
      - Increments attempt_count on every send attempt.
      - Sets:
          status       = 'SENT'   on success
                       = 'FAILED' on exception during send
                       = 'PENDING' if we are not sending email (SMS-only, or capped)
          sent_at      = NOW()    on success
          error_message= exception text on failure
          email_to     = email we actually used
          preferred_contact = current preference snapshot
      - Only *sends* email if:
            • preferred_contact is 'Email' or 'Both' (case-insensitive), AND
            • we have not exceeded the per-call email cap.
        Otherwise it still logs/updates the notification row but skips sending.

    NOTE: This function does NOT call db.commit(). Caller must commit/rollback.
    """
    if not request_id or not donors:
        return

    # Cap: maximum number of actual emails we send in a single call
    MAX_EMAILS_PER_CALL = 7
    emails_sent = 0

    for d in donors:
        # Extract donor_id in a robust way.
        donor_id = (
            d.get("donor_id")
            or d.get("DonorId")
            or d.get("id")
        )
        donor_id = str(donor_id) if donor_id is not None else None

        # Extract email and preferred_contact
        email_to = (d.get("email") or d.get("Email") or "").strip()
        preferred = (d.get("preferred_contact") or
                     d.get("PreferredContact") or
                     "Email").strip() or "Email"

        # Skip if we have no donor_id or no email address
        if not donor_id or not email_to:
            continue

        # Upsert-style: find existing notification row for (donor_id, request_id)
        existing = db.execute("""
            SELECT notification_id, attempt_count, status
              FROM ops.notifications
             WHERE donor_id = %s
               AND request_id = %s;
        """, (donor_id, request_id)).fetchone()

        if existing:
            notif_id = existing["notification_id"]
            # Keep created_at, increment attempt_count in UPDATE below
        else:
            # Create a fresh PENDING row with attempt_count = 0
            row = db.execute("""
                INSERT INTO ops.notifications (
                    donor_id,
                    request_id,
                    preferred_contact,
                    status,
                    email_to,
                    attempt_count,
                    error_message
                )
                VALUES (%s, %s, %s, 'PENDING', %s, 0, NULL)
                ON CONFLICT (donor_id, request_id) DO NOTHING
                RETURNING notification_id;
            """, (donor_id, request_id, preferred, email_to)).fetchone()
            
            if row:
                notif_id = row['notification_id']
            elif existing:
                 notif_id = existing['notification_id']
            else:
                 # If it was inserted by someone else right now, query again
                 existing_retry = db.execute("""
                    SELECT notification_id FROM ops.notifications
                    WHERE donor_id = %s AND request_id = %s
                 """, (donor_id, request_id)).fetchone()
                 if existing_retry:
                     notif_id = existing_retry['notification_id']
                 else:
                     continue # Should not happen

        if notif_id is None:
            # Something went wrong with insert; skip this donor safely
            continue

        # Decide on whether we actually send an email or just log
        pc_lower = preferred.lower()
        wants_email = pc_lower == "email" or pc_lower == "both"
        can_send_now = wants_email and (emails_sent < MAX_EMAILS_PER_CALL)

        # Build name for greeting if available
        first_name = d.get("firstname") or d.get("FirstName") or ""
        last_name = d.get("lastname") or d.get("LastName") or ""
        full_name = (f"{first_name} {last_name}").strip() or "donor"

        if can_send_now:
            try:
                subject = f"BloodBridge – New request {request_id}"
                body = (
                    f"Dear {full_name},\n\n"
                    f"You have a new request on your BloodBridge dashboard.\n"
                    f"Request ID: {request_id}\n\n"
                    f"Please log in to view details and respond.\n\n"
                    f"- BloodBridge"
                )

                # Attempt to send the email
                send_email(email_to, subject, body)
                emails_sent += 1

                # Mark as SENT, increment attempt_count, record sent_at & current snapshot
                db.execute("""
                    UPDATE ops.notifications
                       SET status            = 'SENT',
                           sent_at           = NOW(),
                           attempt_count     = attempt_count + 1,
                           email_to          = %s,
                           preferred_contact = %s,
                           error_message     = NULL
                     WHERE notification_id   = %s;
                """, (email_to, preferred, notif_id))

            except Exception as e:
                # On error, increment attempt_count, record error, mark as FAILED
                db.execute("""
                    UPDATE ops.notifications
                       SET status            = 'FAILED',
                           attempt_count     = attempt_count + 1,
                           email_to          = %s,
                           preferred_contact = %s,
                           error_message     = %s
                     WHERE notification_id   = %s;
                """, (email_to, preferred, str(e), notif_id))
        else:
            # Either:
            #   - Donor does not prefer email (e.g. SMS only), OR
            #   - We have hit the per-call cap on emails.
            # In both cases, log/update the row but keep it PENDING and do not send.
            db.execute("""
                UPDATE ops.notifications
                   SET status            = 'PENDING',
                       email_to          = %s,
                       preferred_contact = %s,
                       error_message     = NULL
                 WHERE notification_id   = %s;
            """, (email_to, preferred, notif_id))



# User model
class User(UserMixin):
    __slots__ = ("id","username","role","org_id","donor_id")
    def __init__(self, user_pk, username, role, org_id=None, donor_id=None):
        self.id = str(user_pk)
        self.username = username
        self.role = role
        self.org_id = str(org_id) if org_id is not None else None
        self.donor_id = str(donor_id) if donor_id is not None else None
    def get_id(self): return self.id

# loader
@login_manager.user_loader
def load_user(user_id):
    db = get_db()
    row = db.execute("""
        SELECT username, id, role
        FROM core.auth
        WHERE username = %s OR lower(username) = lower(%s)
        LIMIT 1;
    """, (user_id, user_id)).fetchone()
    if not row: return None

    if row["role"] in ("Hospital","BloodBank"):
        return User(row["username"], row["username"], row["role"], org_id=row["id"])
    if row["role"] == "Donor":
        return User(row["username"], row["username"], row["role"], donor_id=row["id"])
    return User(row["username"], row["username"], row["role"])



# Auth utils
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
_ph = PasswordHasher()

def verify_password(plain_text: str, stored_value: str) -> bool:
    if not stored_value:
        return False
    try:
        return _ph.verify(stored_value, plain_text)
    except VerifyMismatchError:
        return False
    except Exception:
        return False

def role_required(*roles):
    def wrapper(fn):
        @wraps(fn)
        def decorated_view(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('login'))
            if current_user.role not in roles:
                return redirect(url_for('dashboard'))
            return fn(*args, **kwargs)
        return decorated_view
    return wrapper

# Inventory management UI
@app.route('/inventory/manage')
@login_required
def manage_inventory():
    db = get_db()
    rows = db.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (i.blood_type, i.component)
                   i.org_id, i.blood_type, i.component, i.units, i.updated_at
              FROM ops.inventory i
             WHERE i.org_id = %s
             ORDER BY i.blood_type, i.component, i.updated_at DESC
        )
        SELECT NULL::bigint AS id,
               org_id,
               UPPER(blood_type) AS blood_type,
               UPPER(component)  AS component,
               COALESCE(units,0) AS units,
               COALESCE(updated_at::text, '') AS updated_at
          FROM latest
         ORDER BY blood_type, component;
    """, (current_user.org_id,)).fetchall()

    return render_template('inventory_manage.html',
                           org=current_user.org_id,
                           role=current_user.role,
                           inventory=rows)

@app.route('/inventory/update', methods=['POST'])
@login_required
def update_inventory():
    db = get_db()
    org_id = current_user.org_id

    blood_type = _norm_bt(request.form.get('blood_type') or '')
    component_in = (request.form.get('component') or '')
    action     = (request.form.get('action') or '').strip().lower()

    try:
        component = norm_component_for_db(component_in)
    except ValueError:
        return redirect(url_for('manage_inventory'))

    if not blood_type or not component:
        return redirect(url_for('manage_inventory'))

    units_raw = request.form.get('units')
    try:
        units_val = int(units_raw) if units_raw not in (None, '') else 0
    except ValueError:
        return redirect(url_for('manage_inventory'))

    try:
        if action == 'set':
            current_units = get_inventory_units(db, org_id, blood_type, component)
            delta = int(units_val) - current_units
            upsert_inventory(db, org_id, blood_type, component, delta)
            db.commit()

        elif action == 'add':
            if units_val > 0:
                upsert_inventory(db, org_id, blood_type, component, +units_val)
                db.commit()

        elif action == 'remove':
            if units_val > 0:
                upsert_inventory(db, org_id, blood_type, component, -units_val)
                db.commit()

        elif action == 'delete':
            current_units = get_inventory_units(db, org_id, blood_type, component)
            if current_units > 0:
                upsert_inventory(db, org_id, blood_type, component, -current_units)
            else:
                upsert_inventory(db, org_id, blood_type, component, 0)
            db.commit()

        else:
            pass

    except Exception:
        db.rollback()

    return redirect(url_for('manage_inventory'))

# Routes
@app.route('/admin/send-notifications')
@login_required
@role_required('Hospital', 'BloodBank')
def admin_send_notifications():
    db = get_db()
    send_pending_notifications(db)
    flash("Notifications processed.", "info")
    return redirect(url_for('dashboard'))



@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

# Helpers for timestamps in inserts
EARLIEST_REQ_SQL = """
COALESCE(
  (SELECT requested_at
     FROM ops.transaction_logs
    WHERE request_id = %s
    ORDER BY requested_at ASC
    LIMIT 1),
  NOW()
)
"""

def insert_open_event(db, tx_id, request_id, hospital_id, blood_type, component, level, units, requested_at_sql="NOW()"):
    """OPEN event: requested_at provided (default NOW()), completed_at NULL."""
    db.execute(f"""
        INSERT INTO ops.transaction_logs
            (transaction_id,
             request_id,
             requester_entity_type, requester_entity_id,
             blood_type, component, level,
             units_requested, status, requested_at, completed_at)
        VALUES
            (%s,
             %s,
             'Hospital', %s,
             %s, %s, %s,
             %s, 'OPEN', {requested_at_sql}, NULL);
    """, (tx_id, request_id, hospital_id,
          blood_type, component, level, units))

def insert_fulfillment_event(db, tx_id, request_id, hospital_id, bank_id, blood_type, component, level, units):
    """FULFILLED/PARTIAL/REJECTED/EXPIRED helper uses earliest requested_at and NOW() completed."""
    # status is determined by caller (FULFILLED/PARTIAL/REJECTED/EXPIRED); we pass it.
    pass

@app.route('/request/new', methods=['GET', 'POST'])
@login_required
def new_request():
    """
    Hospital request creation.

    - If user is Hospital:
        * GET  → show unified form (Emergency or Blood Drive).
        * POST → create either:
            - Emergency (Level 1)  → sent to Blood Banks first.
            - Blood Drive (Level 2) → sent only to Donors.
    - Any other role: redirect to dashboard.
    """
    role = (current_user.role or "").strip()

    # Only Hospitals can create requests here
    if role != "Hospital":
        return redirect(url_for('dashboard'))

    db = get_db()

    if request.method == 'POST':
        # What kind of request did the user choose?, we have first one is for emergency and then blood drive which is level 2
        urgency_kind = (request.form.get('urgency_kind') or '').strip().lower()

        # BLOOD DRIVE (LEVEL 2) → BROADCAST TO DONORS ONLY
        if urgency_kind == 'blood_drive':
            drive_date     = (request.form.get('drive_date') or '').strip()
            drive_location = (request.form.get('drive_location') or '').strip()
            drive_radius_raw = (request.form.get('drive_radius') or '').strip()

            # Basic validation: require both date and location
            if not drive_date or not drive_location:
                flash("Please fill both date and location.", "error")
                return redirect(url_for('new_request'))

            # Editable radius, default 25 miles for blood drive
            try:
                drive_radius = int(drive_radius_raw) if drive_radius_raw else 25
            except ValueError:
                drive_radius = 25
            if drive_radius < 1:
                drive_radius = 1

            prefix     = f"{current_user.org_id}-req-"
            request_id = next_request_id(db, prefix)
            tx_id      = gen_transaction_id(db, current_user.org_id)

            # Notes are purely descriptive (no radius embedded)
            notes_text = f"Blood Drive | Date: {drive_date} | Location: {drive_location}"

            try:
                # 1) Insert the blood drive request row
                db.execute("""
                    INSERT INTO ops.transaction_logs (
                        transaction_id,
                        request_id,
                        requester_entity_type, requester_entity_id,
                        blood_type, component, level,
                        units_requested, units_fulfilled,
                        status,
                        requested_at, completed_at,
                        fulfilled_by_entity_type, fulfilled_by_entity_id,
                        notes, inventory_updated,
                        request_to,
                        radius_miles
                    )
                    VALUES (
                        %s, %s,
                        'Hospital', %s,
                        NULL, NULL, 2,
                        1000, 0,
                        'OPEN',
                        NOW(), NULL,
                        NULL, NULL,
                        %s, NULL,
                        'Donor',
                        %s
                    );
                """, (
                    tx_id, request_id,
                    current_user.org_id,
                    notes_text,
                    drive_radius
                ))

                # 2) Get the hospital's location (origin of the blood drive)
                org_loc = db.execute("""
                    SELECT latitude, longitude
                      FROM core.organizations
                     WHERE org_id = %s;
                """, (current_user.org_id,)).fetchone()

                donors_in_radius = []
                if org_loc and org_loc["latitude"] is not None and org_loc["longitude"] is not None:
                    org_lat = org_loc["latitude"]
                    org_lon = org_loc["longitude"]

                    # 3) Fetch all donors eligible for blood drives (level 2 or 3) in nearby vicinity of 25 miles default or chnageable to any x miles.
                    donor_rows = db.execute("""
                        SELECT donor_id,
                               firstname,
                               lastname,
                               email,
                               preferred_contact,
                               latitude,
                               longitude
                          FROM core.donors
                         WHERE level IN (2, 3);
                    """).fetchall()

                    # 4) Filter donors by radius using haversine_distance
                    for d in donor_rows:
                        d_lat = d["latitude"]
                        d_lon = d["longitude"]
                        if d_lat is None or d_lon is None:
                            continue

                        dist = haversine_distance(org_lat, org_lon, d_lat, d_lon)
                        if dist <= drive_radius:
                            donors_in_radius.append(d)

                # 5) Log + send notifications for all eligible donors
                if donors_in_radius:
                    notify_donors_for_request(db, request_id, donors_in_radius)

                # 6) Commit everything (request + notifications) in the ops.notifications table
                db.commit()

                flash("Blood Drive request created successfully!", "success")
                return redirect(url_for('view_requests'))

            except Exception as e:
                db.rollback()
                app.logger.exception("Blood Drive creation failed")
                flash("Error creating Blood Drive request.", "error")
                return redirect(url_for('new_request'))


        # EMERGENCY (LEVEL 1) → BANKS → HOSPITALS → DONORS
        # Emergency request fields
        blood_type = _norm_bt(request.form.get('blood_type'))
        component_raw = request.form.get('component')

        try:
            component = norm_component_for_db(component_raw)
        except ValueError:
            return render_template('request_form.html', error="Invalid component")

        try:
            units = int(request.form.get('units') or 0)
        except ValueError:
            units = 0

        try:
            radius = int(request.form.get('radius') or 10)
        except ValueError:
            radius = 10
        if radius < 1:
            radius = 1

        if not blood_type or not component or units <= 0:
            # Basic validation failed
            return render_template('request_form.html', error="Invalid input")

        # Level 1 (Emergency), initial target: BloodBank
        prefix     = f"{current_user.org_id}-req-"
        request_id = next_request_id(db, prefix)
        tx_id      = gen_transaction_id(db, current_user.org_id)

        try:
            db.execute("""
                INSERT INTO ops.transaction_logs (
                    transaction_id,
                    request_id,
                    requester_entity_type, requester_entity_id,
                    blood_type, component, level,
                    units_requested, units_fulfilled,
                    status,
                    requested_at, completed_at,
                    fulfilled_by_entity_type, fulfilled_by_entity_id,
                    notes, inventory_updated,
                    request_to,
                    radius_miles
                )
                VALUES (
                    %s, %s,
                    'Hospital', %s,
                    %s, %s, 1,
                    %s, 0,
                    'OPEN',
                    NOW(), NULL,
                    NULL, NULL,
                    'Emergency Request', NULL,
                    'BloodBank',
                    %s
                );
            """, (
                tx_id, request_id, current_user.org_id,
                blood_type, component, units,
                radius
            ))
            db.commit()
            return redirect(url_for('track_request', request_id=request_id))

        except Exception as e:
            db.rollback()
            app.logger.exception("Failed to create new emergency request")
            return render_template('request_form.html', error="Database error")

    # GET request - show the form
    return render_template("request_form.html", org_id=current_user.org_id)


@app.route('/register', methods=['GET', 'POST'])
def register():
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username_in = (request.form.get('username') or '').strip()
        password_in = (request.form.get('password') or '')
        if not username_in or not password_in:
            return redirect(url_for('login'))

        db = get_db()
        row = db.execute("""
            SELECT username, id, role, password
              FROM core.auth
             WHERE lower(username) = lower(%s)
             LIMIT 1;
        """, (username_in,)).fetchone()

        from argon2 import PasswordHasher
        from argon2.exceptions import VerifyMismatchError
        _ph = PasswordHasher()

        def verify_password(plain_text: str, stored_value: str) -> bool:
            if not stored_value:
                return False
            try:
                return _ph.verify(stored_value, plain_text)
            except VerifyMismatchError:
                return False
            except Exception:
                return False

        if row and verify_password(password_in, row['password']):
            role = row["role"]
            if role in ("Hospital", "BloodBank"):
                user_obj = User(row['username'], row['username'], role, org_id=row["id"])
            elif role == "Donor":
                user_obj = User(row['username'], row['username'], role, donor_id=row["id"])
            else:
                user_obj = User(row['username'], row['username'], role)
            login_user(user_obj)
            return redirect(url_for('dashboard'))
        else:
            return redirect(url_for('login'))

    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def dashboard():
    db = get_db()

    # Automatically expire requests older than 20 minutes
    db.execute("""
        UPDATE ops.transaction_logs
        SET status = 'EXPIRED'
        WHERE status = 'OPEN' 
          AND requested_at < NOW() - INTERVAL '20 minutes';
    """)
    db.commit()
    
    # 1. DONOR DASHBOARD
    if current_user.role == "Donor":
        # --- Fetch Donor Details ---
        donor = db.execute("""
            SELECT
              d.donor_id         AS "DonorId",
              d.firstname        AS "FirstName",
              d.lastname         AS "LastName",
              UPPER(d.bloodtype) AS "BloodType",
              d.age              AS "Age",
              d.gender           AS "Gender",
              d.city             AS "City",
              d.state            AS "State",
              d.level            AS "Level",
              d.last_donation_date   AS last_donation_date,
              d.total_donations      AS total_donations
            FROM core.donors d
            WHERE d.donor_id = %s
        """, (current_user.donor_id,)).fetchone()
        
        if donor:
            donor = dict(donor)
            
            # Use DB value directly form the core.donors table
            t_don = donor.get('total_donations') or 1
            
            donor['total_donations'] = t_don
            donor['lives_impacted'] = max(t_don // 3, 1) # Standard estimate
            donor['preferred_center'] = None 
            
            # Calculate days_since_last_donation from the core.donors table
            if donor.get('last_donation_date'):
               today = datetime.datetime.now().date()
               delta = today - donor['last_donation_date']
               donor['days_since_last_donation'] = delta.days
               donor['last_donation_date'] = donor['last_donation_date'].strftime('%Y-%m-%d')
            else:
               donor['days_since_last_donation'] = 0


        requests_rows = []

        # If donor is valid level, fetch requests for them
        if donor and int(donor["Level"]) in (1, 2, 3):
            
            # Requests this donor has already acted on (Fulfilled or Rejected)
            acted_on = db.execute("""
                SELECT DISTINCT request_id 
                  FROM ops.transaction_logs
                 WHERE fulfilled_by_entity_type = 'Donor'
                   AND fulfilled_by_entity_id   = %s
                   AND status IN ('REJECTED','FULFILLED','PARTIAL')
            """, (current_user.donor_id,)).fetchall()
            
            # Create a set of IDs to hide: {'req-001', 'hops-002'}
            hidden_ids = {row['request_id'] for row in acted_on}


            # Fetch Raw Requests (Emergencies + Drives)
            donor_raw_sql = """
                SELECT
                    t.request_id,
                    t.requester_entity_id,
                    t.blood_type,
                    t.component,
                    t.level,
                    t.units_requested,
                    t.units_fulfilled,
                    t.requested_at,
                    t.status,
                    t.radius_miles,
                    o.name,
                    o.city,
                    o.state,
                    o.org_type,
                    o.latitude, o.longitude
                FROM ops.transaction_logs t
                JOIN core.organizations o ON o.org_id = t.requester_entity_id
                WHERE t.requester_entity_type IN ('Hospital', 'BloodBank')
                  AND t.level IN (1, 2)
                  AND t.status IN ('OPEN', 'PARTIAL')
                  AND (
                      (t.level = 1 AND t.requested_at >= NOW() - INTERVAL '20 minutes')
                      OR
                      (t.level = 2)
                  )
                ORDER BY t.requested_at DESC;
            """
            raw_rows = db.execute(donor_raw_sql).fetchall()
            
            # Get donor location
            donor_loc = db.execute(
                "SELECT latitude, longitude FROM core.donors WHERE donor_id = %s",
                (current_user.donor_id,)
            ).fetchone()
            d_lat = donor_loc['latitude'] if donor_loc else None
            d_lon = donor_loc['longitude'] if donor_loc else None
            
            # Get Donor Level for Visibility Rules
            donor_level_row = db.execute("SELECT level FROM core.donors WHERE donor_id = %s", (current_user.donor_id,)).fetchone()
            donor_level = int(donor_level_row['level']) if donor_level_row else 1
            
            donor_requests_map = {}
            for row in raw_rows:
                rid = row['request_id']

                # VISIBILITY CHECK: Skip if acted on ---
                if rid in hidden_ids:
                    continue

                if rid not in donor_requests_map:
                    donor_requests_map[rid] = {
                        'request_id': rid,
                        'hospital_id': row['requester_entity_id'],
                        'name': row['name'],
                        'city': row['city'],
                        'state': row['state'],
                        'org_type': row['org_type'],
                        'blood_type': row['blood_type'],
                        'component': row['component'],
                        'level': row['level'],
                        'total_requested': 0,
                        'total_fulfilled': 0,
                        'requested_at': row['requested_at'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'radius_miles': row['radius_miles'],
                        'status': 'OPEN'
                    }
                
                if row['units_requested']:
                    donor_requests_map[rid]['total_requested'] = max(
                        donor_requests_map[rid]['total_requested'],
                        int(row['units_requested'])
                    )
                    if row['units_fulfilled'] is None or row['units_fulfilled'] == 0:
                        donor_requests_map[rid]['requested_at'] = row['requested_at']

                if row['units_fulfilled']:
                    donor_requests_map[rid]['total_fulfilled'] += int(row['units_fulfilled'])


            # Process & Filter the Map
            for rid, data in donor_requests_map.items():
                is_drive = (data['level'] == 2)
                
                # 1. Check Remaining Units (Skip for Blood Drives)
                if not is_drive:
                    remaining = data['total_requested'] - data['total_fulfilled']
                    if remaining <= 0:
                        continue
                        
                # 2. Donor Level Visibility Mapping
                # Request Level 1 (Emergency) -> See if Donor Level IN (1, 3)
                # Request Level 2 (Blood Drive) -> See if Donor Level == 2
                req_lvl = int(data['level'] or 1)
                
                if req_lvl == 1:
                    # Emergency (L1) -> Visible to Donor 1, 3
                    if donor_level not in (1, 3):
                        continue
                elif req_lvl == 2:
                    # Blood Drive (L2) -> Visible to Donor 2, 3
                    if donor_level not in (2, 3):
                        continue
                
                # 3. Check Blood Type Match (Skip for Blood Drives)
                if not is_drive:
                    req_bt = _norm_bt(data['blood_type'])
                    donor_bt = _norm_bt(donor["BloodType"])

                    compatible_donors = {
                        "O-": {"O-"},
                        "O+": {"O-", "O+"},
                        "A-": {"O-", "A-"},
                        "A+": {"O-", "O+", "A-", "A+"},
                        "B-": {"O-", "B-"},
                        "B+": {"O-", "O+", "B-", "B+"},
                        "AB-": {"O-", "A-", "B-", "AB-"},
                        "AB+": {"O-", "O+", "A-", "A+", "B-", "B+", "AB-", "AB+"},
                    }

                    if donor_bt not in compatible_donors.get(req_bt, set()):
                        continue
                
                # 3. Check Escalation Time (> 6 mins for Level 1; Immediate for Level 2), this is just for demo purpose, can be changable as per the requirement.
                elapsed = 0
                if data['requested_at']:
                    elapsed = (
                        datetime.datetime.now(datetime.timezone.utc) - data['requested_at']
                    ).total_seconds() / 60.0
                
                if not is_drive:
                    if elapsed <= 6:
                        continue
                    
                # 4. Check Distance
                dist = 9999.0
                if d_lat is not None and data['latitude'] is not None:
                    dist = haversine_distance(
                        d_lat, d_lon,
                        data['latitude'], data['longitude']
                    )
                
                default_radius = 100 if is_drive else 10
                req_radius = int(data.get('radius_miles') or default_radius)
                
                if dist > req_radius:
                    continue
                
                # Add to list
                calc_remaining = int(data['total_requested'] or 0) - int(data['total_fulfilled'] or 0)
                data['units'] = calc_remaining
                data['status'] = 'PARTIAL' if data['total_fulfilled'] > 0 else 'OPEN'
                data['ts'] = data['requested_at']
                data['drive_location'] = None # or extract from notes if structured
                requests_rows.append(data)

        # Sort combined rows by most recent timestamp
        requests_rows = sorted(requests_rows, key=lambda r: r.get("ts") or datetime.datetime.min, reverse=True)

        return render_template(
            "donor_dashboard.html",
            donor=donor,
            requests_rows=requests_rows
        )

    # 2. ORG DASHBOARD (Hospital / BloodBank)
    org = db.execute("""
        SELECT org_id, org_type, name, address, city, state, zip, phone, email, latitude, longitude
          FROM core.organizations
         WHERE org_id = %s;
    """, (current_user.org_id,)).fetchone()

    q          = (request.args.get('q') or '').strip()
    blood_type = _norm_bt(request.args.get('blood_type') or '')
    component_in  = (request.args.get('component') or '')

    component = None
    if component_in:
        try:
            component = norm_component_for_db(component_in)
        except ValueError:
            component = None

    filters = {"q": q.upper(), "blood_type": blood_type, "component": (component or '').upper()}

    where_clauses = ["i.org_id = %s"]
    params = [current_user.org_id]

    if blood_type:
        where_clauses.append("i.blood_type = %s")
        params.append(blood_type)
    if component:
        where_clauses.append("i.component = %s")
        params.append(component)
    if q:
        where_clauses.append("(UPPER(i.blood_type) LIKE %s OR UPPER(i.component) LIKE %s)")
        like = f"%{q.upper()}%"
        params.extend([like, like])

    sql = f"""
        WITH latest AS (
            SELECT DISTINCT ON (i.blood_type, i.component)
                   i.org_id, i.blood_type, i.component, i.units, i.updated_at
              FROM ops.inventory i
             WHERE {" AND ".join(where_clauses)}
             ORDER BY i.blood_type, i.component, i.updated_at DESC
        )
        SELECT org_id,
               UPPER(blood_type) AS blood_type,
               UPPER(component)  AS component,
               COALESCE(units,0) AS units,
               COALESCE(updated_at::text,'') AS updated_at
          FROM latest
          ORDER BY blood_type, component;
    """
    inventory_rows = db.execute(sql, tuple(params)).fetchall()

    return render_template(
        'org_dashboard.html',
        org=org,
        role=current_user.role,
        inventory=inventory_rows,
        filters=filters
    )
@app.route('/requests')
@login_required
def view_requests():
    db = get_db()

    # Automatically expire requests older than 20 minutes (OPEN or PARTIAL), this is demo purpose again, can be inputable to different values
    db.execute("""
        UPDATE ops.transaction_logs
        SET status = 'EXPIRED'
        WHERE status IN ('OPEN', 'PARTIAL')
          AND requested_at < NOW() - INTERVAL '20 minutes';
    """)
    db.commit()

    # 1. Parsing Query Params
    q          = (request.args.get('q') or '').strip()
    blood_type = _norm_bt((request.args.get('blood_type') or ''))
    component_in  = (request.args.get('component') or '')
    status     = (request.args.get('status') or '').strip().upper()

    component = None
    if component_in:
        try:
            component = norm_component_for_db(component_in)
        except ValueError:
            component = None

    filters = {
        "q": q.upper(),
        "blood_type": blood_type,
        "component": (component or '').upper(),
        "status": status,
    }

    # 2. Get Current User Location
    user_loc = db.execute(
        "SELECT latitude, longitude FROM core.organizations WHERE org_id = %s",
        (current_user.org_id,)
    ).fetchone()
    user_lat = user_loc['latitude'] if user_loc else None
    user_lon = user_loc['longitude'] if user_loc else None

    # 3. Fetch Blocklist (Requests that got rejected)
    # We fetch IDs where fulfilled_by_entity_id == current_user.org_id AND status == 'REJECTED'
    rejected_rows = db.execute("""
        SELECT DISTINCT request_id
          FROM ops.transaction_logs
         WHERE fulfilled_by_entity_id = %s
           AND status = 'REJECTED'
    """, (current_user.org_id,)).fetchall()
    
    # Create a set for fast O(1) lookups
    my_rejected_ids = {row['request_id'] for row in rejected_rows}

    # 4. Fetch Raw Logs for Aggregation
    raw_sql = """
        SELECT
            t.request_id,
            t.requester_entity_id,
            t.blood_type,
            t.component,
            t.level,
            t.units_requested,
            t.units_fulfilled,
            t.requested_at,
            t.status,
            t.notes,
            t.radius_miles,
            o.name AS hospital_name,
            o.city, o.state,
            o.latitude, o.longitude
        FROM ops.transaction_logs t
        JOIN core.organizations o ON o.org_id = t.requester_entity_id
        WHERE t.requester_entity_type = 'Hospital'
          AND t.requested_at >= NOW() - INTERVAL '20 minutes'
          AND (t.status = 'OPEN' or t.status = 'PARTIAL')
          AND t.level = 1
        ORDER BY t.requested_at DESC;
    """
    raw_rows = db.execute(raw_sql).fetchall()

    # 5. Python-Side Aggregation
    requests_map = {}
    for row in raw_rows:
        rid = row['request_id']
        
        # SKIP if I have already rejected this request
        if rid in my_rejected_ids:
            continue

        if rid not in requests_map:
            requests_map[rid] = {
                'request_id': rid,
                'hospital_id': row['requester_entity_id'],
                'hospital_name': row['hospital_name'],
                'city': row['city'],
                'state': row['state'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'blood_type': row['blood_type'],
                'component': row['component'],
                'level': row['level'],
                'total_requested': 0,
                'total_fulfilled': 0,
                'requested_at': row['requested_at'],
                'status': 'OPEN',
                'notes': row['notes'],
                'radius_miles': row['radius_miles'],
            }

        # Summation logic
        if row['units_requested']:
            requests_map[rid]['total_requested'] = max(
                requests_map[rid]['total_requested'],
                int(row['units_requested'])
            )
            # Ensure we keep the creation timestamp
            if row['units_fulfilled'] is None or row['units_fulfilled'] == 0:
                requests_map[rid]['requested_at'] = row['requested_at']

        if row['units_fulfilled']:
            requests_map[rid]['total_fulfilled'] += int(row['units_fulfilled'])

    # 6. Build Active List
    all_active = []
    import datetime

    for rid, data in requests_map.items():
        remaining = data['total_requested'] - data['total_fulfilled']
        
        # If fulfilled, it's not "active" for the incoming list
        if remaining > 0:
            if data['requested_at']:
                elapsed = (
                    datetime.datetime.now(datetime.timezone.utc) - data['requested_at']
                ).total_seconds() / 60.0
            else:
                elapsed = 0

            data['units'] = remaining
            data['remaining_units'] = remaining
            data['elapsed_minutes'] = elapsed
            data['status'] = 'PARTIAL' if data['total_fulfilled'] > 0 else 'OPEN'
            all_active.append(data)

    # Sort newest first
    all_active.sort(
        key=lambda x: x['requested_at'] or datetime.datetime.min,
        reverse=True
    )

    # 7. Apply Filters (Role, Distance, Time)
    visible_requests = []

    for r in all_active:
        # Standard Search Filters
        if blood_type and _norm_bt(r['blood_type']) != blood_type: continue
        if component and norm_component_for_db(r['component']) != component: continue
        if q:
            search_str = f"{r['hospital_name']} {r['city']} {r['state']} {r['blood_type']} {r['component']}"
            if q.upper() not in search_str.upper(): continue

        # Escalation / Distance Filters
        elapsed = float(r['elapsed_minutes'] or 0)
        dist = 999999.0
        if user_lat is not None and r['latitude'] is not None:
            dist = haversine_distance(user_lat, user_lon, r['latitude'], r['longitude'])

        radius = int(r.get('radius_miles') or 10)
        is_visible = False

        if current_user.role == 'BloodBank':
            # Banks see immediately if within radius
            if dist <= radius:
                is_visible = True
        elif current_user.role == 'Hospital':
            # Hospitals see PEER requests after 3 minutes
            # EXCEPTION: Blood Drives (Level 2) are direct-to-donor only, PEERS HOSPITALS don't see them.
            if r['hospital_id'] != current_user.org_id:
                if str(r.get('level', '')) != '2':
                     if elapsed > 3 and dist <= radius:
                         is_visible = True
            # Note: Own requests are handled in the "My Requests" section below

        if is_visible:
            r_dict = dict(r)
            r_dict['status'] = 'OPEN' if r['total_fulfilled'] == 0 else 'PARTIAL'
            r_dict['created_at'] = r['requested_at']
            visible_requests.append(r_dict)

    # 8. "My Requests" (History for the requester)
    my_raw_sql = """
        SELECT
            t.request_id,
            t.requester_entity_id,
            t.blood_type,
            t.component,
            t.level,
            t.units_requested,
            t.units_fulfilled,
            t.requested_at,
            t.status
        FROM ops.transaction_logs t
        WHERE t.requester_entity_id = %s
        ORDER BY t.requested_at DESC;
    """
    my_raw_rows = db.execute(my_raw_sql, (current_user.org_id,)).fetchall()

    my_requests_map = {}
    for row in my_raw_rows:
        rid = row['request_id']
        if rid not in my_requests_map:
            my_requests_map[rid] = {
                'request_id': rid,
                'hospital_id': row['requester_entity_id'],
                'blood_type': row['blood_type'],
                'component': row['component'],
                'level': row['level'],
                'total_requested': 0,
                'total_fulfilled': 0,
                'requested_at': row['requested_at'],
                'remaining_units': 0,
                'requested_at': row['requested_at'],
                'db_status': row['status']
            }

        if row['units_requested']:
            my_requests_map[rid]['total_requested'] = max(
                my_requests_map[rid]['total_requested'],
                int(row['units_requested'])
            )
            if row['units_fulfilled'] is None or row['units_fulfilled'] == 0:
                my_requests_map[rid]['requested_at'] = row['requested_at']

        if row['units_fulfilled']:
            my_requests_map[rid]['total_fulfilled'] += int(row['units_fulfilled'])

    my_requests_all = []
    today_date = datetime.date.today()

    for rid, data in my_requests_map.items():
        remaining = data['total_requested'] - data['total_fulfilled']
        data['units'] = remaining
        data['remaining_units'] = remaining

        # Logic: If DB says EXPIRED, we got everything we need.
        # Otherwise, calculate based on units.
        db_st = data.get('db_status', 'OPEN')
        
        # Check time-based expiration (Forcefully expire if > 20 mins)
        # We need timezone-aware comparison. 'requested_at' is likely offset-aware from DB.
        # datetime.datetime.now(datetime.timezone.utc) is safest if DB is UTC.
        import datetime
        
        is_force_expired = False
        if data['requested_at']:
             # Ensure we have a timezone-aware 'now' 
             now_utc = datetime.datetime.now(datetime.timezone.utc)
             req_time = data['requested_at']
             if req_time.tzinfo is None:
                 req_time = req_time.replace(tzinfo=datetime.timezone.utc)
             
             if (now_utc - req_time).total_seconds() > (20 * 60):
                 is_force_expired = True

        if is_force_expired or db_st == 'EXPIRED':
             data['status'] = 'EXPIRED'
        elif remaining <= 0:
            data['status'] = 'FULFILLED'
        elif data['total_fulfilled'] > 0:
            data['status'] = 'PARTIAL'
        else:
            data['status'] = 'OPEN'

        data['created_at'] = data['requested_at']
        my_requests_all.append(data)

    my_requests_all.sort(
        key=lambda x: x['requested_at'] or datetime.datetime.min,
        reverse=True
    )

    # 9. Fulfilled List (For Banks to see what they did)
    fulfilled = []
    if current_user.role == 'BloodBank':
        fulfilled = db.execute("""
            SELECT t.transaction_id, t.request_id,
                   t.requester_entity_id AS hospital,
                   t.blood_type, t.component, t.units_fulfilled,
                   t.level, t.completed_at AS fulfilled_at
            FROM ops.transaction_logs t
            WHERE t.fulfilled_by_entity_id = %s
            ORDER BY t.completed_at DESC NULLS LAST;
        """, (current_user.org_id,)).fetchall()

    return render_template(
        'requests.html',
        org=current_user.org_id,
        role=current_user.role,
        my_requests=my_requests_all,
        requests=my_requests_all,
        all_hospital_requests=visible_requests if current_user.role == 'BloodBank' else [],
        incoming_peer_requests=visible_requests if current_user.role == 'Hospital' else [],
        fulfilled=fulfilled,
        filters=filters
    )

# DONOR: ACCEPT  (emergencies = 1-unit, blood drives = RSVP)
from flask import jsonify  # kept in case it's used elsewhere

@app.route('/donor/accept/<string:request_id>', methods=['POST'])
@login_required
@role_required('Donor')
def donor_accept_request(request_id):
    from datetime import datetime, timezone
    db = get_db()

    try:
        # 1. Aggregate current state for this request
        summary = db.execute("""
            SELECT 
                MAX(units_requested)              AS total_requested,
                SUM(COALESCE(units_fulfilled, 0)) AS total_fulfilled,
                MAX(blood_type)                   AS blood_type,
                MAX(component)                    AS component,
                MAX(requester_entity_id)          AS hospital_id,
                MIN(requested_at)                 AS requested_at,
                MAX(level)                        AS level
            FROM ops.transaction_logs
            WHERE request_id = %s
        """, (request_id,)).fetchone()

        # No such request at all
        if not summary:
            return redirect(url_for('dashboard'))

        level = int(summary['level'] or 0)

        # CASE A: BLOOD DRIVE (level = 2) → simple RSVP by donor
        if level == 2:
            # Has this donor already taken any action on this request?
            already = db.execute("""
                SELECT 1
                  FROM ops.transaction_logs
                 WHERE request_id = %s
                   AND fulfilled_by_entity_type = 'Donor'
                   AND fulfilled_by_entity_id   = %s
                   AND status IN ('FULFILLED','REJECTED','PARTIAL')
                 LIMIT 1
            """, (request_id, current_user.donor_id)).fetchone()

            if already:
                return redirect(url_for('dashboard'))

            tx_id = gen_transaction_id(db, summary['hospital_id'])

            db.execute("""
                INSERT INTO ops.transaction_logs (
                    transaction_id,
                    request_id,
                    requester_entity_type, requester_entity_id,
                    blood_type, component, level,
                    units_requested, units_fulfilled,
                    status,
                    requested_at, completed_at,
                    fulfilled_by_entity_type, fulfilled_by_entity_id,
                    notes, inventory_updated,
                    request_to
                )
                VALUES (
                    %s, %s,
                    'Hospital', %s,
                    %s, %s, %s,
                    NULL, 0,
                    'FULFILLED',
                    %s, NOW(),
                    'Donor', %s,
                    'Donor RSVP for blood drive', NULL,
                    'Donor'
                );
            """, (
                tx_id, request_id,
                summary['hospital_id'],
                summary['blood_type'], summary['component'], level,
                summary['requested_at'],
                current_user.donor_id
            ))

            db.commit()
            return redirect(url_for('dashboard'))

        # CASE B: EMERGENCY (level 1 or others with units_requested)
        if not summary['total_requested']:
            # For emergencies we expect units_requested; if missing, bail safely.
            return redirect(url_for('dashboard'))

        total_requested = int(summary['total_requested'])
        total_fulfilled = int(summary['total_fulfilled'] or 0)
        remaining = total_requested - total_fulfilled

        if remaining <= 0:
            # Already fully fulfilled
            return redirect(url_for('dashboard'))

            # Check if request has expired (> 10 minutes), can be changeable but for demo purposes, it kept as 10 minutes.
        if summary['requested_at']:
            req_at = summary['requested_at']
            if req_at.tzinfo is None:
                now = datetime.now()
            else:
                now = datetime.now(timezone.utc)
            elapsed = (now - req_at).total_seconds() / 60.0
            if elapsed > 20:
                return redirect(url_for('dashboard'))

        # Donor gives exactly 1 unit by default
        give = 1
        new_total = total_fulfilled + give
        status = 'FULFILLED' if new_total >= total_requested else 'PARTIAL'
        if elapsed > 20:
            status = 'EXPIRED'


        # Insert fulfillment row
        tx_id = gen_transaction_id(db, summary['hospital_id'])

        db.execute("""
            INSERT INTO ops.transaction_logs (
                transaction_id,
                request_id,
                requester_entity_type, requester_entity_id,
                blood_type, component, level,
                units_requested, units_fulfilled,
                status,
                requested_at, completed_at,
                fulfilled_by_entity_type, fulfilled_by_entity_id,
                notes, inventory_updated,
                request_to
            )
            VALUES (
                %s, %s,
                'Hospital', %s,
                %s, %s, %s,
                NULL, %s,
                %s,
                %s, NOW(),
                'Donor', %s,
                'Donor fulfillment', NULL,
                'Donor'
            );
        """, (
            tx_id, request_id,
            summary['hospital_id'],
            summary['blood_type'], summary['component'], level,
            give,
            status,
            summary['requested_at'],
            current_user.donor_id
        ))

        db.commit()
        return redirect(url_for('dashboard'))

    except Exception:
        app.logger.exception("donor_accept_request failed")
        db.rollback()
        return redirect(url_for('dashboard'))


# DONOR: REJECT  (per-donor event; others still see the OPEN request)
@app.route('/donor/reject/<string:request_id>', methods=['POST'])
@login_required
@role_required('Donor')
def donor_reject_request(request_id):
    db = get_db()
    try:
        # Has THIS donor already made a decision on this request?
        already = db.execute("""
            SELECT 1
              FROM ops.transaction_logs
             WHERE request_id = %s
               AND fulfilled_by_entity_type = 'Donor'
               AND fulfilled_by_entity_id   = %s
               AND status IN ('REJECTED','FULFILLED','PARTIAL')
             LIMIT 1;
        """, (request_id, current_user.donor_id)).fetchone()

        if already:
            return redirect(url_for('dashboard'))

        # Aggregate base info for this request (works for emergencies + blood drives)
        base = db.execute("""
            SELECT
                MIN(requested_at)        AS earliest,
                MAX(requester_entity_id) AS hospital_id,
                MAX(blood_type)          AS blood_type,
                MAX(component)           AS component,
                MAX(level)               AS level
            FROM ops.transaction_logs
            WHERE request_id = %s;
        """, (request_id,)).fetchone()

        if not base or not base["earliest"]:
            return redirect(url_for('dashboard'))

        level = int(base["level"] or 0)
        tx_id = gen_transaction_id(db, current_user.donor_id)

        # CASE A: BLOOD DRIVE (level = 2) → simple RSVP "REJECTED"
        if level == 2:
            db.execute("""
                INSERT INTO ops.transaction_logs(
                    transaction_id, request_id,
                    requester_entity_type, requester_entity_id,
                    fulfilled_by_entity_type, fulfilled_by_entity_id,
                    blood_type, component, level,
                    units_fulfilled, status,
                    requested_at, completed_at, notes
                )
                VALUES(
                    %s, %s,
                    'Hospital', %s,
                    'Donor', %s,
                    NULL, NULL, %s,
                    0, 'REJECTED',
                    %s, NOW(), 'Donor rejected blood drive'
                );
            """, (
                tx_id, request_id,
                base["hospital_id"], current_user.donor_id,
                level,
                base["earliest"]
            ))

            db.commit()
            return redirect(url_for('dashboard'))

        # CASE B: EMERGENCY / OTHER LEVELS (with blood/component)
        bt   = base["blood_type"]
        comp = base["component"]

        # Safely normalize only if present
        bt_norm   = _norm_bt(bt) if bt else None
        comp_norm = norm_component_for_db(comp) if comp else None

        db.execute("""
            INSERT INTO ops.transaction_logs(
                transaction_id, request_id,
                requester_entity_type, requester_entity_id,
                fulfilled_by_entity_type, fulfilled_by_entity_id,
                blood_type, component, level,
                units_fulfilled, status,
                requested_at, completed_at, notes
            )
            VALUES(
                %s, %s,
                'Hospital', %s,
                'Donor', %s,
                %s, %s, %s,
                0, 'REJECTED',
                %s, NOW(), 'Donor rejected emergency request'
            );
        """, (
            tx_id, request_id,
            base["hospital_id"], current_user.donor_id,
            bt_norm, comp_norm, level,
            base["earliest"]
        ))

        db.commit()
        return redirect(url_for('dashboard'))

    except Exception:
        db.rollback()
        app.logger.exception("donor_reject_request failed")
        return redirect(url_for('dashboard'))

from flask import flash
@app.route('/blooddrive/new', methods=['GET', 'POST'])
@login_required
def new_blood_drive():
    """
    Create a Blood Drive announcement (Hospital or BloodBank).
    Writes ONE OPEN row into ops.transaction_logs with:
      - requester_entity_type = current user's role ("Hospital" | "BloodBank")
      - requester_entity_id   = current user's org_id
      - level                 = 2 (blood drive)
      - request_to            = 'Donor'
      - requested_at          = <chosen date> at midnight (server tz)
      - notes                 = location text
    All other request fields remain NULL.
    """
    role = (current_user.role or "").strip()
    if role not in ("Hospital", "BloodBank"):
        return redirect(url_for('dashboard'))

    db = get_db()

    if request.method == 'POST':
        drive_date_raw = (request.form.get('drive_date') or '').strip()   
        location_raw   = (request.form.get('location') or '').strip()
        if not drive_date_raw:
            return redirect(url_for('new_blood_drive'))
        # "Hospital" or "BloodBank" shoudl go in the role next.
        requester_type = role                                  
        requester_id   = current_user.org_id
        request_to_val = 'Donor'
        prefix         = "hops-" if role == "Hospital" else "bank-"
        request_id     = next_request_id(db, prefix)
        tx_id_open     = gen_transaction_id(db, requester_id)

        try:
            # 1) Insert the blood drive request row
            db.execute("""
                INSERT INTO ops.transaction_logs (
                    transaction_id,
                    request_id,
                    requester_entity_type, requester_entity_id,
                    blood_type, component, level,
                    units_requested, units_fulfilled,
                    status,
                    requested_at, completed_at,
                    fulfilled_by_entity_type, fulfilled_by_entity_id,
                    notes, inventory_updated,
                    request_to
                )
                VALUES (
                    %s,                     -- transaction_id
                    %s,                     -- request_id
                    %s, %s,                 -- requester_entity_type, requester_entity_id
                    NULL, NULL, 2,          -- blood_type, component, level=2
                    1000, 0,                -- units_requested (dummy target), units_fulfilled0
                    'OPEN',                 -- status
                    (%s::date)::timestamptz,-- requested_at (midnight)
                    NULL,                   -- completed_at
                    NULL, NULL,             -- fulfilled_by_entity_type, fulfilled_by_entity_id
                    %s,                     -- notes (location)
                    NULL,                   -- inventory_updated
                    %s                      -- request_to
                );
            """, (
                tx_id_open,
                request_id,
                requester_type, requester_id,
                drive_date_raw,
                location_raw,
                request_to_val
            ))

            # 2) Use the requester's organization location as the origin
            org_loc = db.execute("""
                SELECT latitude, longitude
                  FROM core.organizations
                 WHERE org_id = %s;
            """, (requester_id,)).fetchone()

            donors_in_radius = []
            # For /blooddrive/new we use a fixed radius of 25 miles(by default, can be changed in the future)
            drive_radius = 25

            if org_loc and org_loc["latitude"] is not None and org_loc["longitude"] is not None:
                org_lat = org_loc["latitude"]
                org_lon = org_loc["longitude"]

                # 3) Fetch all donors eligible for blood drives (level 2 or 3)
                donor_rows = db.execute("""
                    SELECT donor_id,
                           firstname,
                           lastname,
                           email,
                           preferred_contact,
                           latitude,
                           longitude
                      FROM core.donors
                     WHERE level IN (2, 3);
                """).fetchall()

                # 4) Filter donors by distance from the organization
                for d in donor_rows:
                    d_lat = d["latitude"]
                    d_lon = d["longitude"]
                    if d_lat is None or d_lon is None:
                        continue

                    dist = haversine_distance(org_lat, org_lon, d_lat, d_lon)
                    if dist <= drive_radius:
                        donors_in_radius.append(d)

            # 5) Log + send notifications for all eligible donors
            if donors_in_radius:
                notify_donors_for_request(db, request_id, donors_in_radius)

            # 6) Commit everything
            db.commit()
            flash("Blood drive request submitted successfully!", "success")
            return redirect(url_for('view_requests'))
        except Exception:
            db.rollback()
            return redirect(url_for('new_blood_drive'))

    return render_template('blood_drive_form.html', org=current_user.org_id)


# BANK actions (partial-aware with timestamp rules)
@app.route('/requests/accept/<string:request_id>', methods=['POST'])
@login_required
def accept_request(request_id):
    # Allow both BloodBank and Hospital (Peer) to accept
    if current_user.role not in ['BloodBank', 'Hospital']:
        return redirect(url_for('dashboard'))

    db = get_db()

    # 1. Calculate current state of the request
    summary = db.execute("""
        SELECT 
            MAX(units_requested) as total_requested,
            SUM(COALESCE(units_fulfilled, 0)) as total_fulfilled,
            MAX(blood_type) as blood_type,
            MAX(component) as component,
            MAX(requester_entity_id) as hospital_id,
            MIN(requested_at) as requested_at,
            MAX(level) as level
        FROM ops.transaction_logs
        WHERE request_id = %s
    """, (request_id,)).fetchone()

    if not summary or not summary['total_requested']:
        return redirect(url_for('view_requests'))

    total_requested = int(summary['total_requested'])
    total_fulfilled = int(summary['total_fulfilled'] or 0)
    remaining = total_requested - total_fulfilled

    if remaining <= 0:
        return redirect(url_for('view_requests'))

    # 2. Determine how much to give
    fulfiller_id = current_user.org_id
    fulfiller_type = current_user.role 
    
    bt   = _norm_bt(summary['blood_type'])
    comp = norm_component_for_db(summary['component'])
    
    # Input Validation means if they are matching to the requested entity inventory or not, if not, then there's no point in sending
    try:
        val_str = request.form.get("units")
        # If input is empty, default to giving ALL remaining
        if not val_str:
            requested_fill = remaining
        else:
            requested_fill = int(val_str)
    except ValueError:
        requested_fill = remaining

    # Fulfiller capacity check
    available = get_bank_stock(db, fulfiller_id, bt, comp)
    if available <= 0:
        flash("Not enough stock to fulfill this request.", "error")
        return redirect(url_for('view_requests'))

    # Logic: Give min of (What they need, What I offered, What I have)
    give = max(1, min(remaining, requested_fill, available))

    # 3. Determine Status
    new_total = total_fulfilled + give
    status = 'FULFILLED' if new_total >= total_requested else 'PARTIAL'

    try:
        # 4. Reduce Fulfiller Inventory
        upsert_inventory(db, fulfiller_id, bt, comp, -give)

        # 5. Insert Fulfillment Log
        tx_id = gen_transaction_id(db, summary['hospital_id'])
        
        db.execute("""
            INSERT INTO ops.transaction_logs (
                transaction_id,
                request_id,
                requester_entity_type, requester_entity_id,
                blood_type, component, level,
                units_requested, units_fulfilled,
                status,
                requested_at, completed_at,
                fulfilled_by_entity_type, fulfilled_by_entity_id,
                notes, inventory_updated,
                request_to
            )
            VALUES (
                %s, %s,
                'Hospital', %s,
                %s, %s, %s,
                NULL, %s,     -- FIX: units_requested is NULL, units_fulfilled is 'give'
                %s,
                %s, NOW(),
                %s, %s,
                %s, NULL,
                %s
            );
        """, (
            tx_id, request_id,
            summary['hospital_id'],
            bt, comp, summary['level'],
            give,             
            status,           
            summary['requested_at'],
            fulfiller_type, fulfiller_id,
            f'{fulfiller_type} fulfillment',
            fulfiller_type
        ))

        db.commit()
        flash(f"Successfully accepted {give} units. Status: {status}", "success")
    except Exception:
        db.rollback()
        app.logger.exception("accept_request failed")
        flash("Failed to accept request. Please try again.", "error")

    return redirect(url_for('view_requests'))


@app.route('/requests/reject/<string:request_id>', methods=['POST'])
@login_required
@role_required('BloodBank', 'Hospital')  
def reject_request(request_id):
    db = get_db()

    # Find the OLDEST OPEN row to preserve details
    open_row = db.execute("""
        SELECT transaction_id,
            requester_entity_id AS hospital_id,
            blood_type, component, level,
            requested_at,
            request_to
        FROM ops.transaction_logs
        WHERE request_id = %s
        AND status = 'OPEN'
        ORDER BY requested_at ASC, transaction_id ASC
        LIMIT 1;
    """, (request_id,)).fetchone()

    if not open_row:
        return redirect(url_for('view_requests'))

    # We allow rejection regardless of who it was originally sent to, as long as the user has visibility (handled by view_requests).
    org_id = current_user.org_id
    org_role = current_user.role
    note = (request.form.get("note") or "").strip()

    try:
        # Check if WE already rejected/fulfilled this specific request
        already = db.execute("""
            SELECT 1
              FROM ops.transaction_logs
             WHERE request_id = %s
               AND fulfilled_by_entity_type = %s
               AND fulfilled_by_entity_id   = %s
               AND status IN ('REJECTED','FULFILLED','PARTIAL')
             LIMIT 1;
        """, (request_id, org_role, org_id)).fetchone()

        if not already:
            # Insert REJECTED log for this entity (Bank or Hospital), This ensures it hits the "Blocklist" in view_requests.
            tx_id = gen_transaction_id(db, org_id)
            
            db.execute("""
                INSERT INTO ops.transaction_logs
                    (transaction_id,
                     request_id,
                     requester_entity_type, requester_entity_id,
                     fulfilled_by_entity_type, fulfilled_by_entity_id,
                     blood_type, component, level,
                     units_fulfilled, status,
                     requested_at, completed_at, notes)
                VALUES
                    (%s, %s,
                     'Hospital', %s,
                     %s, %s,
                     %s, %s, %s,
                     0, 'REJECTED',
                     %s, NOW(), %s);
            """, (tx_id, request_id,
                  open_row["hospital_id"], 
                  org_role, org_id,  # Dynamic Role/ID
                  _norm_bt(open_row["blood_type"]),
                  norm_component_for_db(open_row["component"]),
                  open_row["level"],
                  open_row["requested_at"],
                  note))

        # Only run "Global Close" if it is a BLOOD BANK rejecting. If a Hospital rejects, it just disappears from THEIR list (via view_requests blocklist),
        # but remains open for other hospitals/banks.
        if org_role == 'BloodBank':
            # Have all banks rejected?
            total_banks = db.execute("""
                SELECT COUNT(*) AS c FROM core.organizations WHERE org_type = 'BloodBank'
            """).fetchone()["c"]

            rejected_banks = db.execute("""
                SELECT COUNT(DISTINCT fulfilled_by_entity_id) AS c
                  FROM ops.transaction_logs
                 WHERE request_id = %s
                   AND status = 'REJECTED'
                   AND fulfilled_by_entity_type = 'BloodBank';
            """, (request_id,)).fetchone()["c"]

            # If every bank has rejected, globally close the OPEN row.
            if int(rejected_banks) >= int(total_banks):
                db.execute("""
                    UPDATE ops.transaction_logs
                       SET status       = 'REJECTED',
                           completed_at = NOW()
                     WHERE transaction_id = %s
                       AND status = 'OPEN';
                """, (open_row["transaction_id"],))

        db.commit()
    except Exception:
        db.rollback()
        app.logger.exception("reject_request failed")

    return redirect(url_for('view_requests'))

# App entry
def first_run_bootstrap():
    try:
        with get_db() as conn:
            conn.execute("SELECT 1;")
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")

@app.route('/track/<string:request_id>')
@login_required
def track_request(request_id):
    return render_template('request_tracking.html', request_id=request_id)

@app.route('/live-map')
@login_required
def live_map():
    db = get_db()
    lat = None
    lon = None
    
    if current_user.role in ['BloodBank', 'Hospital'] and current_user.org_id:
        row = db.execute("SELECT latitude, longitude FROM core.organizations WHERE org_id=%s", (current_user.org_id,)).fetchone()
        if row:
            lat = row['latitude']
            lon = row['longitude']
    elif current_user.role == 'Donor' and current_user.donor_id:
        row = db.execute("SELECT latitude, longitude FROM core.donors WHERE donor_id=%s", (current_user.donor_id,)).fetchone()
        if row:
            lat = row['latitude']
            lon = row['longitude']
            
    return render_template('live_map.html', user_lat=lat, user_lon=lon)

@app.route('/api/map-data')
def map_data():
    db = get_db()
    
    # 1. Orgs
    orgs = db.execute("""
        SELECT org_id, org_type, name, latitude, longitude
        FROM core.organizations
    """).fetchall()
    
    # 2. Donors
    donors = db.execute("""
        SELECT donor_id, bloodtype, latitude, longitude
        FROM core.donors
    """).fetchall()
    
    # 3. Requests (calculate level)
    requests = db.execute("""
        SELECT t.request_id,
               t.requester_entity_id,
               t.blood_type,
               t.component,
               t.units_requested,
               t.status,
               t.requested_at,
               EXTRACT(EPOCH FROM (NOW() - t.requested_at))/60 AS elapsed_minutes,
               o.latitude, o.longitude,
               o.name as requester_name
        FROM ops.transaction_logs t
        JOIN core.organizations o ON o.org_id = t.requester_entity_id
        WHERE t.status = 'OPEN'
    """).fetchall()
    
    req_list = []
    for r in requests:
        elapsed = float(r['elapsed_minutes'] or 0)
        if elapsed < 3:
            lvl = 1
        elif elapsed < 6:
            lvl = 2
        else:
            lvl = 3
            
        req_list.append({
            "id": r['request_id'],
            "latitude": r['latitude'],
            "longitude": r['longitude'],
            "blood_type": r['blood_type'],
            "component": r['component'],
            "units_requested": r['units_requested'],
            "requester_name": r['requester_name'],
            "level": lvl,
            "elapsed": elapsed
        })

    return jsonify({
        "orgs": [dict(o) for o in orgs],
        "donors": [dict(d) for d in donors],
        "requests": req_list
    })

@app.route('/api/request-matches/<string:request_id>')
def request_matches_api(request_id):
    db = get_db()
    
    # 1. Get Request Details (including radius_miles, no parsing from notes)
    req = db.execute("""
        SELECT 
            t.request_id, 
            MAX(t.blood_type)        AS blood_type, 
            MAX(t.component)         AS component, 
            MAX(t.units_requested)   AS units_requested,
            MAX(t.status)            AS status, 
            MAX(t.notes)             AS notes,
            MAX(t.radius_miles)      AS radius_miles,
            MIN(t.requested_at)      AS requested_at,
            EXTRACT(
                EPOCH FROM (NOW() - MIN(t.requested_at))
            )/60                     AS elapsed_minutes,
            MAX(o.latitude)          AS latitude, 
            MAX(o.longitude)         AS longitude,
            MAX(o.name)              AS requester_name
        FROM ops.transaction_logs t
        JOIN core.organizations o ON o.org_id = t.requester_entity_id
        WHERE t.request_id = %s
          AND t.status IN ('OPEN', 'PARTIAL')
        GROUP BY t.request_id
    """, (request_id,)).fetchone()
    
    if req:
        print(f"DEBUG MAP: ReqID={request_id}, Lat={req['latitude']}, Lon={req['longitude']}, Name={req['requester_name']}")
    else:
        print(f"DEBUG MAP: ReqID={request_id} NOT FOUND")

    if not req:
        return jsonify({"error": "Request not found"}), 404

    # Escalation level based on elapsed minutes
    elapsed = float(req['elapsed_minutes'] or 0)
    if elapsed < 3:
        level = 1
    elif elapsed < 6:
        level = 2
    else:
        level = 3

    # Radius: use stored radius_miles; default to 10 if missing
    radius = int(req['radius_miles'] or 10)

    # 10-minute Escalation Rule (Donors Only):
    # If elapsed time is between 10 and 20 minutes, increase search radius by 10 miles FOR DONORS ONLY
    donor_radius = radius
    if 10 <= elapsed < 20:
        donor_radius += 10

    # 2. Get Matches (Banks always, Hospitals if level>=2)
    # Donors are hidden in the response (only counts), but are considered as matches
    # when level >= 3. We still filter everything by radius from the request origin.
    
    req_lat = req['latitude']
    req_lon = req['longitude']
    
    banks = []
    hospitals = []

    # If elapsed <= 10, we still target Banks (Level 1) and Hospitals (Level 2).
    # If elapsed > 10, we strictly target Donors only (requested by user).
    # User Override: Fetch everything regardless of elapsed time (< 20 mins)
    if True:
        # Fetch all banks
        all_banks = db.execute("""
            SELECT org_id, name, latitude, longitude
            FROM core.organizations o
            WHERE org_type = 'BloodBank'
        """).fetchall()
        
        for b in all_banks:
            if (
                b['latitude']  is not None and 
                b['longitude'] is not None and 
                req_lat        is not None and 
                req_lon        is not None
            ):
                dist = haversine_distance(req_lat, req_lon, b['latitude'], b['longitude'])
                if dist <= radius:
                    banks.append(b)
        
        if level >= 2:
            all_hospitals = db.execute("""
                SELECT org_id, name, latitude, longitude
                FROM core.organizations
                WHERE org_type = 'Hospital'
            """).fetchall()
            for h in all_hospitals:
                if (
                    h['latitude']  is not None and 
                    h['longitude'] is not None and 
                    req_lat        is not None and 
                    req_lon        is not None
                ):
                    dist = haversine_distance(req_lat, req_lon, h['latitude'], h['longitude'])
                    if dist <= radius:
                        hospitals.append(h)
        
    donors = []
    if level >= 3 and req['blood_type'] is not None:
        # Donors matching blood type; we also pull email + preferred_contact for notifications
        
        # [MAPPING LOGIC]
        # Urgency 1 (Emergency) -> Targets Donors Level 1 & 3
        # Urgency 2 (Blood Drive) -> Targets Donors Level 2
        # Default (if null) -> Assume Urgency 1
        
        urgency_val = 1
        try:
            urgency_val = int(req.get('urgency_level') or 1)
        except:
            pass
            
        target_levels = (1, 3)
        if urgency_val == 2:
            target_levels = (2,)  # Tuple with single value
            
        # Dynamically build query
        # Note: If target_levels has 1 item, syntax is matches IN (2) which works, 
        # but Python tuple string conversion (2,) might be tricky in raw SQL if not handled carefully.
        # Safest is to use ANY or explicit syntax.
        
        all_donors = db.execute(f"""
            SELECT donor_id,
                   firstname,
                   lastname,
                   email,
                   preferred_contact,
                   latitude,
                   longitude
            FROM core.donors
            WHERE bloodtype = %s
              AND level IN {target_levels}
        """, (req['blood_type'],)).fetchall()

        for d in all_donors:
            if (
                d['latitude']  is not None and 
                d['longitude'] is not None and 
                req_lat        is not None and 
                req_lon        is not None
            ):
                dist = haversine_distance(req_lat, req_lon, d['latitude'], d['longitude'])
                if dist <= donor_radius:
                    donors.append(d)

        # Emergency escalation → DONOR EMAILS (only donors, once per request)
        if donors:
            # notify_donors_for_request is now idempotent:
            # it will find NEW tokens in `donors` (due to radius expansion) and notify them,
            # while skipping anyone who already has a row in ops.notifications.
            notify_donors_for_request(db, request_id, donors)
            db.commit()

    # 3. Activity Log
    activity = db.execute("""
        SELECT 
            t.transaction_id, 
            t.fulfilled_by_entity_type, 
            t.fulfilled_by_entity_id,
            t.units_fulfilled, 
            t.status, 
            t.completed_at,
            COALESCE(o.name, d.firstname || ' ' || d.lastname) AS fulfiller_name
        FROM ops.transaction_logs t
        LEFT JOIN core.organizations o 
            ON t.fulfilled_by_entity_type IN ('BloodBank','Hospital') 
           AND o.org_id = t.fulfilled_by_entity_id
        LEFT JOIN core.donors d 
            ON t.fulfilled_by_entity_type = 'Donor' 
           AND d.donor_id = t.fulfilled_by_entity_id
        WHERE t.request_id = %s
          AND t.status IN ('FULFILLED', 'PARTIAL')
        ORDER BY t.completed_at DESC
    """, (request_id,)).fetchall()
    
    # Calculate remaining units safely (handle NULL units_requested)
    total_fulfilled = sum(int(a['units_fulfilled'] or 0) for a in activity)
    requested_units = int(req['units_requested'] or 0)
    remaining = max(0, requested_units - total_fulfilled)
    
    # Build activity list
    activity_list = []
    for a in activity:
        activity_list.append({
            'transaction_id': a['transaction_id'],
            'fulfilled_by_entity_type': a['fulfilled_by_entity_type'],
            'fulfilled_by_entity_id': a['fulfilled_by_entity_id'],
            'units_fulfilled': a['units_fulfilled'],
            'status': a['status'],
            'completed_at': a['completed_at'].isoformat() if a['completed_at'] else None,
            'fulfiller_name': a['fulfiller_name']
        })
    
    return jsonify({
        'request': {
            'id': req['request_id'],
            'blood_type': req['blood_type'],
            'component': req['component'],
            'units_requested': req['units_requested'],
            'requester_name': req.get('requester_name', 'Unknown'),
            'latitude': req['latitude'],
            'longitude': req['longitude'],
            'requested_at': req['requested_at'].isoformat() if req['requested_at'] else None,
            'status': req['status'],
            'radius_miles': radius,
        },
        'level': level,
        'remaining_units': remaining,
        'banks': [dict(b) for b in banks],
        'hospitals': [dict(h) for h in hospitals],
        # Security: Do not expose other donors' locations on the map API for security reasons.
        'donors': [],
        'counts': {
            'banks': len(banks),
            'hospitals': len(hospitals),
            'donors': len(donors)
        },
        'activity': activity_list
    })



@app.route('/api/demo/fast-forward/<string:request_id>', methods=['POST'])
def demo_fast_forward(request_id):
    """
    DEMO ONLY: Fast-forwards time by shifting the request's start time into the past.
    This tricks the system into thinking more time has elapsed.
    """
    minutes = int(request.args.get('minutes', 1))
    db = get_db()
    
    # Check if request exists
    req = db.execute("SELECT requested_at FROM ops.transaction_logs WHERE request_id = %s", (request_id,)).fetchone()
    if not req:
        return jsonify({"error": "Request not found"}), 404
        
    # Shift time back by 'minutes'
    db.execute(f"""
        UPDATE ops.transaction_logs 
        SET requested_at = requested_at - INTERVAL '{minutes} minutes'
        WHERE request_id = %s
    """, (request_id,))
    db.commit()
    
    return jsonify({"success": True, "message": f"Fast-forwarded {minutes} minutes"})


if __name__ == "__main__":
    first_run_bootstrap()
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )

