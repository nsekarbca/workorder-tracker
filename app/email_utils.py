"""
Sends transactional email (new-account notices, password resets) via any
standard SMTP relay. Works with Office 365 SMTP AUTH, Gmail app passwords,
or a free transactional service like Brevo or Resend's SMTP endpoint —
whatever you can get credentials for.

Configure with these environment variables on Render:
  SMTP_HOST      e.g. smtp.office365.com / smtp-relay.brevo.com
  SMTP_PORT      usually 587
  SMTP_USER      the mailbox/account username
  SMTP_PASSWORD  the mailbox/account password or app-specific password
  SMTP_FROM      the "from" address shown to recipients
  APP_BASE_URL   your deployed app's URL, e.g. https://workorder-tracker-1.onrender.com
                 (used to build the password-reset link in the email)

If these aren't set, email sending is skipped entirely and a warning is
logged — every caller in this app is written to still work and return a
usable result (e.g. the temporary password) even when email isn't
configured, so nothing is blocked on setting this up.
"""
import os
import smtplib
import logging
from email.mime.text import MIMEText

logger = logging.getLogger("email_utils")


def _config():
    host = os.getenv("SMTP_HOST")
    port = os.getenv("SMTP_PORT")
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASSWORD")
    from_addr = os.getenv("SMTP_FROM", user)
    if not (host and port and user and password and from_addr):
        return None
    return {
        "host": host, "port": int(port), "user": user,
        "password": password, "from_addr": from_addr,
    }


def is_configured() -> bool:
    return _config() is not None


def get_base_url() -> str:
    return os.getenv("APP_BASE_URL", "").rstrip("/")


last_error = ""
last_result = {"ok": None, "at": None, "error": "", "ok_at": None}   # for the System health screen


def _note(ok: bool, err: str = ""):
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    now = _dt.now(_tz(_td(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
    last_result.update({"ok": ok, "at": now, "error": err})
    if ok:
        last_result["ok_at"] = now


def send_email(to_email: str, subject: str, body: str, html: str = None) -> bool:
    """Best-effort send. Returns True on success, False if not configured or if sending failed — never raises.
    The reason for a failure is kept in email_utils.last_error. Pass html for a formatted version (body stays as the plain-text part)."""
    global last_error
    last_error = ""
    if not to_email:
        last_error = "No email address"
        return False
    cfg = _config()
    if not cfg:
        last_error = "SMTP is not configured (set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM)"
        logger.warning("SMTP not configured — skipped email to %s: %s", to_email, subject)
        return False
    try:
        if html:
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart("alternative")
            msg.attach(MIMEText(body, "plain", "utf-8"))
            msg.attach(MIMEText(html, "html", "utf-8"))
        else:
            msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = cfg["from_addr"]
        msg["To"] = to_email
        with smtplib.SMTP(cfg["host"], cfg["port"], timeout=20) as server:
            server.starttls()
            server.login(cfg["user"], cfg["password"])
            server.sendmail(cfg["from_addr"], [to_email], msg.as_string())
        _note(True)
        return True
    except Exception as e:
        last_error = f"{type(e).__name__}: {e}"
        _note(False, last_error)
        logger.exception("Failed to send email to %s", to_email)
        return False


def send_new_account_email(to_email: str, full_name: str, username: str, temp_password: str) -> bool:
    subject = "Your WorkFlow Hub account"
    base_url = get_base_url()
    login_line = f"Log in here: {base_url}" if base_url else "Log in at the WorkFlow Hub URL your Team Lead shared."
    body = (
        f"Hi {full_name},\n\n"
        f"An account has been created for you on the WorkFlow Hub.\n\n"
        f"Username: {username}\n"
        f"Temporary password: {temp_password}\n\n"
        f"{login_line}\n\n"
        f"You'll be asked to set your own password the first time you log in.\n\n"
        f"If you didn't expect this, please contact your Team Lead."
    )
    return send_email(to_email, subject, body)


def send_username_reminder_email(to_email: str, username: str) -> bool:
    subject = "Your WorkFlow Hub username"
    body = (
        f"Hi,\n\n"
        f"You requested a reminder of your username for the WorkFlow Hub.\n\n"
        f"Username: {username}\n\n"
        f"If you didn't request this, you can safely ignore this email."
    )
    return send_email(to_email, subject, body)


def send_password_reset_email(to_email: str, username: str, reset_token: str) -> bool:
    subject = "Reset your WorkFlow Hub password"
    base_url = get_base_url()
    reset_link = f"{base_url}/?reset_token={reset_token}" if base_url else f"(open the app and use this code): {reset_token}"
    body = (
        f"Hi {username},\n\n"
        f"You requested a password reset for the WorkFlow Hub.\n\n"
        f"Reset link (valid for 1 hour): {reset_link}\n\n"
        f"If you didn't request this, you can safely ignore this email — your password won't change."
    )
    return send_email(to_email, subject, body)
