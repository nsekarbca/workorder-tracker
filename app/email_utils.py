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


def get_base_url() -> str:
    return os.getenv("APP_BASE_URL", "").rstrip("/")


def send_email(to_email: str, subject: str, body: str) -> bool:
    """Best-effort send. Returns True on success, False if not configured or if sending failed — never raises."""
    if not to_email:
        return False
    cfg = _config()
    if not cfg:
        logger.warning("SMTP not configured — skipped email to %s: %s", to_email, subject)
        return False
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = cfg["from_addr"]
        msg["To"] = to_email
        with smtplib.SMTP(cfg["host"], cfg["port"], timeout=10) as server:
            server.starttls()
            server.login(cfg["user"], cfg["password"])
            server.sendmail(cfg["from_addr"], [to_email], msg.as_string())
        return True
    except Exception:
        logger.exception("Failed to send email to %s", to_email)
        return False


def send_new_account_email(to_email: str, full_name: str, username: str, temp_password: str) -> bool:
    subject = "Your Work Order Tracker account"
    base_url = get_base_url()
    login_line = f"Log in here: {base_url}" if base_url else "Log in at the Work Order Tracker URL your Team Lead shared."
    body = (
        f"Hi {full_name},\n\n"
        f"An account has been created for you on the Work Order Allocation Tracker.\n\n"
        f"Username: {username}\n"
        f"Temporary password: {temp_password}\n\n"
        f"{login_line}\n\n"
        f"You'll be asked to set your own password the first time you log in.\n\n"
        f"If you didn't expect this, please contact your Team Lead."
    )
    return send_email(to_email, subject, body)


def send_username_reminder_email(to_email: str, username: str) -> bool:
    subject = "Your Work Order Tracker username"
    body = (
        f"Hi,\n\n"
        f"You requested a reminder of your username for the Work Order Allocation Tracker.\n\n"
        f"Username: {username}\n\n"
        f"If you didn't request this, you can safely ignore this email."
    )
    return send_email(to_email, subject, body)


def send_password_reset_email(to_email: str, username: str, reset_token: str) -> bool:
    subject = "Reset your Work Order Tracker password"
    base_url = get_base_url()
    reset_link = f"{base_url}/?reset_token={reset_token}" if base_url else f"(open the app and use this code): {reset_token}"
    body = (
        f"Hi {username},\n\n"
        f"You requested a password reset for the Work Order Allocation Tracker.\n\n"
        f"Reset link (valid for 1 hour): {reset_link}\n\n"
        f"If you didn't request this, you can safely ignore this email — your password won't change."
    )
    return send_email(to_email, subject, body)
