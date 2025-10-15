# smtp_test.py
import os
import smtplib
from dotenv import load_dotenv

load_dotenv()  # ensure this runs in the same folder as your .env

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_EMAIL = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

def test_smtp_login():
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        print("✖ SMTP credentials not found in environment.")
        return False

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(SMTP_EMAIL, SMTP_PASSWORD)
        print("✔ SMTP authentication successful.")
        return True
    except smtplib.SMTPAuthenticationError as e:
        print("✖ Authentication failed (SMTPAuthenticationError). Check username/password or app password.")
        return False
    except smtplib.SMTPException as e:
        print(f"✖ SMTP error: {e}")
        return False
    except Exception as e:
        print(f"✖ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    test_smtp_login()
