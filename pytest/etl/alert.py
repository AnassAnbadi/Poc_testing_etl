import smtplib
from email.message import EmailMessage
import logging

logger = logging.getLogger(__name__)

def send_email_alert(subject, body, to_email, smtp_server='smtp.gmail.com', smtp_port=587,
                     from_email='', from_password=''):
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = to_email

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(from_email, from_password)
            server.send_message(msg)

        logger.info(f"📬 Email envoyé à {to_email}")
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi de l'email : {e}")
