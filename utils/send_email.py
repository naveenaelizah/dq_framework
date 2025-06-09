import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(recipient_email, subject, body, smtp_server="smtp.gmail.com",
               smtp_port=587, html_body=None):
    """
    Sends an email using the provided SMTP server details.

    Parameters:
    - sender_email (str): Sender's email address.
    - sender_password (str): Sender's email password or app-specific password.
    - recipient_email (str): Recipient's email address.
    - subject (str): Subject of the email.
    - body (str): Plain text body of the email.
    - smtp_server (str): SMTP server (default: 'smtp.gmail.com').
    - smtp_port (int): SMTP port (default: 587 for TLS).
    - html_body (str): HTML content for the email body (optional).
    """
    sender_email = "dipakkolhe4444@gmail.com"
    sender_password = "wdwl qeyd pgta latj"
    try:
        # Create the email message
        message = MIMEMultipart("alternative")
        message["From"] = sender_email
        message["To"] = recipient_email
        message["Subject"] = subject

        # Attach plain text and optional HTML content
        text_part = MIMEText(body, "plain")
        message.attach(text_part)
        if html_body:
            html_part = MIMEText(html_body, "html")
            message.attach(html_part)

        # Connect to the SMTP server
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Start TLS encryption
            server.login(sender_email, sender_password)  # Login to the SMTP server
            server.sendmail(sender_email, recipient_email, message.as_string())  # Send the email
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
