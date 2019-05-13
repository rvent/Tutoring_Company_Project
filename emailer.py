import smtplib, ssl
import email.utils
import config

password = config.__main__()

def send_email(subject, message):
    
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = config.email
    receiver_email = config.email
    message = f"""\
    Subject: {subject}
    
    {message}"""
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, config.f.decrypt(password).decode())
        server.sendmail(sender_email, receiver_email, message)