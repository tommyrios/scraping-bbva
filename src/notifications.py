import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class MensajeSender:
    def __init__(self):
        self.email_user = os.environ.get("EMAIL_USER")
        self.email_pass = os.environ.get("EMAIL_PASSWORD")
        
        destinatarios_str = os.environ.get("EMAIL_DESTINATARIO", "")
        self.destinatarios = [d.strip() for d in destinatarios_str.split(",") if d.strip()]

    def enviar_difusion(self, contenido_html, asunto="Reporte Regulatorio BBVA"):
        if not self.email_user or not self.email_pass:
            print("❌ Faltan credenciales de correo.")
            return

        if not self.destinatarios:
            print("❌ No hay destinatarios definidos.")
            return

        msg = MIMEMultipart('alternative')
        msg['Subject'] = asunto
        msg['From'] = self.email_user
        msg['To'] = ", ".join(self.destinatarios)

        texto_plano = "Por favor, habilite la visualización HTML para ver este reporte."
        part1 = MIMEText(texto_plano, 'plain')
        
        part2 = MIMEText(contenido_html, 'html')

        msg.attach(part1)
        msg.attach(part2)

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_user, self.email_pass)
            server.send_message(msg)
            server.quit()
            print(f"✅ Correo HTML enviado exitosamente a {len(self.destinatarios)} destinatarios.")
        except Exception as e:
            print(f"❌ Error al conectar con el servidor de correo: {e}")
