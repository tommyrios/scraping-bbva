import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class MensajeSender:
    def __init__(self):
        self.email_user = os.environ.get("EMAIL_USER")
        self.email_pass = os.environ.get("EMAIL_PASS")
        # Lista de destinatarios separada por comas en las variables de entorno
        self.destinatarios = os.environ.get("EMAIL_TO", "").split(",") 

    def enviar_difusion(self, contenido_html, asunto="Reporte Regulatorio BBVA"):
        if not self.email_user or not self.email_pass:
            print("❌ Faltan credenciales de correo.")
            return

        msg = MIMEMultipart('alternative') # Importante: 'alternative' para HTML
        msg['Subject'] = asunto
        msg['From'] = self.email_user
        msg['To'] = ", ".join(self.destinatarios)

        # 1. Cuerpo en texto plano (fallback por si el cliente no lee HTML)
        texto_plano = "Por favor, habilite la visualización HTML para ver este reporte."
        part1 = MIMEText(texto_plano, 'plain')
        
        # 2. Cuerpo en HTML (El diseño bonito)
        part2 = MIMEText(contenido_html, 'html')

        msg.attach(part1)
        msg.attach(part2)

        try:
            # Configuración típica de Gmail/Outlook
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_user, self.email_pass)
            server.send_message(msg)
            server.quit()
            print(f"✅ Correo HTML enviado a {len(self.destinatarios)} destinatarios.")
        except Exception as e:
            print(f"❌ Error enviando correo: {e}")