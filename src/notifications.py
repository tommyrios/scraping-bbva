import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class MensajeSender:
    def __init__(self):
        # Intenta leer las variables del entorno
        self.email_user = os.environ.get("EMAIL_USER")
        self.email_pass = os.environ.get("EMAIL_PASSWORD")
        
        # Lee destinatarios (separados por coma si hay varios)
        destinatarios_str = os.environ.get("EMAIL_TO", "")
        self.destinatarios = [d.strip() for d in destinatarios_str.split(",") if d.strip()]

    def enviar_difusion(self, contenido_html, asunto="Reporte Regulatorio BBVA"):
        # Chequeo de seguridad: Si no hay credenciales, avisa y corta.
        if not self.email_user or not self.email_pass:
            print("❌ Faltan credenciales de correo (Revisa EMAIL_USER y EMAIL_PASS).")
            return

        if not self.destinatarios:
            print("❌ No hay destinatarios definidos (Revisa EMAIL_TO).")
            return

        # Preparamos el mensaje MULTIPART (Texto + HTML)
        msg = MIMEMultipart('alternative')
        msg['Subject'] = asunto
        msg['From'] = self.email_user
        msg['To'] = ", ".join(self.destinatarios)

        # 1. Versión Texto Plano (para relojes o clientes viejos)
        texto_plano = "Por favor, habilite la visualización HTML para ver este reporte."
        part1 = MIMEText(texto_plano, 'plain')
        
        # 2. Versión HTML (El diseño bonito que hiciste)
        part2 = MIMEText(contenido_html, 'html')

        msg.attach(part1)
        msg.attach(part2)

        try:
            # Configuración para GMAIL
            # Si usas Outlook/Office365 cambia a: smtp.office365.com
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_user, self.email_pass)
            server.send_message(msg)
            server.quit()
            print(f"✅ Correo HTML enviado exitosamente a {len(self.destinatarios)} destinatarios.")
        except Exception as e:
            print(f"❌ Error al conectar con el servidor de correo: {e}")
