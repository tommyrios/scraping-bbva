import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class MensajeSender:
    def __init__(self):
        self.email_user = os.environ.get("EMAIL_USER")
        self.email_pass = os.environ.get("EMAIL_PASSWORD")
        # Aquí recibimos la cadena "mail1, mail2, mail3"
        self.email_destinatarios_str = os.environ.get("EMAIL_DESTINATARIO")

    def formatear_mensaje_a_html(self, texto):
        if not texto: return ""
        
        # Convertir formato WhatsApp (*negrita*, saltos) a HTML
        html = texto.replace("\n", "<br>")
        partes = html.split('*')
        nuevo_texto = ""
        for i, parte in enumerate(partes):
            if i % 2 == 0:
                nuevo_texto += parte
            else:
                nuevo_texto += f"<b>{parte}</b>"
        
        return f"""
        <html>
          <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="background-color: #f4f4f4; padding: 20px; border-radius: 5px;">
                <h2 style="color: #0044cc;">🏛️ Reporte Legislativo IA</h2>
                <div style="background-color: white; padding: 15px; border-radius: 5px; border-left: 5px solid #0044cc;">
                    {nuevo_texto}
                </div>
                <p style="font-size: 12px; color: #777; margin-top: 20px;">
                    Reporte generado automáticamente por Gemini y GitHub Actions.
                </p>
            </div>
          </body>
        </html>
        """

    def enviar_difusion(self, mensaje):
        if not self.email_user or not self.email_pass or not self.email_destinatarios_str:
            print("⚠️ Error: Faltan credenciales de Email.")
            return

        print("📧 Preparando envío de correos...")

        # 1. Crear la lista de correos (separa por comas y quita espacios extra)
        lista_emails = [e.strip() for e in self.email_destinatarios_str.split(',')]

        msg = MIMEMultipart()
        msg["From"] = f"Bot Legislativo <{self.email_user}>"
        # En el encabezado "To" ponemos todos juntos para que se vea a quién se envió
        msg["To"] = self.email_destinatarios_str 
        msg["Subject"] = "📜 Reporte Diario de Proyectos (Diputados/Senado)"

        cuerpo_html = self.formatear_mensaje_a_html(mensaje)
        msg.attach(MIMEText(cuerpo_html, "html"))

        context = ssl.create_default_context()
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(self.email_user, self.email_pass)
                
                # 2. Enviar a la LISTA de destinatarios
                server.sendmail(self.email_user, lista_emails, msg.as_string())
                
            print(f"✅ Correo enviado exitosamente a {len(lista_emails)} destinatarios.")
        except Exception as e:
            print(f"❌ Error al enviar correo: {e}")
