import asyncio
from datetime import datetime
from email.message import EmailMessage
import json
import os
import ssl
import time

import aiohttp
import aiosmtplib
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import google.auth.transport.requests
from google.oauth2.service_account import Credentials
import gspread_asyncio

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FIRMAS_DRIVE_IDS = {
    "frank": "1p4bwKeUsOvKSmUOprns2sfqrTCqvjS_1",
    "cesilia": "1TlhKpyyn4ngdRGLIBF9uP7vde4NcKHko",
    "jose": "14Xw-ZnJOPrRKEuY8oyl8BCdWyIXp0bVk",
}

SMTP_CONFIG = {
    "frank": {
        "user": "frank.chavez@paperu.pe",
        "pass": os.getenv("SMTP_PASS_FRANK", "ZXs{(+P#3&wl"),
        "host": "mail.paperu.pe",
        "port": 587,
        "use_tls": False,
        "start_tls": True,
    },
    "jose": {
        "user": "jose.huacles@paperu.pe",
        "pass": os.getenv("SMTP_PASS_JOSE", "GI45n.s{GDe5"),
        "host": "mail.paperu.pe",
        "port": 465,
        "use_tls": True,
        "start_tls": False,
    },
    "cesilia": {
        "user": "cesilia@paperu.pe",
        "pass": os.getenv("SMTP_PASS_CESILIA", "CONTRASEÑA_REAL_CESILIA"),
        "host": "mail.paperu.pe",
        "port": 465,
        "use_tls": True,
        "start_tls": False,
    },
}

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_creds():
  return Credentials.from_service_account_file(
      "credenciales.json", scopes=SCOPES
  )


def obtener_perfil_remitente(correo):
  correo = correo.lower() if correo else ""
  if "cesilia" in correo or "penadillo" in correo:
    return "cesilia"
  if "jose" in correo or "huacles" in correo:
    return "jose"
  return "frank"


async def descargar_archivo_drive_async(
    file_id: str, file_name: str, token: str, session: aiohttp.ClientSession
):
  if not file_id:
    return None

  url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
  headers = {"Authorization": f"Bearer {token}"}

  try:
    async with session.get(url, headers=headers) as resp:
      if resp.status == 200:
        contenido = await resp.read()
        return {"nombre": file_name, "contenido": contenido}
      else:
        print(f"⚠️ Error HTTP {resp.status} al descargar {file_name}")
        return None
  except Exception as e:
    print(f"⚠️ Error de red al descargar {file_name}: {e}")
    return None


async def procesar_destinatario(destinatario, perfil, token, session, semaphore):
  async with semaphore:
    fecha_hora = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    dni = destinatario.get("dni", "")
    nombre = destinatario.get("nombre", "")
    email = destinatario.get("email", "")
    archivos = destinatario.get("archivos", [])
    cant_archivos = len(archivos)

    # Extrae el atributo "tipo" de cada archivo (ej: CER, BOLETA, etc.)
    tipo_archivos = ", ".join(
        [a.get("tipo", "") for a in archivos if a.get("tipo")]
    )

    try:
      msg = EmailMessage()
      msg["Subject"] = destinatario.get("asuntoPersonalizado", "Documentación")
      msg["From"] = SMTP_CONFIG[perfil]["user"]
      msg["To"] = email

      firma_url = f"https://lh3.googleusercontent.com/d/{FIRMAS_DRIVE_IDS[perfil]}"
      cuerpo_html = f"{destinatario.get('mensajePersonalizado', '')}<br><br><img src='{firma_url}'>"
      msg.set_content(cuerpo_html, subtype="html")

      archivos_validos = [a for a in archivos if a.get("id")]
      tareas_descarga = [
          descargar_archivo_drive_async(
              arch["id"], arch["nombre"], token, session
          )
          for arch in archivos_validos
      ]
      archivos_descargados = await asyncio.gather(*tareas_descarga)

      for arch in archivos_descargados:
        if arch and arch.get("contenido"):
          msg.add_attachment(
              arch["contenido"],
              maintype="application",
              subtype="pdf",
              filename=arch["nombre"],
          )

      cfg = SMTP_CONFIG[perfil]
      tls_context = ssl.create_default_context()
      tls_context.check_hostname = False
      tls_context.verify_mode = ssl.CERT_NONE

      await aiosmtplib.send(
          msg,
          hostname=cfg["host"],
          port=cfg["port"],
          use_tls=cfg["use_tls"],
          start_tls=cfg["start_tls"],
          tls_context=tls_context,
          username=cfg["user"],
          password=cfg["pass"],
      )
      print(f"✅ Correo enviado con éxito a: {nombre}")

      # Retorna las 8 columnas estructuradas para Google Sheets
      return [
          fecha_hora,
          dni,
          nombre,
          email,
          cant_archivos,
          tipo_archivos,
          "ENVIADO",
          "OK",
      ]

    except Exception as e:
      print(f"❌ Error enviando a {nombre}: {e}")
      return [
          fecha_hora,
          dni,
          nombre,
          email,
          cant_archivos,
          tipo_archivos,
          "ERROR",
          str(e),
      ]


async def flujo_principal(payload: dict):
  inicio = time.time()  # Inicia cronómetro

  body = payload
  remitente = body.get("correo_remitente", "frank.chavez@paperu.pe")
  perfil = obtener_perfil_remitente(remitente)

  creds = get_creds()
  req = google.auth.transport.requests.Request()
  creds.refresh(req)
  token = creds.token

  semaphore = asyncio.Semaphore(2)  # Control de 2 envíos concurrentes
  destinatarios = body.get("destinatarios", [])

  if not destinatarios:
    print("⚠️ No se encontraron destinatarios en el payload recibido.")
    return

  async with aiohttp.ClientSession() as session:
    tareas = [
        procesar_destinatario(dest, perfil, token, session, semaphore)
        for dest in destinatarios
    ]
    resultados = await asyncio.gather(*tareas)

  try:
    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)
    gc = await agcm.authorize()
    sh = await gc.open_by_key("1sI2MH3X-uU4ptLh6qweB9irPfRAOypgUFOKV11ZgIcU")
    ws = await sh.worksheet("DATA")
    if resultados:
      await ws.append_rows(resultados)
      print("📊 Registros guardados en Google Sheets.")
  except Exception as e:
    print(f"⚠️ Error actualizando Google Sheets: {e}")

  tiempo_total = time.time() - inicio
  print(
      f"🏁 Proceso completado con éxito en: {tiempo_total:.2f} segundos"
      f" ({tiempo_total / 60:.2f} minutos)\n"
  )


@app.post("/webhook-correo")
async def recibir_webhook(request: Request, background_tasks: BackgroundTasks):
  try:
    payload = await request.json()
    background_tasks.add_task(flujo_principal, payload)
    return {
        "status": "Recibido",
        "message": "Procesando correos en segundo plano",
    }
  except Exception as e:
    print(f"❌ Error al recibir webhook: {e}")
    return {"status": "Error", "detail": str(e)}