import asyncio
from datetime import datetime
from email.message import EmailMessage
from email.utils import make_msgid
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
from gspread.utils import rowcol_to_a1

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

FOLDER_EVIDENCIAS_DRIVE_ID = "1LvSAeV6GP5mWGol6SprHeQLBgKDKIOxm"
BITACORA_SHEET_ID = "1k76V48txBYDwTaIGPcyIvZmxKMEp06J4zcltUNfvK1s"
BITACORA_HOJA = "DATA"

# 3 personas: hasta 3 envíos a la vez en total, 2 por buzón.
SMTP_GLOBAL = asyncio.Semaphore(3)
SMTP_POR_PERFIL = {
    "frank": asyncio.Semaphore(2),
    "jose": asyncio.Semaphore(2),
    "cesilia": asyncio.Semaphore(2),
}

_firmas_cache = {}
_firmas_lock = asyncio.Lock()
_creds = None
_creds_lock = asyncio.Lock()
_bitacora_queue = None
_bitacora_worker = None

SMTP_CONFIG = {
    "frank": {
        "user": "frank.chavez@paperu.pe",
        "pass": os.getenv("SMTP_PASS_FRANK", "ZXs{(+P#3&wl"),
        "host": "mail.paperu.pe",
        "port": 465,
        "use_tls": True,
        "start_tls": False,
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
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_creds():
  global _creds
  if _creds is None:
    _creds = Credentials.from_service_account_file(
        "credenciales.json", scopes=SCOPES
    )
  return _creds


async def obtener_token():
  async with _creds_lock:
    creds = get_creds()
    if not creds.valid:
      req = google.auth.transport.requests.Request()
      creds.refresh(req)
    return creds.token


def _subtipo_imagen(contenido: bytes) -> str:
  if contenido.startswith(b"\x89PNG"):
    return "png"
  if contenido.startswith(b"\xff\xd8"):
    return "jpeg"
  if contenido.startswith(b"GIF8"):
    return "gif"
  return "png"


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

  url = (
      f"https://www.googleapis.com/drive/v3/files/{file_id}"
      "?alt=media&supportsAllDrives=true"
  )
  headers = {"Authorization": f"Bearer {token}"}

  try:
    async with session.get(url, headers=headers) as resp:
      if resp.status == 200:
        contenido = await resp.read()
        return {"nombre": file_name, "contenido": contenido}
      print(f"⚠️ Error HTTP {resp.status} al descargar {file_name}")
      return None
  except Exception as e:
    print(f"⚠️ Error de red al descargar {file_name}: {e}")
    return None


async def obtener_firma(perfil, token, session):
  firma_id = FIRMAS_DRIVE_IDS.get(perfil)
  if not firma_id:
    return None

  async with _firmas_lock:
    if perfil in _firmas_cache:
      return _firmas_cache[perfil]
    firma = await descargar_archivo_drive_async(
        firma_id, "firma", token, session
    )
    if firma and firma.get("contenido"):
      _firmas_cache[perfil] = firma
    return firma


async def subir_eml_drive_async(
    eml_bytes: bytes,
    file_name: str,
    token: str,
    session: aiohttp.ClientSession,
    folder_id: str,
):
  url = (
      "https://www.googleapis.com/upload/drive/v3/files"
      "?uploadType=multipart&supportsAllDrives=true"
  )
  headers = {"Authorization": f"Bearer {token}"}

  metadata = {"name": file_name, "mimeType": "message/rfc822"}
  if folder_id:
    metadata["parents"] = [folder_id]

  form = aiohttp.FormData()
  form.add_field(
      "metadata",
      json.dumps(metadata),
      content_type="application/json; charset=UTF-8",
  )
  form.add_field(
      "file", eml_bytes, content_type="message/rfc822", filename=file_name
  )

  try:
    async with session.post(url, headers=headers, data=form) as resp:
      if resp.status == 200:
        res_json = await resp.json()
        file_id = res_json.get("id")
        return f"https://drive.google.com/file/d/{file_id}/view"
      err_text = await resp.text()
      print(f"⚠️ Error HTTP {resp.status} al subir EML: {err_text}")
      return "ERROR_AL_SUBIR_EVIDENCIA"
  except Exception as e:
    print(f"⚠️ Error de red al subir EML a Drive: {e}")
    return "ERROR_RED_EVIDENCIA"


async def procesar_destinatario(destinatario, perfil, token, session):
  async with SMTP_GLOBAL:
    async with SMTP_POR_PERFIL[perfil]:
      fecha_hora = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
      dni = destinatario.get("dni", "")
      nombre = destinatario.get("nombre", "")
      email = destinatario.get("email", "")
      archivos = destinatario.get("archivos", [])
      cant_archivos = len(archivos)
      tipo_archivos = ", ".join(
          [a.get("tipo", "") for a in archivos if a.get("tipo")]
      )

      try:
        msg = EmailMessage()
        msg["Subject"] = destinatario.get("asuntoPersonalizado", "Documentación")
        msg["From"] = SMTP_CONFIG[perfil]["user"]
        msg["To"] = email

        firma_cid = make_msgid(domain="paperu.pe")
        firma_cid_ref = firma_cid.strip("<>")
        cuerpo_html = (
            "<!DOCTYPE html>"
            '<html><head><meta charset="utf-8"></head><body>'
            f"{destinatario.get('mensajePersonalizado', '')}"
            f'<br><br><img src="cid:{firma_cid_ref}" alt="Firma">'
            "</body></html>"
        )
        msg.set_content(cuerpo_html, subtype="html", charset="utf-8", cte="8bit")

        archivos_validos = [a for a in archivos if a.get("id")]
        tareas_descarga = [
            descargar_archivo_drive_async(
                arch["id"], arch["nombre"], token, session
            )
            for arch in archivos_validos
        ]
        archivos_descargados = await asyncio.gather(*tareas_descarga)
        firma = await obtener_firma(perfil, token, session)

        if firma and firma.get("contenido"):
          msg.add_related(
              firma["contenido"],
              maintype="image",
              subtype=_subtipo_imagen(firma["contenido"]),
              cid=firma_cid,
          )
        else:
          print(f"⚠️ No se pudo incrustar la firma del perfil {perfil}")

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

        eml_bytes = msg.as_bytes()
        eml_filename = f"EVIDENCIA_{nombre}.eml"
        link_evidencia = await subir_eml_drive_async(
            eml_bytes,
            eml_filename,
            token,
            session,
            FOLDER_EVIDENCIAS_DRIVE_ID,
        )

        return [
            fecha_hora,
            dni,
            nombre,
            email,
            cant_archivos,
            tipo_archivos,
            "ENVIADO",
            link_evidencia,
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


async def _escribir_bitacora(resultados: list) -> None:
  ultimo_error = None
  for intento in range(3):
    try:
      agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)
      gc = await agcm.authorize()
      sh = await gc.open_by_key(BITACORA_SHEET_ID)
      ws = await sh.worksheet(BITACORA_HOJA)

      existentes = await ws.get("A:H")
      siguiente_fila = max(len(existentes) + 1, 2)
      n_cols = max(len(fila) for fila in resultados)
      fila_fin = siguiente_fila + len(resultados) - 1
      rango = (
          f"{rowcol_to_a1(siguiente_fila, 1)}:"
          f"{rowcol_to_a1(fila_fin, n_cols)}"
      )

      await ws.update(
          resultados,
          range_name=rango,
          value_input_option="USER_ENTERED",
      )
      print(
          f"📊 {len(resultados)} registros guardados en Google Sheets "
          f"(filas {siguiente_fila}-{fila_fin})."
      )
      return
    except Exception as e:
      ultimo_error = e
      print(f"⚠️ Intento {intento + 1}/3 al guardar bitácora: {e}")
      await asyncio.sleep(1.5 * (intento + 1))

  raise RuntimeError(f"Error actualizando Google Sheets: {ultimo_error}")


async def bitacora_worker():
  """Un solo escritor: el lote de A termina antes de empezar el de B."""
  while True:
    resultados, future = await _bitacora_queue.get()
    try:
      await _escribir_bitacora(resultados)
      if not future.done():
        future.set_result(True)
    except Exception as e:
      if not future.done():
        future.set_exception(e)
    finally:
      _bitacora_queue.task_done()


async def guardar_bitacora(resultados: list) -> None:
  if not resultados:
    return
  future = asyncio.get_running_loop().create_future()
  await _bitacora_queue.put((resultados, future))
  await future


async def flujo_principal(payload: dict):
  inicio = time.time()
  remitente = payload.get("correo_remitente", "frank.chavez@paperu.pe")
  perfil = obtener_perfil_remitente(remitente)
  destinatarios = payload.get("destinatarios", [])

  if not destinatarios:
    print("⚠️ No se encontraron destinatarios en el payload recibido.")
    return

  token = await obtener_token()
  print(f"📨 Lote de {perfil}: {len(destinatarios)} destinatario(s).")

  async with aiohttp.ClientSession() as session:
    await obtener_firma(perfil, token, session)
    tareas = [
        procesar_destinatario(dest, perfil, token, session)
        for dest in destinatarios
    ]
    resultados = await asyncio.gather(*tareas)

  try:
    await guardar_bitacora(resultados)
  except Exception as e:
    print(f"⚠️ Error actualizando Google Sheets: {e}")

  tiempo_total = time.time() - inicio
  print(
      f"🏁 Lote de {perfil} completado en: {tiempo_total:.2f} segundos"
      f" ({tiempo_total / 60:.2f} minutos)\n"
  )


@app.on_event("startup")
async def al_iniciar():
  global _bitacora_queue, _bitacora_worker
  _bitacora_queue = asyncio.Queue()
  _bitacora_worker = asyncio.create_task(bitacora_worker())
  print("🚀 Servidor listo para 3 remitentes. Bitácora en cola.")


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


if __name__ == "__main__":
  import uvicorn

  uvicorn.run(
      "correos_masivos:app",
      host="0.0.0.0",
      port=8000,
      reload=True,
  )
