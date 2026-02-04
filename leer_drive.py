import os
import os.path
import re
import warnings
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import psycopg2
from psycopg2.extras import execute_batch
from tabulate import tabulate
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.rule import Rule
from rich.text import Text
from rich.columns import Columns
from rich.align import Align
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

# Suprimir warnings de versiones de Python y dependencias
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='.*urllib3.*')
warnings.filterwarnings('ignore', message='.*OpenSSL.*')
warnings.filterwarnings('ignore', message='.*LibreSSL.*')
warnings.filterwarnings('ignore', message='.*Python version.*')

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Si modificas los permisos, elimina el archivo token.json
# SCOPES actualizados para incluir acceso a Sheets
SCOPES = [
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'  # Para leer Google Sheets
]

# Cargar variables de entorno
POSTGRES_URL = os.getenv('POSTGRES_URL')
FOLDER_ID = os.getenv('FOLDER_ID')
GOOGLE_CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')

def connect_to_postgres(postgres_url):
    """
    Conecta a PostgreSQL usando una URL de conexión
    
    Args:
        postgres_url: URL de conexión en formato postgres://user:password@host:port/database?params
    
    Returns:
        psycopg2.connection object si la conexión es exitosa, None si falla
    """
    try:
        # Parsear la URL de conexión
        parsed = urlparse(postgres_url)
        
        # Extraer parámetros de la query string
        params = parse_qs(parsed.query)
        
        # Construir diccionario de conexión
        conn_params = {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path.lstrip('/'),
            'user': parsed.username,
            'password': parsed.password,
        }
        
        # Agregar parámetros SSL si están presentes
        if 'sslmode' in params:
            conn_params['sslmode'] = params['sslmode'][0]
        
        # Intentar conexión
        conn = psycopg2.connect(**conn_params)
        
        # Verificar conexión con una query simple
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            cur.execute("SELECT current_database();")
            db_name = cur.fetchone()[0]
        
        # La información se mostrará en main() con rich
        return conn
        
    except psycopg2.Error as e:
        return None
    except Exception as e:
        return None

def get_existing_events(postgres_conn):
    """
    Obtiene los eventos existentes en la base de datos
    
    Args:
        postgres_conn: Conexión activa a PostgreSQL
    
    Returns:
        Lista de tuplas con (nombreArtistico, venue, fecha, ticketera) o None si hay error
    """
    try:
        query = """
        SELECT
            a."nombreArtistico",
            e.venue,
            TO_CHAR(e.fecha, 'DD/MM/YYYY') as fecha,
            e.ticketera
        FROM
            audiencia_eventos e
        LEFT JOIN
            audiencia_artistas a ON e.artista_id = a.id;
        """
        
        with postgres_conn.cursor() as cur:
            cur.execute(query)
            events = cur.fetchall()
        
        return events
        
    except psycopg2.Error as e:
        print(f"❌ Error ejecutando query: {e}")
        return None
    except Exception as e:
        print(f"❌ Error inesperado al obtener eventos: {e}")
        return None

def normalize_date_for_comparison(date_str):
    """
    Normaliza una fecha en formato DD/MM/YYYY a YYYY-MM-DD para comparación
    También maneja fechas en formato DDMMYYYY
    """
    try:
        # Si es formato DD/MM/YYYY
        if '/' in date_str and len(date_str) == 10:
            parts = date_str.split('/')
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        # Si es formato DDMMYYYY
        elif len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[4:8]}-{date_str[2:4]}-{date_str[0:2]}"
    except:
        pass
    return date_str.lower().strip()

def normalize_string_for_comparison(s):
    """Normaliza un string para comparación (lowercase, sin espacios extra)"""
    if not s:
        return ""
    return str(s).lower().strip()

def compare_events_with_database(db_events, sheets):
    """
    Compara eventos de los sheets con eventos de la base de datos
    
    Args:
        db_events: Lista de tuplas (nombreArtistico, venue, fecha, ticketera) de la BD
        sheets: Lista de diccionarios con info de sheets (name, id, etc)
    
    Returns:
        dict con estadísticas y sheets que no coinciden
    """
    # Crear set de eventos de BD para comparación rápida
    db_events_set = set()
    for event in db_events:
        artista = normalize_string_for_comparison(event[0] if event[0] else "")
        venue = normalize_string_for_comparison(event[1] if event[1] else "")
        fecha = normalize_date_for_comparison(event[2] if event[2] else "")
        db_events_set.add((artista, venue, fecha))
    
    # Parsear eventos de sheets
    sheets_events = []
    sheets_not_in_db = []
    sheets_in_db = []
    
    for sheet in sheets:
        parsed = parse_filename(sheet['name'])
        artista = normalize_string_for_comparison(parsed['artista'])
        venue = normalize_string_for_comparison(parsed['venue'])
        fecha = normalize_date_for_comparison(parsed['fecha'])
        
        event_key = (artista, venue, fecha)
        sheet_event = {
            'sheet_name': sheet['name'],
            'sheet_id': sheet['id'],
            'artista': parsed['artista'],
            'venue': parsed['venue'],
            'fecha': parsed['fecha'],
            'ticketera': parsed['ticketera'],
            'key': event_key
        }
        sheets_events.append(sheet_event)
        
        # Verificar si existe en BD
        if event_key not in db_events_set:
            sheets_not_in_db.append(sheet_event)
        else:
            sheets_in_db.append(sheet_event)
    
    return {
        'db_count': len(db_events),
        'sheets_count': len(sheets_events),
        'not_in_db_count': len(sheets_not_in_db),
        'in_db_count': len(sheets_in_db),
        'sheets_not_in_db': sheets_not_in_db,
        'sheets_in_db': sheets_in_db
    }

def get_all_folders(service):
    """Obtiene todas las carpetas accesibles por el usuario"""
    all_folders = []
    page_token = None
    
    while True:
        query = "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = service.files().list(
            q=query,
            pageSize=100,
            pageToken=page_token,
            fields="nextPageToken, files(id, name, owners, shared, webViewLink)"
        ).execute()
        
        items = results.get('files', [])
        all_folders.extend(items)
        
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    
    return all_folders

def get_user_email(service):
    """Obtiene el email del usuario autenticado"""
    try:
        about = service.about().get(fields='user').execute()
        return about.get('user', {}).get('emailAddress', '')
    except Exception as e:
        print(f"Error obteniendo email del usuario: {e}")
        return None

def get_sheets_in_folder(drive_service, folder_id):
    """Obtiene todos los Google Sheets dentro de una carpeta específica"""
    query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name, webViewLink)"
    ).execute()
    return results.get('files', [])

def read_sheet_data(sheets_service, spreadsheet_id, range_name='A1:Z1000'):
    """
    Lee datos de un Google Sheet
    
    Args:
        sheets_service: Servicio de Google Sheets API
        spreadsheet_id: ID del spreadsheet
        range_name: Rango a leer (ej: 'Sheet1!A1:C10' o 'A1:Z1000')
    
    Returns:
        Lista de listas con los datos de las filas
    """
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        return result.get('values', [])
    except Exception as e:
        print(f"Error leyendo sheet {spreadsheet_id}: {e}")
        return []

def read_sheet_data_large(sheets_service, spreadsheet_id, sheet_title='Hoja 1'):
    """
    Lee datos de un Google Sheet grande. Chunks de 50k para minimizar llamadas API.
    """
    all_data = []
    start_row = 1
    chunk_size = 50000  # Leer 50k filas a la vez (30k filas = 1 sola llamada)
    
    try:
        while True:
            end_row = start_row + chunk_size - 1
            range_name = f"{sheet_title}!A{start_row}:Z{end_row}"
            
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            chunk = result.get('values', [])
            if not chunk:
                break
            
            if start_row == 1:
                all_data = chunk
            else:
                all_data.extend(chunk)
            
            # Si leímos menos de chunk_size, terminamos
            if len(chunk) < chunk_size:
                break
            
            start_row = end_row + 1
        
        return all_data
    except Exception as e:
        print(f"Error leyendo sheet grande {spreadsheet_id}: {e}")
        return all_data if all_data else []

def get_sheet_info(sheets_service, spreadsheet_id):
    """Obtiene información sobre las hojas (sheets) dentro de un spreadsheet"""
    try:
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        sheets = spreadsheet.get('sheets', [])
        return [{'title': sheet['properties']['title'], 
                 'sheetId': sheet['properties']['sheetId']} 
                for sheet in sheets]
    except Exception as e:
        print(f"Error obteniendo info del sheet {spreadsheet_id}: {e}")
        return []

def parse_filename(filename):
    """
    Parsea el nombre del archivo con formato: Artista-Venue-Fecha-Ticketera
    Los guiones (-) separan los componentes
    Los guiones bajos (_) dentro de cada componente representan espacios
    
    Ejemplos:
    - Milo_J-Niceto-02112024-Ticketek -> Artista: "Milo J", Venue: "Niceto", Fecha: "02/11/2024", Ticketera: "Ticketek"
    - Duki-Movistar_Arena-25122025-AllAccess -> Artista: "Duki", Venue: "Movistar Arena", Fecha: "25/12/2025", Ticketera: "AllAccess"
    
    Returns:
        dict con keys: artista, venue, fecha, ticketera
    """
    # Remover extensión si existe
    name_without_ext = filename.rsplit('.', 1)[0]
    
    # Dividir por guiones (separadores principales)
    parts = name_without_ext.split('-')
    
    if len(parts) >= 4:
        # Los guiones bajos dentro de cada componente representan espacios
        artista = parts[0].replace('_', ' ')
        venue = parts[1].replace('_', ' ')
        fecha = parts[2]
        
        # Si hay más de 4 partes, el ticketera puede tener guiones (ej: "All-Access")
        if len(parts) > 4:
            ticketera = '-'.join(parts[3:]).replace('_', ' ')
        else:
            ticketera = parts[3].replace('_', ' ')
        
        # Formatear fecha si es posible (DDMMYYYY -> DD/MM/YYYY)
        try:
            if len(fecha) == 8 and fecha.isdigit():
                fecha_formatted = f"{fecha[0:2]}/{fecha[2:4]}/{fecha[4:8]}"
            else:
                fecha_formatted = fecha
        except:
            fecha_formatted = fecha
        
        return {
            'artista': artista,
            'venue': venue,
            'fecha': fecha_formatted,
            'fecha_raw': fecha,
            'ticketera': ticketera
        }
    else:
        # Si no coincide el formato, intentar parsear lo que tenemos
        artista = parts[0].replace('_', ' ') if len(parts) > 0 else 'N/A'
        venue = parts[1].replace('_', ' ') if len(parts) > 1 else 'N/A'
        fecha = parts[2] if len(parts) > 2 else 'N/A'
        ticketera = parts[3].replace('_', ' ') if len(parts) > 3 else 'N/A'
        
        # Formatear fecha si es posible
        try:
            if len(fecha) == 8 and fecha.isdigit():
                fecha_formatted = f"{fecha[0:2]}/{fecha[2:4]}/{fecha[4:8]}"
            else:
                fecha_formatted = fecha
        except:
            fecha_formatted = fecha
        
        return {
            'artista': artista,
            'venue': venue,
            'fecha': fecha_formatted,
            'fecha_raw': fecha,
            'ticketera': ticketera
        }

def read_all_sheets_data(sheets_service, spreadsheet_id):
    """Lee todas las hojas de un spreadsheet y retorna sus datos"""
    sheet_info = get_sheet_info(sheets_service, spreadsheet_id)
    all_data = {}
    
    for sheet in sheet_info:
        sheet_title = sheet['title']
        # Leer datos de la hoja completa (ajustar rango si es necesario)
        data = read_sheet_data(sheets_service, spreadsheet_id, f"{sheet_title}!A1:Z1000")
        all_data[sheet_title] = data
    
    return all_data

def get_or_create_artista(postgres_conn, nombre_artistico):
    """Obtiene o crea un artista en la base de datos"""
    try:
        with postgres_conn.cursor() as cur:
            # Buscar artista existente
            cur.execute(
                "SELECT id FROM audiencia_artistas WHERE \"nombreArtistico\" = %s",
                (nombre_artistico,)
            )
            result = cur.fetchone()
            
            if result:
                return result[0]
            
            # Crear nuevo artista con campos opcionales en NULL y tags como array vacío
            cur.execute(
                """INSERT INTO audiencia_artistas 
                   ("nombreArtistico", nacionalidad, foto, tags, "repOwner", genero) 
                   VALUES (%s, NULL, NULL, '{}', NULL, NULL) RETURNING id""",
                (nombre_artistico,)
            )
            artista_id = cur.fetchone()[0]
            postgres_conn.commit()
            return artista_id
    except Exception as e:
        postgres_conn.rollback()
        raise e

def create_evento(postgres_conn, artista_id, venue, fecha, ticketera):
    """Crea un nuevo evento en la base de datos"""
    try:
        with postgres_conn.cursor() as cur:
            cur.execute(
                """INSERT INTO audiencia_eventos (artista_id, venue, fecha, ticketera)
                   VALUES (%s, %s, %s, %s) RETURNING id""",
                (artista_id, venue, fecha, ticketera)
            )
            evento_id = cur.fetchone()[0]
            postgres_conn.commit()
            return evento_id
    except Exception as e:
        postgres_conn.rollback()
        raise e

def get_or_create_usuario(postgres_conn, email, dni, nombre, apellido, pais, provincia, ciudad, 
                          telefono, direccion, genero, nombre_completo=None):
    """Obtiene o crea/actualiza un usuario en la base de datos"""
    try:
        with postgres_conn.cursor() as cur:
            usuario_id = None
            
            # Buscar por email primero, luego por DNI
            if email:
                cur.execute("SELECT id FROM audiencia_usuarios WHERE email = %s", (email,))
                result = cur.fetchone()
                if result:
                    usuario_id = result[0]
            
            # Si no se encontró por email, buscar por DNI
            if not usuario_id and dni:
                cur.execute("SELECT id FROM audiencia_usuarios WHERE dni = %s", (dni,))
                result = cur.fetchone()
                if result:
                    usuario_id = result[0]
            
            if usuario_id:
                # Actualizar datos del usuario existente
                cur.execute(
                    """UPDATE audiencia_usuarios 
                       SET nombre = COALESCE(NULLIF(%s, ''), nombre),
                           apellido = COALESCE(NULLIF(%s, ''), apellido),
                           pais = COALESCE(NULLIF(%s, ''), pais),
                           provincia = COALESCE(NULLIF(%s, ''), provincia),
                           ciudad = COALESCE(NULLIF(%s, ''), ciudad),
                           telefono = COALESCE(NULLIF(%s, ''), telefono),
                           direccion = COALESCE(NULLIF(%s, ''), direccion),
                           genero = COALESCE(NULLIF(%s, ''), genero),
                           nombre_completo = COALESCE(NULLIF(%s, ''), nombre_completo),
                           email = COALESCE(NULLIF(%s, ''), email),
                           dni = COALESCE(NULLIF(%s, ''), dni),
                           updated_at = NOW()
                       WHERE id = %s""",
                    (nombre, apellido, pais, provincia, ciudad, telefono, direccion, genero, 
                     nombre_completo or (f"{nombre} {apellido}".strip() if nombre or apellido else None),
                     email, dni, usuario_id)
                )
                postgres_conn.commit()
                return usuario_id
            else:
                # Crear nuevo usuario
                nombre_completo_val = nombre_completo or (f"{nombre} {apellido}".strip() if nombre or apellido else None)
                cur.execute(
                    """INSERT INTO audiencia_usuarios 
                       (email, dni, nombre, apellido, pais, provincia, ciudad, telefono, direccion, genero, nombre_completo)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                    (email, dni, nombre, apellido, pais, provincia, ciudad, telefono, direccion, genero, nombre_completo_val)
                )
                usuario_id = cur.fetchone()[0]
                postgres_conn.commit()
                return usuario_id
    except Exception as e:
        postgres_conn.rollback()
        raise e

def parse_monto(monto_str):
    """Parsea un monto en formato $XX,XXX.XX a float"""
    if not monto_str:
        return None
    try:
        # Remover $, espacios y comas
        cleaned = monto_str.replace('$', '').replace(',', '').replace(' ', '').strip()
        return float(cleaned)
    except:
        return None

def parse_fecha_pago(fecha_str):
    """Parsea fecha en formato DD/MM/YYYY a datetime"""
    if not fecha_str:
        return None
    try:
        # Intentar formato DD/MM/YYYY
        if '/' in fecha_str:
            parts = fecha_str.split('/')
            if len(parts) == 3:
                return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    except:
        pass
    return None

def parse_hora_pago(hora_str):
    """Parsea hora en formato HH:MM:SS a.m./p.m. a número decimal (horas)"""
    if not hora_str:
        return None
    try:
        # Formato: "1:40:00 p.m." o "9:15:00 a.m."
        hora_str = hora_str.strip().lower()
        es_pm = 'p.m.' in hora_str or 'pm' in hora_str
        
        # Extraer números
        time_part = re.search(r'(\d+):(\d+):?(\d+)?', hora_str)
        if time_part:
            horas = int(time_part.group(1))
            minutos = int(time_part.group(2))
            
            if es_pm and horas != 12:
                horas += 12
            elif not es_pm and horas == 12:
                horas = 0
            
            # Convertir a decimal (horas + minutos/60)
            return horas + minutos / 60.0
    except:
        pass
    return None

def create_or_update_usuario_evento(postgres_conn, usuario_id, evento_id, cantidad_tickets, 
                                    gasto_final, promociones, forma_de_pago, tarjeta, sector,
                                    cuotas, dias_de_venta, fecha_de_pago, hora_de_pago):
    """Crea o actualiza la relación usuario-evento"""
    try:
        with postgres_conn.cursor() as cur:
            # Verificar si ya existe
            cur.execute(
                "SELECT id FROM audiencia_usuarios_eventos WHERE usuario_id = %s AND evento_id = %s",
                (usuario_id, evento_id)
            )
            result = cur.fetchone()
            
            if result:
                # Actualizar registro existente
                cur.execute(
                    """UPDATE audiencia_usuarios_eventos
                       SET cantidad_tickets = COALESCE(%s, cantidad_tickets),
                           gasto_final = COALESCE(%s, gasto_final),
                           promociones = COALESCE(%s, promociones),
                           forma_de_pago = COALESCE(%s, forma_de_pago),
                           tarjeta = COALESCE(%s, tarjeta),
                           sector = COALESCE(%s, sector),
                           cuotas = COALESCE(%s, cuotas),
                           dias_de_venta = COALESCE(%s, dias_de_venta),
                           fecha_de_pago = COALESCE(%s, fecha_de_pago),
                           hora_de_pago = COALESCE(%s, hora_de_pago),
                           updated_at = NOW()
                       WHERE id = %s""",
                    (cantidad_tickets, gasto_final, promociones, forma_de_pago, tarjeta, sector,
                     cuotas, dias_de_venta, fecha_de_pago, hora_de_pago, result[0])
                )
            else:
                # Crear nuevo registro
                cur.execute(
                    """INSERT INTO audiencia_usuarios_eventos
                       (usuario_id, evento_id, cantidad_tickets, gasto_final, promociones,
                        forma_de_pago, tarjeta, sector, cuotas, dias_de_venta, fecha_de_pago, hora_de_pago)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (usuario_id, evento_id, cantidad_tickets, gasto_final, promociones, forma_de_pago,
                     tarjeta, sector, cuotas, dias_de_venta, fecha_de_pago, hora_de_pago)
                )
            postgres_conn.commit()
    except Exception as e:
        postgres_conn.rollback()
        raise e


# ---------------------------------------------------------------------------
# Batch processing: aggregate by user, few DB round-trips, one commit per batch
# Table audiencia_usuarios_eventos must have UNIQUE(usuario_id, evento_id)
# ---------------------------------------------------------------------------
BATCH_SIZE_USERS = 5000
BATCH_SIZE_USUARIO_EVENTO = 5000
LOOKUP_CHUNK = 5000


def _batch_get_existing_users(postgres_conn, emails, dnis):
    """Returns dict: user_key (email or dni) -> usuario id. Case-insensitive on email so sheet casing matches DB."""
    lookup = {}
    with postgres_conn.cursor() as cur:
        for i in range(0, len(emails), LOOKUP_CHUNK):
            chunk = emails[i : i + LOOKUP_CHUNK]
            if chunk:
                chunk_lower = [e.lower() for e in chunk]
                cur.execute("SELECT id, email FROM audiencia_usuarios WHERE LOWER(email) = ANY(%s)", (chunk_lower,))
                for row in cur.fetchall():
                    uid, db_email = row[0], row[1]
                    lookup[db_email] = uid
                    for e in chunk:
                        if e and e.lower() == (db_email or "").lower():
                            lookup[e] = uid
        for i in range(0, len(dnis), LOOKUP_CHUNK):
            chunk = dnis[i : i + LOOKUP_CHUNK]
            if chunk:
                cur.execute("SELECT id, dni FROM audiencia_usuarios WHERE dni = ANY(%s)", (chunk,))
                for row in cur.fetchall():
                    if row[1]:
                        lookup[row[1]] = row[0]
    return lookup


def _batch_insert_users(postgres_conn, users_data):
    """Multi-row INSERT per batch. ON CONFLICT ON CONSTRAINT uq_audiencia_usuarios_email DO NOTHING so existing users are skipped; RETURNING + fallback lookup returns all ids."""
    if not users_data:
        return {}
    out = {}
    with postgres_conn.cursor() as cur:
        for i in range(0, len(users_data), BATCH_SIZE_USERS):
            batch = users_data[i : i + BATCH_SIZE_USERS]
            batch_keys = [u.get('email') or u.get('dni') for u in batch]
            values = []
            for u in batch:
                nc = u.get('nombre_completo') or (f"{u.get('nombre') or ''} {u.get('apellido') or ''}".strip() or None)
                values.append((
                    u.get('email'), u.get('dni'), u.get('nombre'), u.get('apellido'),
                    u.get('pais'), u.get('provincia'), u.get('ciudad'), u.get('telefono'),
                    u.get('direccion'), u.get('genero'), nc,
                ))
            n = len(values)
            ph = ", ".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * n)
            cur.execute(
                f"""INSERT INTO audiencia_usuarios
                    (email, dni, nombre, apellido, pais, provincia, ciudad, telefono, direccion, genero, nombre_completo)
                    VALUES {ph}
                    ON CONFLICT ON CONSTRAINT uq_audiencia_usuarios_email DO NOTHING
                    RETURNING id, email, dni""",
                [v for t in values for v in t],
            )
            for row in cur.fetchall():
                k = row[1] or row[2]
                if k:
                    out[k] = row[0]
            missing_keys = [k for k in batch_keys if k and k not in out]
            if missing_keys:
                emails_missing = [m for m in missing_keys if m and "@" in m]
                dnis_missing = [m for m in missing_keys if m and "@" not in m]
                if emails_missing:
                    cur.execute(
                        "SELECT id, email FROM audiencia_usuarios WHERE LOWER(email) = ANY(%s)",
                        ([e.lower() for e in emails_missing],),
                    )
                    for row in cur.fetchall():
                        uid, db_email = row[0], row[1]
                        for k in emails_missing:
                            if k and k.lower() == (db_email or "").lower():
                                out[k] = uid
                if dnis_missing:
                    cur.execute("SELECT id, dni FROM audiencia_usuarios WHERE dni = ANY(%s)", (dnis_missing,))
                    for row in cur.fetchall():
                        if row[1]:
                            out[row[1]] = row[0]
            postgres_conn.commit()
    return out


def _batch_upsert_usuario_eventos(postgres_conn, evento_id, records):
    """One multi-row INSERT ... ON CONFLICT per batch (minimal round-trips)."""
    if not records:
        return
    with postgres_conn.cursor() as cur:
        for i in range(0, len(records), BATCH_SIZE_USUARIO_EVENTO):
            batch = records[i : i + BATCH_SIZE_USUARIO_EVENTO]
            n = len(batch)
            ph = ", ".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * n)
            flat = []
            for r in batch:
                flat.extend([r[0], evento_id, r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10]])
            cur.execute(
                f"""INSERT INTO audiencia_usuarios_eventos
                    (usuario_id, evento_id, cantidad_tickets, gasto_final, promociones, forma_de_pago,
                     tarjeta, sector, cuotas, dias_de_venta, fecha_de_pago, hora_de_pago)
                    VALUES {ph}
                    ON CONFLICT (usuario_id, evento_id) DO UPDATE SET
                      cantidad_tickets = audiencia_usuarios_eventos.cantidad_tickets + EXCLUDED.cantidad_tickets,
                      gasto_final = COALESCE(audiencia_usuarios_eventos.gasto_final,0) + COALESCE(EXCLUDED.gasto_final,0),
                      cuotas = COALESCE(audiencia_usuarios_eventos.cuotas,0) + COALESCE(EXCLUDED.cuotas,0),
                      dias_de_venta = COALESCE(audiencia_usuarios_eventos.dias_de_venta,0) + COALESCE(EXCLUDED.dias_de_venta,0),
                      promociones = COALESCE(EXCLUDED.promociones, audiencia_usuarios_eventos.promociones),
                      forma_de_pago = COALESCE(EXCLUDED.forma_de_pago, audiencia_usuarios_eventos.forma_de_pago),
                      tarjeta = COALESCE(EXCLUDED.tarjeta, audiencia_usuarios_eventos.tarjeta),
                      sector = COALESCE(EXCLUDED.sector, audiencia_usuarios_eventos.sector),
                      fecha_de_pago = COALESCE(EXCLUDED.fecha_de_pago, audiencia_usuarios_eventos.fecha_de_pago),
                      hora_de_pago = COALESCE(EXCLUDED.hora_de_pago, audiencia_usuarios_eventos.hora_de_pago),
                      updated_at = NOW()""",
                flat,
            )
            postgres_conn.commit()


def process_sheet_data(postgres_conn, sheet_data, artista_nombre, venue, fecha, ticketera):
    """
    Procesa los datos de un sheet e inserta/actualiza en la BD
    Retorna estadísticas del procesamiento
    """
    if not sheet_data or len(sheet_data) < 2:
        return {'processed': 0, 'errors': 0, 'created_users': 0, 'updated_users': 0}
    
    # Obtener o crear artista
    artista_id = get_or_create_artista(postgres_conn, artista_nombre)
    
    # Parsear fecha del evento
    fecha_evento = parse_fecha_pago(fecha)
    if not fecha_evento:
        # Intentar formato DDMMYYYY
        try:
            if len(fecha) == 8 and fecha.isdigit():
                fecha_evento = datetime(int(fecha[4:8]), int(fecha[2:4]), int(fecha[0:2]))
        except:
            pass
    
    # Crear evento
    evento_id = create_evento(postgres_conn, artista_id, venue, fecha_evento, ticketera)
    
    # Obtener headers (primera fila)
    headers = [h.lower().strip() if h else '' for h in sheet_data[0]]
    
    # Mapear índices de columnas
    col_map = {}
    for idx, header in enumerate(headers):
        if 'email' in header:
            col_map['email'] = idx
        elif 'nombre' in header and 'apellido' in header:
            col_map['nombre_completo'] = idx
        elif 'nombre' in header:
            col_map['nombre'] = idx
        elif 'apellido' in header:
            col_map['apellido'] = idx
        elif 'pais' in header:
            col_map['pais'] = idx
        elif 'provincia' in header:
            col_map['provincia'] = idx
        elif 'localidad' in header:
            col_map['ciudad'] = idx
        elif 'direccion' in header:
            col_map['direccion'] = idx
        elif 'telefono' in header:
            col_map['telefono'] = idx
        elif 'dni' in header:
            col_map['dni'] = idx
        elif 'genero' in header:
            col_map['genero'] = idx
        elif 'cantidad' in header and 'ticket' in header:
            col_map['cantidad_tickets'] = idx
        elif 'monto' in header or 'factura' in header:
            col_map['monto'] = idx
        elif 'promocion' in header:
            col_map['promociones'] = idx
        elif 'forma' in header and 'pago' in header:
            col_map['forma_de_pago'] = idx
        elif 'tarjeta' in header:
            col_map['tarjeta'] = idx
        elif 'sector' in header:
            col_map['sector'] = idx
        elif 'cuota' in header:
            col_map['cuotas'] = idx
        elif 'fecha' in header and 'pago' in header:
            col_map['fecha_pago'] = idx
        elif 'hora' in header and 'pago' in header:
            col_map['hora_pago'] = idx
        elif 'dias' in header and 'venta' in header:
            col_map['dias_venta'] = idx

    def get_col(row, key, default=None):
        idx = col_map.get(key)
        if idx is not None and idx < len(row):
            val = row[idx]
            return val.strip() if val else default
        return default

    rows_to_process = sheet_data[1:]
    stats = {'processed': 0, 'errors': 0, 'created_users': 0, 'updated_users': 0}

    # 1) Parse + aggregate by (email or dni): same person in N rows = sum tickets/monto
    aggregated = {}
    for row in rows_to_process:
        try:
            email = get_col(row, 'email')
            dni = get_col(row, 'dni')
            if not email and not dni:
                stats['errors'] += 1
                continue
            user_key = (email or '').strip() or (dni or '').strip()
            if not user_key:
                stats['errors'] += 1
                continue

            ct = get_col(row, 'cantidad_tickets', '0')
            ct = int(ct) if ct and ct.isdigit() else None
            gasto_final = parse_monto(get_col(row, 'monto', '0'))
            cuotas_str = get_col(row, 'cuotas', '0')
            cuotas = float(cuotas_str) if cuotas_str else None
            dv_str = get_col(row, 'dias_venta', '0')
            dias_venta = float(dv_str) if dv_str else None
            fecha_pago = parse_fecha_pago(get_col(row, 'fecha_pago'))
            hora_pago = parse_hora_pago(get_col(row, 'hora_pago'))
            nombre = get_col(row, 'nombre')
            apellido = get_col(row, 'apellido')
            nombre_completo = get_col(row, 'nombre_completo')
            if not nombre_completo and (nombre or apellido):
                nombre_completo = f"{nombre or ''} {apellido or ''}".strip()

            if user_key not in aggregated:
                aggregated[user_key] = {
                    'email': email or None, 'dni': dni or None,
                    'nombre': nombre, 'apellido': apellido, 'nombre_completo': nombre_completo or None,
                    'pais': get_col(row, 'pais'), 'provincia': get_col(row, 'provincia'),
                    'ciudad': get_col(row, 'ciudad'), 'direccion': get_col(row, 'direccion'),
                    'telefono': get_col(row, 'telefono'), 'genero': get_col(row, 'genero'),
                    'cantidad_tickets': ct or 0, 'gasto_final': gasto_final or 0.0,
                    'cuotas': cuotas or 0.0, 'dias_venta': dias_venta or 0.0,
                    'promociones': get_col(row, 'promociones'), 'forma_de_pago': get_col(row, 'forma_de_pago'),
                    'tarjeta': get_col(row, 'tarjeta'), 'sector': get_col(row, 'sector'),
                    'fecha_pago': fecha_pago, 'hora_pago': hora_pago, 'row_count': 1,
                }
            else:
                agg = aggregated[user_key]
                agg['cantidad_tickets'] = (agg['cantidad_tickets'] or 0) + (ct or 0)
                agg['gasto_final'] = (agg['gasto_final'] or 0.0) + (gasto_final or 0.0)
                agg['cuotas'] = (agg['cuotas'] or 0.0) + (cuotas or 0.0)
                agg['dias_venta'] = (agg['dias_venta'] or 0.0) + (dias_venta or 0.0)
                agg['row_count'] += 1
                agg['promociones'] = get_col(row, 'promociones') or agg['promociones']
                agg['forma_de_pago'] = get_col(row, 'forma_de_pago') or agg['forma_de_pago']
                agg['tarjeta'] = get_col(row, 'tarjeta') or agg['tarjeta']
                agg['sector'] = get_col(row, 'sector') or agg['sector']
                agg['fecha_pago'] = fecha_pago or agg['fecha_pago']
                agg['hora_pago'] = hora_pago or agg['hora_pago']
        except Exception:
            stats['errors'] += 1

    if not aggregated:
        return stats

    # 2) Batch lookup existing users (chunked)
    emails = list({k for k in aggregated if k and '@' in k})
    dnis = list({k for k in aggregated if k and '@' not in k})
    existing_ids = _batch_get_existing_users(postgres_conn, emails, dnis)

    # 3) New users: multi-row INSERT per batch
    new_user_keys = [k for k in aggregated if k not in existing_ids]
    users_to_insert = []
    for k in new_user_keys:
        a = aggregated[k]
        users_to_insert.append({
            'email': a['email'], 'dni': a['dni'], 'nombre': a['nombre'], 'apellido': a['apellido'],
            'nombre_completo': a['nombre_completo'], 'pais': a['pais'], 'provincia': a['provincia'],
            'ciudad': a['ciudad'], 'telefono': a['telefono'], 'direccion': a['direccion'], 'genero': a['genero'],
        })
    new_ids = _batch_insert_users(postgres_conn, users_to_insert) if users_to_insert else {}

    # 4) Full user_id map
    user_id_map = {**existing_ids, **new_ids}
    stats['created_users'] = len(new_ids)
    stats['updated_users'] = len([k for k in aggregated if k in existing_ids])

    # 5) Batch upsert usuario_evento (one multi-row INSERT per batch)
    records = []
    for user_key, a in aggregated.items():
        uid = user_id_map.get(user_key)
        if not uid:
            continue
        records.append((
            uid, a['cantidad_tickets'], a['gasto_final'], a['promociones'], a['forma_de_pago'],
            a['tarjeta'], a['sector'], a['cuotas'], a['dias_venta'], a['fecha_pago'], a['hora_pago'],
        ))
    _batch_upsert_usuario_eventos(postgres_conn, evento_id, records)

    stats['processed'] = sum(a['row_count'] for a in aggregated.values())
    return stats

def load_processed_sheets_state():
    """Carga el estado de sheets procesados desde archivo JSON"""
    state_file = 'processed_sheets.json'
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except:
            return {'processed_sheet_ids': [], 'last_check': None}
    return {'processed_sheet_ids': [], 'last_check': None}

def save_processed_sheets_state(state):
    """Guarda el estado de sheets procesados en archivo JSON"""
    state_file = 'processed_sheets.json'
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Error guardando estado: {e}")

def main():
    console = Console()
    
    # Validar variables de entorno
    if not POSTGRES_URL:
        console.print(Panel.fit(
            "[bold red]❌ Error: POSTGRES_URL no está configurada.[/bold red]\n"
            "[yellow]Por favor, configura la variable de entorno POSTGRES_URL[/yellow]",
            style="red",
            border_style="red"
        ))
        return
    
    if not FOLDER_ID:
        console.print(Panel.fit(
            "[bold red]❌ Error: FOLDER_ID no está configurada.[/bold red]\n"
            "[yellow]Por favor, configura la variable de entorno FOLDER_ID[/yellow]",
            style="red",
            border_style="red"
        ))
        return
    
    # ============================================================================
    # PASO 1: Conectar a PostgreSQL
    # ============================================================================
    console.print(Rule("[bold blue]🔌 CONECTANDO A POSTGRESQL[/bold blue]", style="blue"))
    console.print()
    console.print(Panel.fit("Intentando conectar a la base de datos...", style="cyan"))
    console.print()
    
    postgres_conn = connect_to_postgres(POSTGRES_URL)
    
    if postgres_conn:
        # Obtener información de la conexión para mostrarla
        from urllib.parse import urlparse
        parsed = urlparse(POSTGRES_URL)
        with postgres_conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            db_name = cur.fetchone()[0]
        
        connection_info = (
            f"[bold green]✅ Conexión a PostgreSQL exitosa![/bold green]\n\n"
            f"[dim]📍 Host:[/dim] {parsed.hostname}\n"
            f"[dim]🗄️  Base de datos:[/dim] {db_name}\n"
            f"[dim]👤 Usuario:[/dim] {parsed.username}"
        )
        console.print(Panel.fit(
            connection_info,
            style="green",
            border_style="green"
        ))
        console.print()
        
        # Obtener eventos existentes
        console.print(Rule("[bold cyan]📋 EVENTOS EXISTENTES EN LA BASE DE DATOS[/bold cyan]", style="cyan"))
        console.print()
        
        events = get_existing_events(postgres_conn)
        
        if events is not None:
            if len(events) == 0:
                console.print(Panel.fit(
                    "[yellow]ℹ️  No hay eventos registrados en la base de datos.[/yellow]",
                    style="yellow",
                    border_style="yellow"
                ))
                console.print()
            else:
                console.print(f"[bold green]✅ Se encontraron [cyan]{len(events)}[/cyan] eventos:[/bold green]\n")
                
                # Crear tabla con rich
                table = Table(
                    show_header=True,
                    header_style="bold cyan",
                    box=None,
                    show_lines=False,
                    padding=(0, 1),
                    title=f"Eventos en Base de Datos ({len(events)})"
                )
                table.add_column("Artista", style="magenta", max_width=30)
                table.add_column("Venue", style="blue", max_width=30)
                table.add_column("Fecha", style="green", max_width=12)
                table.add_column("Ticketera", style="yellow", max_width=20)
                
                for event in events:
                    nombre_artistico = event[0] if event[0] else 'N/A'
                    venue = event[1] if event[1] else 'N/A'
                    fecha = event[2] if event[2] else 'N/A'
                    ticketera = event[3] if event[3] else 'N/A'
                    table.add_row(nombre_artistico, venue, fecha, ticketera)
                
                console.print(table)
                console.print()
        else:
            console.print(Panel.fit(
                "[yellow]⚠️  No se pudieron obtener los eventos.[/yellow]",
                style="yellow",
                border_style="yellow"
            ))
            console.print()
        
        # Mantener la conexión abierta para uso futuro
        # postgres_conn.close()  # Comentado para mantener la conexión activa
    else:
        console.print(Panel.fit(
            "[yellow]⚠️  No se pudo conectar a PostgreSQL. Continuando sin base de datos...[/yellow]",
            style="yellow",
            border_style="yellow"
        ))
        console.print()
        postgres_conn = None
    
    # ============================================================================
    # PASO 2: Autenticación con Google Drive/Sheets
    # ============================================================================
    creds = None
    # El archivo token.json almacena los tokens de acceso y actualización del usuario.
    # Se crea automáticamente cuando el flujo de autorización se completa por primera vez.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # Si no hay credenciales válidas, deja que el usuario inicie sesión.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Crear credentials.json desde variable de entorno si no existe
            if not os.path.exists(GOOGLE_CREDENTIALS_FILE) and GOOGLE_CREDENTIALS_JSON:
                with open(GOOGLE_CREDENTIALS_FILE, 'w') as f:
                    f.write(GOOGLE_CREDENTIALS_JSON)
            
            if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
                console.print(Panel.fit(
                    "[bold red]❌ Error: No se encontró credentials.json[/bold red]\n"
                    "[yellow]Configura GOOGLE_CREDENTIALS_FILE o GOOGLE_CREDENTIALS_JSON[/yellow]",
                    style="red",
                    border_style="red"
                ))
                return
            
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Guarda las credenciales para la próxima ejecución
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # Inicializar servicios de Drive y Sheets
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)
    
    # Validar variables de entorno
    if not POSTGRES_URL:
        console.print(Panel.fit(
            "[bold red]❌ Error: POSTGRES_URL no está configurada.[/bold red]\n"
            "[yellow]Por favor, configura la variable de entorno POSTGRES_URL[/yellow]",
            style="red",
            border_style="red"
        ))
        return
    
    if not FOLDER_ID:
        console.print(Panel.fit(
            "[bold red]❌ Error: FOLDER_ID no está configurada.[/bold red]\n"
            "[yellow]Por favor, configura la variable de entorno FOLDER_ID[/yellow]",
            style="red",
            border_style="red"
        ))
        return
    
    console.print(Rule("[bold magenta]📊 PROCESANDO SHEETS DE LA CARPETA[/bold magenta]", style="magenta"))
    console.print()
    console.print(Panel.fit("🔍 Buscando Sheets en la carpeta...", style="magenta"))
    console.print()
    
    # Obtener todos los Sheets de la carpeta
    sheets = get_sheets_in_folder(drive_service, FOLDER_ID)
    
    if not sheets:
        console.print(Panel.fit(
            "[bold red]❌ No se encontraron Sheets en esta carpeta.[/bold red]",
            style="red",
            border_style="red"
        ))
        return
    
    console.print(Panel.fit(
        f"[bold green]✅ Encontrados [cyan]{len(sheets)}[/cyan] Sheets[/bold green]",
        style="green",
        border_style="green"
    ))
    console.print()
    
    # Cargar estado de sheets procesados
    state = load_processed_sheets_state()
    processed_sheet_ids = set(state.get('processed_sheet_ids', []))
    
    # Sheets que nunca se procesaron (solo informativo)
    new_sheets = [sheet for sheet in sheets if sheet['id'] not in processed_sheet_ids]
    
    console.print(Panel.fit(
        f"[bold cyan]📋 Sheets en carpeta: [yellow]{len(sheets)}[/yellow][/bold cyan]\n"
        f"[dim]Nunca procesados: {len(new_sheets)} | Ya procesados (histórico): {len(processed_sheet_ids)}[/dim]",
        style="cyan",
        border_style="cyan"
    ))
    console.print()
    
    # ============================================================================
    # COMPARACIÓN DE EVENTOS: BD vs SHEETS
    # ============================================================================
    if postgres_conn:
        console.print(Rule("[bold yellow]🔍 COMPARACIÓN DE EVENTOS: BASE DE DATOS vs SHEETS[/bold yellow]", style="yellow"))
        console.print()
        
        # Obtener eventos de BD si no los tenemos ya
        db_events = get_existing_events(postgres_conn)
        
        if db_events is not None:
            # Comparar eventos
            comparison = compare_events_with_database(db_events, sheets)
            
            # Mostrar estadísticas en un panel
            stats_text = (
                f"[bold]📊 Eventos en Base de Datos:[/bold] [cyan]{comparison['db_count']}[/cyan]\n"
                f"[bold]📊 Eventos en Sheets:[/bold] [cyan]{comparison['sheets_count']}[/cyan]\n"
                f"[bold green]✅ Eventos en Sheets que SÍ están en BD:[/bold green] [cyan]{comparison['in_db_count']}[/cyan]\n"
                f"[bold yellow]❌ Eventos en Sheets que NO están en BD:[/bold yellow] [cyan]{comparison['not_in_db_count']}[/cyan]"
            )
            console.print(Panel.fit(stats_text, style="blue", border_style="blue"))
            console.print()
            
            # Mostrar primero los sheets que SÍ coinciden
            if comparison['sheets_in_db']:
                table_in_db = Table(
                    show_header=True,
                    header_style="bold green",
                    box=None,
                    show_lines=False,
                    padding=(0, 1),
                    title="✅ Sheets que SÍ coinciden con la Base de Datos"
                )
                table_in_db.add_column("Artista", style="cyan", max_width=25)
                table_in_db.add_column("Venue", style="magenta", max_width=25)
                table_in_db.add_column("Fecha", style="green", max_width=12)
                table_in_db.add_column("Ticketera", style="blue", max_width=20)
                table_in_db.add_column("Nombre Sheet", style="dim", max_width=40)
                
                for sheet_event in comparison['sheets_in_db']:
                    table_in_db.add_row(
                        sheet_event['artista'],
                        sheet_event['venue'],
                        sheet_event['fecha'],
                        sheet_event['ticketera'],
                        sheet_event['sheet_name']
                    )
                
                console.print(table_in_db)
                console.print()
            
            # Mostrar luego los sheets que NO coinciden
            if comparison['sheets_not_in_db']:
                table_not_in_db = Table(
                    show_header=True,
                    header_style="bold yellow",
                    box=None,
                    show_lines=False,
                    padding=(0, 1),
                    title="⚠️  Sheets que NO coinciden con la Base de Datos"
                )
                table_not_in_db.add_column("Artista", style="cyan", max_width=25)
                table_not_in_db.add_column("Venue", style="magenta", max_width=25)
                table_not_in_db.add_column("Fecha", style="green", max_width=12)
                table_not_in_db.add_column("Ticketera", style="blue", max_width=20)
                table_not_in_db.add_column("Nombre Sheet", style="dim", max_width=40)
                
                for sheet_event in comparison['sheets_not_in_db']:
                    table_not_in_db.add_row(
                        sheet_event['artista'],
                        sheet_event['venue'],
                        sheet_event['fecha'],
                        sheet_event['ticketera'],
                        sheet_event['sheet_name']
                    )
                
                console.print(table_not_in_db)
                console.print()
            else:
                console.print(Panel.fit(
                    "[bold green]✅ Todos los eventos de los Sheets están en la Base de Datos.[/bold green]",
                    style="green",
                    border_style="green"
                ))
                console.print()
        
        console.print(Rule(style="dim"))
        console.print()
        
        # Procesar TODOS los sheets que NO están en BD (aunque ya se hayan intentado antes)
        if db_events is not None and comparison['sheets_not_in_db']:
            sheets_not_in_db_ids = {sheet_event['sheet_id'] for sheet_event in comparison['sheets_not_in_db']}
            # Usar lista completa de sheets de Drive, no solo "nuevos": así se procesan los 2 (o N) que no están en BD
            sheets_to_process = [sheet for sheet in sheets if sheet['id'] in sheets_not_in_db_ids]
            
            if not sheets_to_process:
                console.print(Panel.fit(
                    "[bold green]✅ No hay sheets que procesar.[/bold green]\n"
                    "[dim]Todos los sheets están en la Base de Datos.[/dim]",
                    style="green",
                    border_style="green"
                ))
                return
        else:
            # Si no hay conexión a BD o todos están en BD, procesar solo los nuevos
            if db_events is not None:
                console.print(Panel.fit(
                    "[bold green]✅ Todos los eventos de los Sheets están en la Base de Datos.[/bold green]",
                    style="green",
                    border_style="green"
                ))
                return
            else:
                # Si no hay conexión a BD, procesar solo los nuevos
                sheets_to_process = new_sheets
    else:
        # Si no hay conexión a PostgreSQL, procesar solo los sheets nuevos
        sheets_to_process = new_sheets
    
    # Lista para trackear sheets procesados exitosamente
    successfully_processed_ids = []
    
    # Procesar solo los Sheets que NO están en BD
    for idx, sheet in enumerate(sheets_to_process, 1):
        sheet_id = sheet['id']
        sheet_name = sheet['name']
        
        # Parsear el nombre del archivo
        parsed = parse_filename(sheet_name)
        
        # Panel con información del sheet
        sheet_info = (
            f"[bold cyan]🎤 Artista:[/bold cyan]     {parsed['artista']}\n"
            f"[bold cyan]📍 Venue:[/bold cyan]       {parsed['venue']}\n"
            f"[bold cyan]📅 Fecha:[/bold cyan]       {parsed['fecha']}\n"
            f"[bold cyan]🎫 Ticketera:[/bold cyan]   {parsed['ticketera']}\n"
            f"[bold cyan]🔗 Link:[/bold cyan]        [dim]{sheet.get('webViewLink', 'N/A')}[/dim]"
        )
        
        console.print(Rule(f"[bold blue]📄 SHEET {idx}/{len(sheets_to_process)}: {sheet_name}[/bold blue]", style="blue"))
        console.print()
        console.print(Panel(sheet_info, style="blue", border_style="blue", title=f"Sheet {idx}/{len(sheets_to_process)}"))
        console.print()
        
        # Procesar datos del sheet e insertar en BD
        if not postgres_conn:
            console.print(Panel.fit(
                "[yellow]⚠️  No hay conexión a PostgreSQL. No se pueden procesar los datos.[/yellow]",
                style="yellow",
                border_style="yellow"
            ))
            console.print()
            continue
        
        console.print(Rule("[bold magenta]💾 PROCESANDO Y GUARDANDO DATOS EN BASE DE DATOS[/bold magenta]", style="magenta"))
        console.print()
        
        # Leer datos del sheet (usar la primera hoja)
        sheet_info_list = get_sheet_info(sheets_service, sheet_id)
        if not sheet_info_list:
            console.print(Panel.fit(
                "[yellow]⚠️  No se pudo obtener información del sheet.[/yellow]",
                style="yellow",
                border_style="yellow"
            ))
            console.print()
            continue
        
        # Usar la primera hoja
        sheet_title = sheet_info_list[0]['title']
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task(f"[cyan]Leyendo datos de '{sheet_title}'...", total=None)
            
            # Leer datos del sheet (manejar sheets grandes)
            sheet_data = read_sheet_data_large(sheets_service, sheet_id, sheet_title)
            
            if not sheet_data or len(sheet_data) < 2:
                progress.update(task, description="[yellow]Sheet vacío o sin datos")
                console.print()
                console.print(Panel.fit(
                    "[yellow]⚠️  Sheet vacío o sin datos para procesar.[/yellow]",
                    style="yellow",
                    border_style="yellow"
                ))
                console.print()
                continue
            
            total_rows = len(sheet_data) - 1  # Excluir header
            progress.update(task, description=f"[green]Leyendo {total_rows} filas...", total=total_rows, completed=total_rows)
            
            # Procesar datos
            progress.update(task, description="[cyan]Procesando datos e insertando en BD...")
            
            try:
                stats = process_sheet_data(
                    postgres_conn,
                    sheet_data,
                    parsed['artista'],
                    parsed['venue'],
                    parsed['fecha_raw'],  # Usar fecha raw para parsear
                    parsed['ticketera']
                )
                
                progress.update(task, completed=total_rows, description="[green]✅ Procesamiento completado")
                
                # Mostrar estadísticas
                stats_panel = (
                    f"[bold green]✅ Procesamiento completado[/bold green]\n\n"
                    f"[bold]📊 Filas procesadas:[/bold] [cyan]{stats['processed']}[/cyan] / {total_rows}\n"
                    f"[bold green]➕ Usuarios creados:[/bold green] [cyan]{stats['created_users']}[/cyan]\n"
                    f"[bold yellow]🔄 Usuarios actualizados:[/bold yellow] [cyan]{stats['updated_users']}[/cyan]\n"
                    f"[bold red]❌ Errores:[/bold red] [cyan]{stats['errors']}[/cyan]"
                )
                
                console.print()
                console.print(Panel.fit(stats_panel, style="green", border_style="green"))
                console.print()
                
                # Marcar sheet como procesado exitosamente
                successfully_processed_ids.append(sheet_id)
                
            except Exception as e:
                if postgres_conn:
                    try:
                        postgres_conn.rollback()
                    except Exception:
                        pass
                progress.update(task, description="[red]❌ Error procesando datos")
                console.print()
                console.print(Panel.fit(
                    f"[bold red]❌ Error procesando datos:[/bold red]\n[red]{str(e)}[/red]",
                    style="red",
                    border_style="red"
                ))
                console.print()
        
        console.print(Rule(style="dim"))
        console.print()
    
    # Actualizar estado de sheets procesados
    if successfully_processed_ids:
        state['processed_sheet_ids'].extend(successfully_processed_ids)
        state['last_check'] = datetime.now().isoformat()
        save_processed_sheets_state(state)
        console.print(Panel.fit(
            f"[bold green]✅ Estado actualizado: {len(successfully_processed_ids)} sheet(s) marcado(s) como procesado(s)[/bold green]",
            style="green",
            border_style="green"
        ))
        console.print()

if __name__ == '__main__':
    main()