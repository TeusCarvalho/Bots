from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv


# =========================================================
# CARREGAR .ENV
# =========================================================
load_dotenv()

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")


# =========================================================
# CONFIG
# =========================================================
st.set_page_config(
    page_title="Gestor de Rotas",
    page_icon="🚚",
    layout="wide",
)

DB_PATH = Path("rotas.db")
STOP_COLUMNS = [
    "sequencia",
    "pedido",
    "cliente",
    "endereco",
    "cidade",
    "status_entrega",
]


# =========================================================
# DB
# =========================================================
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rotas (
            id_rota INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_rota TEXT NOT NULL,
            origem TEXT NOT NULL,
            destino TEXT NOT NULL,
            motorista TEXT,
            placa TEXT,
            status TEXT DEFAULT 'Nova',
            data_criacao TEXT,
            ultima_atualizacao TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS paradas (
            id_parada INTEGER PRIMARY KEY AUTOINCREMENT,
            id_rota INTEGER NOT NULL,
            sequencia INTEGER NOT NULL,
            pedido TEXT,
            cliente TEXT,
            endereco TEXT NOT NULL,
            cidade TEXT,
            status_entrega TEXT DEFAULT 'Pendente',
            FOREIGN KEY (id_rota) REFERENCES rotas(id_rota)
        )
        """
    )

    conn.commit()
    conn.close()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def list_routes() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT
            id_rota,
            nome_rota,
            origem,
            destino,
            COALESCE(motorista, '') AS motorista,
            COALESCE(placa, '') AS placa,
            COALESCE(status, 'Nova') AS status,
            data_criacao,
            ultima_atualizacao
        FROM rotas
        ORDER BY id_rota DESC
        """,
        conn,
    )
    conn.close()
    return df


def get_route_header(route_id: int) -> dict[str, Any] | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM rotas WHERE id_rota = ?", (route_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_route_stops(route_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT
            sequencia,
            COALESCE(pedido, '') AS pedido,
            COALESCE(cliente, '') AS cliente,
            endereco,
            COALESCE(cidade, '') AS cidade,
            COALESCE(status_entrega, 'Pendente') AS status_entrega
        FROM paradas
        WHERE id_rota = ?
        ORDER BY sequencia
        """,
        conn,
        params=(route_id,),
    )
    conn.close()

    if df.empty:
        return blank_stops_df()

    return ensure_stop_columns(df)


def insert_route(
    nome_rota: str,
    origem: str,
    destino: str,
    motorista: str,
    placa: str,
    status: str,
) -> int:
    conn = get_conn()
    cur = conn.cursor()
    agora = now_str()

    cur.execute(
        """
        INSERT INTO rotas (
            nome_rota, origem, destino, motorista, placa, status,
            data_criacao, ultima_atualizacao
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (nome_rota, origem, destino, motorista, placa, status, agora, agora),
    )

    route_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(route_id)


def replace_stops(route_id: int, stops_df: pd.DataFrame) -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM paradas WHERE id_rota = ?", (route_id,))

    for _, row in stops_df.iterrows():
        cur.execute(
            """
            INSERT INTO paradas (
                id_rota, sequencia, pedido, cliente, endereco, cidade, status_entrega
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                route_id,
                int(row["sequencia"]),
                str(row["pedido"]).strip(),
                str(row["cliente"]).strip(),
                str(row["endereco"]).strip(),
                str(row["cidade"]).strip(),
                str(row["status_entrega"]).strip(),
            ),
        )

    cur.execute(
        "UPDATE rotas SET ultima_atualizacao = ? WHERE id_rota = ?",
        (now_str(), route_id),
    )

    conn.commit()
    conn.close()


def update_route(
    route_id: int,
    nome_rota: str,
    origem: str,
    destino: str,
    motorista: str,
    placa: str,
    status: str,
    stops_df: pd.DataFrame,
) -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE rotas
        SET nome_rota = ?, origem = ?, destino = ?, motorista = ?, placa = ?,
            status = ?, ultima_atualizacao = ?
        WHERE id_rota = ?
        """,
        (nome_rota, origem, destino, motorista, placa, status, now_str(), route_id),
    )

    conn.commit()
    conn.close()

    replace_stops(route_id, stops_df)


def delete_route(route_id: int) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM paradas WHERE id_rota = ?", (route_id,))
    cur.execute("DELETE FROM rotas WHERE id_rota = ?", (route_id,))
    conn.commit()
    conn.close()


def duplicate_route(route_id: int) -> int:
    header = get_route_header(route_id)
    if not header:
        raise ValueError("Rota não encontrada.")

    stops_df = get_route_stops(route_id)

    new_id = insert_route(
        nome_rota=f"{header['nome_rota']} (cópia)",
        origem=header["origem"],
        destino=header["destino"],
        motorista=header.get("motorista", "") or "",
        placa=header.get("placa", "") or "",
        status=header.get("status", "Nova") or "Nova",
    )
    replace_stops(new_id, stops_df)
    return new_id


def route_exists_by_name(nome_rota: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM rotas WHERE nome_rota = ? LIMIT 1", (nome_rota,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def blank_stops_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sequencia": 1,
                "pedido": "",
                "cliente": "",
                "endereco": "",
                "cidade": "",
                "status_entrega": "Pendente",
            }
        ]
    )


def ensure_stop_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in STOP_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[STOP_COLUMNS].copy()
    return df


def normalize_stops_df(df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_stop_columns(df)

    for col in ["pedido", "cliente", "endereco", "cidade", "status_entrega"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["endereco"] = df["endereco"].astype(str).str.strip()
    df = df[df["endereco"] != ""].copy()

    if df.empty:
        raise ValueError("Informe pelo menos uma parada com endereço.")

    df["status_entrega"] = df["status_entrega"].replace("", "Pendente")
    df["sequencia"] = range(1, len(df) + 1)

    return df.reset_index(drop=True)


def stops_to_addresses(stops_df: pd.DataFrame) -> list[str]:
    resultado: list[str] = []
    for _, row in stops_df.iterrows():
        endereco = str(row["endereco"]).strip()
        cidade = str(row["cidade"]).strip()

        if cidade and cidade.lower() not in endereco.lower():
            resultado.append(f"{endereco}, {cidade}")
        else:
            resultado.append(endereco)

    return resultado


def build_labels(stops_df: pd.DataFrame) -> list[str]:
    labels: list[str] = []

    for _, row in stops_df.iterrows():
        pedido = str(row["pedido"]).strip()
        cliente = str(row["cliente"]).strip()
        endereco = str(row["endereco"]).strip()

        if pedido and cliente:
            labels.append(f"{pedido} • {cliente}")
        elif pedido:
            labels.append(f"Pedido {pedido}")
        elif cliente:
            labels.append(cliente)
        else:
            labels.append(endereco)

    return labels


def reorder_stops_df(stops_df: pd.DataFrame, optimized_indices: list[int]) -> pd.DataFrame:
    if not optimized_indices:
        novo = stops_df.copy().reset_index(drop=True)
        novo["sequencia"] = range(1, len(novo) + 1)
        return novo

    if len(optimized_indices) != len(stops_df):
        novo = stops_df.copy().reset_index(drop=True)
        novo["sequencia"] = range(1, len(novo) + 1)
        return novo

    novo = stops_df.iloc[optimized_indices].copy().reset_index(drop=True)
    novo["sequencia"] = range(1, len(novo) + 1)
    return novo


def route_options(df_routes: pd.DataFrame) -> list[tuple[int, str]]:
    options: list[tuple[int, str]] = []
    for _, row in df_routes.iterrows():
        label = f"{int(row['id_rota'])} — {row['nome_rota']}"
        if row["motorista"]:
            label += f" • {row['motorista']}"
        options.append((int(row["id_rota"]), label))
    return options


def get_default_route_id(df_routes: pd.DataFrame) -> int | None:
    if df_routes.empty:
        return None

    saved = st.session_state.get("selected_route_id")
    ids = df_routes["id_rota"].astype(int).tolist()

    if saved in ids:
        return int(saved)

    return int(ids[0])


def seed_demo_route_if_empty() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rotas")
    total_rotas = cur.fetchone()[0]
    conn.close()

    if total_rotas > 0:
        return

    nome_rota = "Rota Demo Goiânia"

    if route_exists_by_name(nome_rota):
        return

    rota_id = insert_route(
        nome_rota=nome_rota,
        origem="Praça Cívica, Setor Central, Goiânia, GO",
        destino="Praça Cívica, Setor Central, Goiânia, GO",
        motorista="Motorista Demo",
        placa="ABC-1234",
        status="Planejada",
    )

    demo_stops = pd.DataFrame(
        [
            {"sequencia": 1, "pedido": "PED-0001", "cliente": "Cliente 01", "endereco": "Avenida Goiás, Setor Central", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 2, "pedido": "PED-0002", "cliente": "Cliente 02", "endereco": "Rua 44, Setor Norte Ferroviário", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 3, "pedido": "PED-0003", "cliente": "Cliente 03", "endereco": "Avenida Anhanguera, Campinas", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 4, "pedido": "PED-0004", "cliente": "Cliente 04", "endereco": "Avenida T-63, Setor Bueno", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 5, "pedido": "PED-0005", "cliente": "Cliente 05", "endereco": "Avenida 85, Setor Sul", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 6, "pedido": "PED-0006", "cliente": "Cliente 06", "endereco": "Rua 90, Setor Sul", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 7, "pedido": "PED-0007", "cliente": "Cliente 07", "endereco": "Avenida Jamel Cecílio, Jardim Goiás", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 8, "pedido": "PED-0008", "cliente": "Cliente 08", "endereco": "Avenida Perimetral Norte, Setor Crimeia Oeste", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 9, "pedido": "PED-0009", "cliente": "Cliente 09", "endereco": "Avenida Mangalô, Morada do Sol", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
            {"sequencia": 10, "pedido": "PED-0010", "cliente": "Cliente 10", "endereco": "Alameda Ricardo Paranhos, Setor Marista", "cidade": "Goiânia, GO", "status_entrega": "Pendente"},
        ]
    )

    replace_stops(rota_id, demo_stops)


def parse_duration_seconds(duration_str: str) -> int:
    if not duration_str:
        return 0
    return int(float(duration_str.replace("s", "").strip()))


def decode_polyline(encoded: str) -> list[list[float]]:
    points: list[list[float]] = []
    index = 0
    lat = 0
    lng = 0

    while index < len(encoded):
        shift = 0
        result = 0

        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break

        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        shift = 0
        result = 0

        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break

        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        points.append([lat / 1e5, lng / 1e5])

    return points


def compute_optimized_route(
    api_key: str,
    origem: str,
    destino: str,
    paradas: list[str],
    routing_preference: str = "TRAFFIC_AWARE",
) -> dict[str, Any]:
    if not api_key.strip():
        raise ValueError("Informe a Google Maps API Key no arquivo .env ou na barra lateral.")

    if not origem.strip():
        raise ValueError("Informe a origem.")

    if not destino.strip():
        raise ValueError("Informe o destino.")

    if not paradas:
        raise ValueError("A rota precisa de pelo menos uma parada.")

    if len(paradas) > 25:
        raise ValueError("Esta rota tem mais de 25 paradas intermediárias.")

    url = "https://routes.googleapis.com/directions/v2:computeRoutes"

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": (
            "routes.distanceMeters,"
            "routes.duration,"
            "routes.polyline.encodedPolyline,"
            "routes.optimizedIntermediateWaypointIndex"
        ),
    }

    body = {
        "origin": {"address": origem},
        "destination": {"address": destino},
        "intermediates": [{"address": p} for p in paradas],
        "travelMode": "DRIVE",
        "routingPreference": routing_preference,
        "optimizeWaypointOrder": True,
        "polylineQuality": "HIGH_QUALITY",
    }

    resp = requests.post(url, headers=headers, json=body, timeout=60)

    if resp.status_code != 200:
        raise Exception(
            f"Erro ao consultar a Routes API.\n\n"
            f"Status: {resp.status_code}\n"
            f"Resposta: {resp.text}"
        )

    data = resp.json()

    if "routes" not in data or not data["routes"]:
        raise Exception("A API respondeu, mas não retornou uma rota válida.")

    route = data["routes"][0]
    optimized_indices = route.get("optimizedIntermediateWaypointIndex", [])
    encoded_polyline = route.get("polyline", {}).get("encodedPolyline", "")
    route_points = decode_polyline(encoded_polyline) if encoded_polyline else []

    distance_km = round(route["distanceMeters"] / 1000, 2)
    duration_seconds = parse_duration_seconds(route.get("duration", "0s"))
    duration_min = round(duration_seconds / 60, 1)

    return {
        "optimized_indices": optimized_indices,
        "distance_km": distance_km,
        "duration_seconds": duration_seconds,
        "duration_min": duration_min,
        "encoded_polyline": encoded_polyline,
        "route_points": route_points,
    }


def generate_google_maps_link(origem: str, destino: str, paradas: list[str]) -> str:
    params = {
        "api": "1",
        "origin": origem,
        "destination": destino,
        "travelmode": "driving",
    }

    if paradas:
        params["waypoints"] = "|".join(paradas)

    return "https://www.google.com/maps/dir/?" + urlencode(params, safe="|,")


def build_map_html(
    route_points: list[list[float]],
    origem: str,
    destino: str,
    stop_labels: list[str],
    stop_addresses: list[str],
    speed_ms: int,
    zoom_start: int,
) -> str:
    if not route_points:
        return "<p>Sem rota para exibir.</p>"

    route_json = json.dumps(route_points, ensure_ascii=False)
    labels_json = json.dumps(stop_labels, ensure_ascii=False)
    addresses_json = json.dumps(stop_addresses, ensure_ascii=False)
    origem_json = json.dumps(origem, ensure_ascii=False)
    destino_json = json.dumps(destino, ensure_ascii=False)

    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>
html, body {{ margin: 0; padding: 0; font-family: Arial, sans-serif; background: #fff; }}
#wrap {{ width: 100%; }}
#controls {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 10px; }}
.btn {{ border: none; border-radius: 10px; padding: 10px 16px; cursor: pointer; font-size: 14px; font-weight: 700; }}
.start {{ background: #16a34a; color: white; }}
.pause {{ background: #f59e0b; color: white; }}
.reset {{ background: #dc2626; color: white; }}
.badge {{ padding: 8px 12px; background: #f3f4f6; border-radius: 999px; font-size: 13px; font-weight: 700; color: #111827; }}
#map {{ width: 100%; height: 700px; border: 1px solid #e5e7eb; border-radius: 16px; overflow: hidden; }}
.truck-icon {{ font-size: 28px; line-height: 28px; text-align: center; }}
.bubble {{ width: 28px; height: 28px; border-radius: 999px; color: #fff; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 700; border: 2px solid white; box-shadow: 0 2px 8px rgba(0,0,0,.25); }}
.start-bubble {{ background: #16a34a; }}
.stop-bubble {{ background: #2563eb; }}
.end-bubble {{ background: #dc2626; }}
</style>
</head>
<body>
<div id="wrap">
<div id="controls">
<button class="btn start" onclick="startRoute()">Iniciar rota</button>
<button class="btn pause" onclick="pauseRoute()">Pausar</button>
<button class="btn reset" onclick="resetRoute()">Resetar</button>
<span class="badge" id="status">Status: pronto</span>
<span class="badge" id="progress">Progresso: 0%</span>
<span class="badge" id="coord">Posição: --</span>
</div>
<div id="map"></div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const route = {route_json};
const stopLabels = {labels_json};
const stopAddresses = {addresses_json};
const originAddress = {origem_json};
const destinationAddress = {destino_json};
const speedMs = {speed_ms};
const zoomStart = {zoom_start};

const map = L.map("map").setView(route[0], zoomStart);

L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap"
}}).addTo(map);

const fullLine = L.polyline(route, {{
    color: "#2563eb",
    weight: 5,
    opacity: 0.85
}}).addTo(map);

const passedLine = L.polyline([route[0]], {{
    color: "#16a34a",
    weight: 6,
    opacity: 0.95
}}).addTo(map);

map.fitBounds(fullLine.getBounds(), {{ padding: [30, 30] }});

const truckIcon = L.divIcon({{
    html: "🚚",
    className: "truck-icon",
    iconSize: [30, 30],
    iconAnchor: [15, 15]
}});

const truck = L.marker(route[0], {{ icon: truckIcon }}).addTo(map);

const startIcon = L.divIcon({{
    html: '<div class="bubble start-bubble">I</div>',
    className: "",
    iconSize: [28, 28],
    iconAnchor: [14, 14]
}});

const endIcon = L.divIcon({{
    html: '<div class="bubble end-bubble">F</div>',
    className: "",
    iconSize: [28, 28],
    iconAnchor: [14, 14]
}});

L.marker(route[0], {{ icon: startIcon }}).addTo(map).bindPopup("<b>Origem</b><br>" + originAddress);
L.marker(route[route.length - 1], {{ icon: endIcon }}).addTo(map).bindPopup("<b>Destino</b><br>" + destinationAddress);

const stopIdx = (() => {{
    if (stopLabels.length <= 0 || route.length <= 2) return [];
    const arr = [];
    for (let i = 1; i <= stopLabels.length; i++) {{
        let idx = Math.floor((i / (stopLabels.length + 1)) * (route.length - 1));
        idx = Math.max(1, Math.min(route.length - 2, idx));
        arr.push(idx);
    }}
    return arr;
}})();

stopLabels.forEach((label, i) => {{
    const point = route[stopIdx[i]] || route[Math.min(i + 1, route.length - 2)];
    const stopIcon = L.divIcon({{
        html: '<div class="bubble stop-bubble">' + (i + 1) + '</div>',
        className: "",
        iconSize: [28, 28],
        iconAnchor: [14, 14]
    }});

    L.marker(point, {{ icon: stopIcon }})
        .addTo(map)
        .bindPopup(
            "<b>Parada " + (i + 1) + "</b><br>" +
            "<b>Rótulo:</b> " + label + "<br>" +
            "<b>Endereço:</b> " + stopAddresses[i]
        );
}});

const statusEl = document.getElementById("status");
const progressEl = document.getElementById("progress");
const coordEl = document.getElementById("coord");

let currentIndex = 0;
let timer = null;
let running = false;

function updateStatus(text) {{
    statusEl.textContent = "Status: " + text;
}}

function updateProgress() {{
    const pct = Math.round((currentIndex / (route.length - 1)) * 100);
    progressEl.textContent = "Progresso: " + pct + "%";
    const point = route[Math.min(currentIndex, route.length - 1)];
    coordEl.textContent = "Posição: " + point[0].toFixed(5) + ", " + point[1].toFixed(5);
}}

function tick() {{
    if (currentIndex >= route.length) {{
        pauseRoute();
        updateStatus("finalizada");
        return;
    }}

    const point = route[currentIndex];
    truck.setLatLng(point);
    passedLine.setLatLngs(route.slice(0, currentIndex + 1));
    map.panTo(point, {{ animate: true, duration: 0.25 }});

    updateProgress();
    currentIndex += 1;
}}

function startRoute() {{
    if (running) return;
    running = true;
    updateStatus("em andamento");

    timer = setInterval(() => {{
        tick();
    }}, speedMs);
}}

function pauseRoute() {{
    if (timer) {{
        clearInterval(timer);
        timer = null;
    }}
    running = false;
    if (currentIndex < route.length) {{
        updateStatus("pausada");
    }}
}}

function resetRoute() {{
    if (timer) {{
        clearInterval(timer);
        timer = null;
    }}
    running = false;
    currentIndex = 0;
    truck.setLatLng(route[0]);
    passedLine.setLatLngs([route[0]]);
    map.fitBounds(fullLine.getBounds(), {{ padding: [30, 30] }});
    updateStatus("reiniciada");
    updateProgress();
}}

updateStatus("pronto");
updateProgress();
</script>
</body>
</html>
"""


def route_selectbox(label: str, routes_df: pd.DataFrame, key: str, default_route_id: int | None) -> int | None:
    if routes_df.empty:
        return None

    opts = route_options(routes_df)
    ids = [item[0] for item in opts]
    labels = {item[0]: item[1] for item in opts}

    default_index = 0
    if default_route_id in ids:
        default_index = ids.index(default_route_id)

    chosen = st.selectbox(
        label,
        options=ids,
        index=default_index,
        format_func=lambda x: labels.get(x, str(x)),
        key=key,
    )
    return int(chosen)


init_db()
seed_demo_route_if_empty()

st.title("🚚 Gestor de Rotas")
st.caption("Cadastre, altere, duplique, exclua e acompanhe rotas com mapa animado.")

with st.sidebar:
    st.subheader("Configuração")

    api_key_sidebar = st.text_input(
        "Google Maps API Key",
        value=GOOGLE_MAPS_API_KEY,
        type="password",
        help="Se estiver no .env, ela já será carregada automaticamente.",
    )

    routing_preference = st.selectbox(
        "Preferência de rota",
        options=["TRAFFIC_AWARE", "TRAFFIC_UNAWARE", "TRAFFIC_AWARE_OPTIMAL"],
        index=0,
    )

    zoom_start = st.slider("Zoom do mapa", 10, 18, 12)
    speed_ms = st.slider("Velocidade da animação (ms)", min_value=20, max_value=300, value=80, step=10)

api_key = api_key_sidebar.strip()

routes_df = list_routes()
default_route_id = get_default_route_id(routes_df)

k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Rotas cadastradas", len(routes_df))
with k2:
    st.metric("Motoristas distintos", routes_df["motorista"].replace("", pd.NA).dropna().nunique() if not routes_df.empty else 0)
with k3:
    st.metric("Rotas ativas", int((routes_df["status"] == "Em rota").sum()) if not routes_df.empty else 0)

tab1, tab2, tab3 = st.tabs(["Nova rota", "Editar rota", "Mapa / Execução"])

with tab1:
    st.subheader("Cadastrar nova rota")

    with st.form("form_nova_rota"):
        c1, c2, c3 = st.columns(3)
        nome_rota = c1.text_input("Nome da rota", value="")
        motorista = c2.text_input("Motorista", value="")
        placa = c3.text_input("Placa", value="")

        c4, c5, c6 = st.columns([2, 2, 1])
        origem = c4.text_input("Origem", value="")
        destino = c5.text_input("Destino", value="")
        status = c6.selectbox("Status", ["Nova", "Planejada", "Em rota", "Finalizada"], index=0)

        st.markdown("**Paradas**")
        nova_df = st.data_editor(
            blank_stops_df(),
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config={
                "sequencia": st.column_config.NumberColumn("Seq.", disabled=True),
                "pedido": st.column_config.TextColumn("Pedido"),
                "cliente": st.column_config.TextColumn("Cliente"),
                "endereco": st.column_config.TextColumn("Endereço", width="large"),
                "cidade": st.column_config.TextColumn("Cidade"),
                "status_entrega": st.column_config.SelectboxColumn(
                    "Status Entrega",
                    options=["Pendente", "Em rota", "Entregue", "Atrasado", "Falha"],
                ),
            },
            key="novo_editor",
        )

        b1, b2 = st.columns(2)
        salvar_nova = b1.form_submit_button("Salvar rota", use_container_width=True)
        salvar_nova_otimizada = b2.form_submit_button("Salvar e reotimizar", use_container_width=True)

    if salvar_nova or salvar_nova_otimizada:
        try:
            if not nome_rota.strip():
                raise ValueError("Informe o nome da rota.")
            if not origem.strip():
                raise ValueError("Informe a origem.")
            if not destino.strip():
                raise ValueError("Informe o destino.")

            stops_df = normalize_stops_df(pd.DataFrame(nova_df))

            if salvar_nova_otimizada:
                paradas = stops_to_addresses(stops_df)
                result = compute_optimized_route(api_key=api_key, origem=origem, destino=destino, paradas=paradas, routing_preference=routing_preference)
                stops_df = reorder_stops_df(stops_df, result["optimized_indices"])

            new_route_id = insert_route(
                nome_rota=nome_rota.strip(),
                origem=origem.strip(),
                destino=destino.strip(),
                motorista=motorista.strip(),
                placa=placa.strip(),
                status=status,
            )
            replace_stops(new_route_id, stops_df)

            st.session_state["selected_route_id"] = new_route_id
            st.success(f"Rota {new_route_id} salva com sucesso.")
            st.rerun()

        except Exception as e:
            st.error(str(e))

with tab2:
    st.subheader("Editar rota existente")

    if routes_df.empty:
        st.info("Ainda não existem rotas cadastradas.")
    else:
        selected_edit_id = route_selectbox("Selecione a rota", routes_df, key="edit_route_select", default_route_id=default_route_id)

        if selected_edit_id is not None:
            header = get_route_header(selected_edit_id)
            stops_df = get_route_stops(selected_edit_id)

            if header is None:
                st.error("Rota não encontrada.")
            else:
                with st.form(f"form_editar_{selected_edit_id}"):
                    e1, e2, e3 = st.columns(3)
                    nome_rota_e = e1.text_input("Nome da rota", value=header["nome_rota"])
                    motorista_e = e2.text_input("Motorista", value=header.get("motorista", "") or "")
                    placa_e = e3.text_input("Placa", value=header.get("placa", "") or "")

                    e4, e5, e6 = st.columns([2, 2, 1])
                    origem_e = e4.text_input("Origem", value=header["origem"])
                    destino_e = e5.text_input("Destino", value=header["destino"])
                    status_e = e6.selectbox(
                        "Status",
                        ["Nova", "Planejada", "Em rota", "Finalizada"],
                        index=["Nova", "Planejada", "Em rota", "Finalizada"].index(
                            header.get("status", "Nova") if header.get("status", "Nova") in ["Nova", "Planejada", "Em rota", "Finalizada"] else "Nova"
                        ),
                    )

                    st.markdown("**Paradas**")
                    edit_df = st.data_editor(
                        stops_df,
                        num_rows="dynamic",
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "sequencia": st.column_config.NumberColumn("Seq.", disabled=True),
                            "pedido": st.column_config.TextColumn("Pedido"),
                            "cliente": st.column_config.TextColumn("Cliente"),
                            "endereco": st.column_config.TextColumn("Endereço", width="large"),
                            "cidade": st.column_config.TextColumn("Cidade"),
                            "status_entrega": st.column_config.SelectboxColumn(
                                "Status Entrega",
                                options=["Pendente", "Em rota", "Entregue", "Atrasado", "Falha"],
                            ),
                        },
                        key=f"edit_editor_{selected_edit_id}",
                    )

                    x1, x2 = st.columns(2)
                    salvar_edicao = x1.form_submit_button("Salvar alterações", use_container_width=True)
                    salvar_edicao_otimizada = x2.form_submit_button("Salvar e reotimizar", use_container_width=True)

                y1, y2 = st.columns(2)
                duplicar = y1.button("Duplicar rota", use_container_width=True, key=f"dup_{selected_edit_id}")
                excluir = y2.button("Excluir rota", use_container_width=True, key=f"del_{selected_edit_id}")

                if salvar_edicao or salvar_edicao_otimizada:
                    try:
                        if not nome_rota_e.strip():
                            raise ValueError("Informe o nome da rota.")
                        if not origem_e.strip():
                            raise ValueError("Informe a origem.")
                        if not destino_e.strip():
                            raise ValueError("Informe o destino.")

                        updated_stops_df = normalize_stops_df(pd.DataFrame(edit_df))

                        if salvar_edicao_otimizada:
                            paradas = stops_to_addresses(updated_stops_df)
                            result = compute_optimized_route(api_key=api_key, origem=origem_e, destino=destino_e, paradas=paradas, routing_preference=routing_preference)
                            updated_stops_df = reorder_stops_df(updated_stops_df, result["optimized_indices"])

                        update_route(
                            route_id=selected_edit_id,
                            nome_rota=nome_rota_e.strip(),
                            origem=origem_e.strip(),
                            destino=destino_e.strip(),
                            motorista=motorista_e.strip(),
                            placa=placa_e.strip(),
                            status=status_e,
                            stops_df=updated_stops_df,
                        )

                        st.session_state["selected_route_id"] = selected_edit_id
                        st.success("Rota atualizada com sucesso.")
                        st.rerun()

                    except Exception as e:
                        st.error(str(e))

                if duplicar:
                    try:
                        novo_id = duplicate_route(selected_edit_id)
                        st.session_state["selected_route_id"] = novo_id
                        st.success(f"Rota duplicada com sucesso. Nova rota: {novo_id}")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

                if excluir:
                    try:
                        delete_route(selected_edit_id)
                        st.session_state.pop("map_result", None)
                        st.success("Rota excluída com sucesso.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

with tab3:
    st.subheader("Mapa e execução da rota")

    if routes_df.empty:
        st.info("Cadastre uma rota para visualizar o mapa.")
    else:
        selected_map_id = route_selectbox("Selecione a rota", routes_df, key="map_route_select", default_route_id=default_route_id)

        if selected_map_id is not None:
            header = get_route_header(selected_map_id)
            stops_df = get_route_stops(selected_map_id)

            if header is None:
                st.error("Rota não encontrada.")
            else:
                st.markdown(
                    f"**Rota:** {header['nome_rota']}  \n"
                    f"**Motorista:** {header.get('motorista', '') or '-'}  \n"
                    f"**Placa:** {header.get('placa', '') or '-'}  \n"
                    f"**Status:** {header.get('status', '') or '-'}"
                )

                save_optimized_order = st.checkbox("Salvar ordem otimizada no banco", value=True, key="save_optimized_checkbox")
                gerar_mapa = st.button("Gerar / Atualizar rota no mapa", use_container_width=True)

                if gerar_mapa:
                    try:
                        stops_df = normalize_stops_df(stops_df)
                        paradas = stops_to_addresses(stops_df)

                        result = compute_optimized_route(
                            api_key=api_key,
                            origem=header["origem"],
                            destino=header["destino"],
                            paradas=paradas,
                            routing_preference=routing_preference,
                        )

                        optimized_stops_df = reorder_stops_df(stops_df, result["optimized_indices"])
                        optimized_addresses = stops_to_addresses(optimized_stops_df)
                        optimized_labels = build_labels(optimized_stops_df)

                        if save_optimized_order:
                            update_route(
                                route_id=selected_map_id,
                                nome_rota=header["nome_rota"],
                                origem=header["origem"],
                                destino=header["destino"],
                                motorista=header.get("motorista", "") or "",
                                placa=header.get("placa", "") or "",
                                status=header.get("status", "Nova") or "Nova",
                                stops_df=optimized_stops_df,
                            )

                        st.session_state["selected_route_id"] = selected_map_id
                        st.session_state["map_result"] = {
                            "route_id": selected_map_id,
                            "result": result,
                            "optimized_stops_df": optimized_stops_df.to_dict(orient="records"),
                            "optimized_addresses": optimized_addresses,
                            "optimized_labels": optimized_labels,
                            "origem": header["origem"],
                            "destino": header["destino"],
                        }

                        st.success("Mapa atualizado com sucesso.")
                        st.rerun()

                    except Exception as e:
                        st.error(str(e))

                map_result = st.session_state.get("map_result")
                if map_result and map_result.get("route_id") == selected_map_id:
                    result = map_result["result"]
                    optimized_stops_df = pd.DataFrame(map_result["optimized_stops_df"])
                    optimized_addresses = map_result["optimized_addresses"]
                    optimized_labels = map_result["optimized_labels"]

                    m1, m2, m3 = st.columns(3)
                    with m1:
                        st.metric("Distância total", f"{result['distance_km']} km")
                    with m2:
                        st.metric("Tempo estimado", f"{result['duration_min']} min")
                    with m3:
                        st.metric("Paradas", len(optimized_stops_df))

                    link_maps = generate_google_maps_link(
                        origem=map_result["origem"],
                        destino=map_result["destino"],
                        paradas=optimized_addresses[:9],
                    )
                    st.link_button("Abrir no Google Maps", link_maps, use_container_width=True)

                    view_df = optimized_stops_df.copy()
                    view_df["ordem_final"] = range(1, len(view_df) + 1)
                    view_df = view_df[
                        ["ordem_final", "pedido", "cliente", "endereco", "cidade", "status_entrega"]
                    ].rename(
                        columns={
                            "ordem_final": "Ordem",
                            "pedido": "Pedido",
                            "cliente": "Cliente",
                            "endereco": "Endereço",
                            "cidade": "Cidade",
                            "status_entrega": "Status",
                        }
                    )

                    st.markdown("**Paradas na ordem otimizada**")
                    st.dataframe(view_df, use_container_width=True, hide_index=True)

                    st.markdown("**Mapa interativo**")
                    html = build_map_html(
                        route_points=result["route_points"],
                        origem=map_result["origem"],
                        destino=map_result["destino"],
                        stop_labels=optimized_labels,
                        stop_addresses=optimized_addresses,
                        speed_ms=speed_ms,
                        zoom_start=zoom_start,
                    )
                    components.html(html, height=760, scrolling=False)
                else:
                    st.info("Clique em 'Gerar / Atualizar rota no mapa' para calcular a rota e exibir o mapa.")