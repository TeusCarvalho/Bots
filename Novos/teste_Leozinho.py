# -*- coding: utf-8 -*-
import pandas as pd
import folium
from folium.plugins import Draw, MarkerCluster
import webbrowser
import os
import json
from pathlib import Path

print("Carregando planilha...")

# ✅ pode ser PASTA ou ARQUIVO .xlsx
entrada = Path(r"C:\Users\mathe_70oz1qs\OneDrive\Desktop\Teste aleatorios")

# ==========================
# RESOLVER ARQUIVO EXCEL
# ==========================
if entrada.is_dir():
    arquivos = sorted(entrada.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not arquivos:
        raise FileNotFoundError(f"Nenhum .xlsx encontrado na pasta: {entrada}")
    arquivo = arquivos[0]
    print(f"Usando o Excel mais recente: {arquivo.name}")
else:
    arquivo = entrada
    if not arquivo.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {arquivo}")

df = pd.read_excel(arquivo)

# ==========================
# LIMPAR NOMES DAS COLUNAS
# ==========================
df.columns = df.columns.str.replace("\n", " ").str.strip()

col_cidade = "Cidade"
col_lat = "Latitude"
col_lon = "Longitude"
col_base = "Localização Base"

# ==========================
# VALIDAR COLUNAS
# ==========================
faltando = [c for c in [col_cidade, col_lat, col_lon, col_base] if c not in df.columns]
if faltando:
    raise ValueError(f"Colunas não encontradas na planilha: {faltando}")

# ==========================
# GARANTIR LAT/LON NUMÉRICO
# ==========================
df[col_lat] = pd.to_numeric(df[col_lat], errors="coerce")
df[col_lon] = pd.to_numeric(df[col_lon], errors="coerce")
df = df.dropna(subset=[col_lat, col_lon]).copy()

if df.empty:
    raise ValueError("Após limpar Latitude/Longitude, não sobrou nenhuma linha válida.")

print("Criando mapa...")

mapa = folium.Map(
    location=[df[col_lat].mean(), df[col_lon].mean()],
    zoom_start=4,
    tiles="CartoDB positron"
)

use_cluster = len(df) > 400
cluster = MarkerCluster().add_to(mapa) if use_cluster else None

# ==========================
# ADICIONAR MARCADORES
# ==========================
for _, row in df.iterrows():
    cidade = str(row[col_cidade])
    lat = float(row[col_lat])
    lon = float(row[col_lon])
    base = str(row[col_base])

    if "abrir" in base.lower():
        icon = folium.Icon(icon="home", prefix="fa", color="red")
    else:
        icon = folium.Icon(icon="circle", prefix="fa", color="blue")

    marcador = folium.Marker(
        [lat, lon],
        tooltip=cidade,
        popup=f"<b>Cidade:</b> {cidade}<br><b>Base:</b> {base}",
        icon=icon
    )

    if cluster is not None:
        marcador.add_to(cluster)
    else:
        marcador.add_to(mapa)

# ==========================
# FERRAMENTA DE DESENHO
# ==========================
Draw(
    export=False,
    draw_options={
        "polygon": True,
        "rectangle": True,
        "circle": True,
        "polyline": False,
        "marker": False,
        "circlemarker": False
    }
).add_to(mapa)

# ==========================
# INJETAR JS (COR + SELEÇÃO + EXPORT EXCEL)
# ==========================
dados_json = json.dumps(df.to_dict(orient="records"), ensure_ascii=False)
map_var = mapa.get_name()

script = f"""
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>

<div style="
position: fixed;
top: 10px;
left: 10px;
z-index: 9999;
background: white;
padding: 10px;
border-radius: 6px;
box-shadow: 0 0 10px rgba(0,0,0,0.3);
font-family: Arial, sans-serif;
">
  <b>Cor do desenho</b><br>
  <input type="color" id="corPoligono" value="#ff0000">
</div>

<script>
document.addEventListener("DOMContentLoaded", function() {{
  var dados = {dados_json};

  // ✅ referência correta do mapa Folium
  var map = {map_var};

  // ✅ cor atual sempre pega do input (evita falha de leitura)
  var corInput = document.getElementById("corPoligono");
  var corAtual = corInput ? corInput.value : "#ff0000";

  if (corInput) {{
    corInput.addEventListener("change", function() {{
      corAtual = this.value;
    }});
  }}

  function dentroPoligono(point, vs) {{
    // point = [lng, lat]
    var x = point[0], y = point[1];
    var inside = false;

    for (var i = 0, j = vs.length - 1; i < vs.length; j = i++) {{
      var xi = vs[i][0], yi = vs[i][1];
      var xj = vs[j][0], yj = vs[j][1];

      var intersect = ((yi > y) != (yj > y))
        && (x < (xj - xi) * (y - yi) / (yj - yi) + xi);

      if (intersect) inside = !inside;
    }}
    return inside;
  }}

  map.on("draw:created", function(e) {{
    var layer = e.layer;
    var tipo = e.layerType; // polygon | rectangle | circle ...

    // ✅ aplica cor escolhida no momento do desenho
    if (layer.setStyle) {{
      layer.setStyle({{
        color: corAtual,
        weight: 3,
        fillColor: corAtual,
        fillOpacity: 0.4
      }});
    }}

    map.addLayer(layer);

    var selecionados = [];

    if (tipo === "circle") {{
      var center = layer.getLatLng();
      var radius = layer.getRadius();

      dados.forEach(function(row) {{
        var p = L.latLng(row["Latitude"], row["Longitude"]);
        if (center.distanceTo(p) <= radius) {{
          selecionados.push(Object.assign({{}}, row, {{
            "Cor_Desenho": corAtual,
            "Tipo_Desenho": tipo
          }}));
        }}
      }});

    }} else if (tipo === "rectangle") {{
      var bounds = layer.getBounds();

      dados.forEach(function(row) {{
        var p = L.latLng(row["Latitude"], row["Longitude"]);
        if (bounds.contains(p)) {{
          selecionados.push(Object.assign({{}}, row, {{
            "Cor_Desenho": corAtual,
            "Tipo_Desenho": tipo
          }}));
        }}
      }});

    }} else if (tipo === "polygon") {{
      var rings = layer.getLatLngs();
      var coords = rings[0].map(function(c) {{
        return [c.lng, c.lat];
      }});

      dados.forEach(function(row) {{
        var p = [row["Longitude"], row["Latitude"]];
        if (dentroPoligono(p, coords)) {{
          selecionados.push(Object.assign({{}}, row, {{
            "Cor_Desenho": corAtual,
            "Tipo_Desenho": tipo
          }}));
        }}
      }});
    }}

    window.selecionados = selecionados;

    var html =
      "<b>Cidades dentro da área</b><br>" +
      "Total: " + selecionados.length +
      "<br><br>" +
      "<button onclick='exportarExcel()'>Exportar Excel</button>";

    layer.bindPopup(html).openPopup();
  }});

  // ✅ exporta seleção atual (com Cor_Desenho e Tipo_Desenho)
  window.exportarExcel = function() {{
    if (!window.selecionados || window.selecionados.length === 0) {{
      alert("Nenhum dado selecionado");
      return;
    }}

    var ws = XLSX.utils.json_to_sheet(window.selecionados);
    var wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Dados");

    // nome com data/hora pra não sobrescrever
    var agora = new Date();
    var nome = "dados_area_" +
      agora.getFullYear() +
      String(agora.getMonth()+1).padStart(2,'0') +
      String(agora.getDate()).padStart(2,'0') + "_" +
      String(agora.getHours()).padStart(2,'0') +
      String(agora.getMinutes()).padStart(2,'0') +
      ".xlsx";

    XLSX.writeFile(wb, nome);
  }};
}});
</script>
"""

mapa.get_root().html.add_child(folium.Element(script))

# ==========================
# SALVAR E ABRIR
# ==========================
saida = arquivo.parent / "MAPA_LOGISTICA.html"
mapa.save(str(saida))

print(f"Mapa criado com sucesso: {saida}")
webbrowser.open_new_tab("file://" + os.path.abspath(str(saida)))