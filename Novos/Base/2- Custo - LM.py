# =========================
# BLOCO 1/3 — IMPORTS + CONFIG + HTTP (RETRY)
# =========================
# -*- coding: utf-8 -*-

import os
import json
import time
import warnings
from datetime import datetime
from typing import Dict, Any, List, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ======================================================
# ⚙️ CONFIGURAÇÕES (AJUSTE AQUI)
# ======================================================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Custo - Coordenador"
COORDENADOR_PATH = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"

OUTPUT_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Custos - Coordenadores"
ARQUIVO_MORTO = os.path.join(OUTPUT_DIR, "Arquivo Morto")
IMAGENS_DIR = os.path.join(OUTPUT_DIR, "Imagens_Coordenadores")

LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/"
    "matheus_carvalho_jtexpressdf_onmicrosoft_com/"
    "IgAcZvPQH2w4Sq4XjYZiL5g1AfacXr80tUhQHJzX8QGR92I?e=Yc0rtm"
)

# ======================================================
# 🏷️ NOME DO INDICADOR (VAI APARECER NA IMAGEM)
# ======================================================
INDICADOR_NOME = "Custos e Ressarcimento"  # <-- ajuste aqui como quiser

# ======================================================
# ✅ WEBHOOKS POR COORDENADOR (OPÇÃO 1: COLE AQUI)
# ======================================================
COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/1f3f48d7-b60c-45c1-87ee-6cc8ab9f6467",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/b448a316-f146-49d0-9f0a-90b1f086b8a7",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/fa768680-b4ab-4d87-bf2c-285c91034dad",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/e14d0307-c6d6-472b-bea1-d83a5573dc1b",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/4cfd01be-defa-4adb-936e-6bfbee5326a6",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/e3e31e14-79ab-4a95-8a2d-be99e1fc9b10",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/9ce83b77-04ad-4558-ab83-39929b30f092",
    "Fabio Souza": "https://open.feishu.cn/open-apis/bot/v2/hook/2490eb81-2b2e-4854-b260-430e2e467926",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/eb777d25-f454-4db7-9364-edf95ee37063",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/99557a7f-ca4e-4ede-b9e5-ccd7ad85b96a",
    "Ana Cunha": "https://open.feishu.cn/open-apis/bot/v2/hook/d39b486f-93d2-4c22-b1cb-46d26e488118",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/7b9fc992-ba9c-4d1d-9c2c-91493f05d4e2",
}

# (Opcional) webhook geral fallback se algum coord não tiver webhook definido
WEBHOOK_FALLBACK = os.getenv("FEISHU_WEBHOOK_URL", "").strip()

# ======================================================
# 🔐 FEISHU OPEN API (upload -> image_key)
# ======================================================
BASE_DOMAIN = "https://open.feishu.cn"
APP_ID = os.getenv("APP_ID", "cli_a906d2d682f8dbd8").strip()
APP_SECRET = os.getenv("APP_SECRET", "Fzh1cr6K55a3oQUBV9wCZd6AWiZH5ONw").strip()

# Se o grupo exigir keyword (opcional)
FEISHU_KEYWORD = os.getenv("FEISHU_KEYWORD", "").strip()

# ======================================================
# 🎛️ AJUSTES
# ======================================================
ROWS_PER_PAGE = 28  # quantas bases por imagem (ajuste)
SLEEP_ENTRE_PAGINAS = 0.4
SLEEP_ENTRE_COORDS = 0.8

DATA_ATUAL = datetime.now().strftime("%Y%m%d_%H%M%S")
DATA_HUMANA = datetime.now().strftime("%d/%m/%Y %H:%M")
ARQUIVO_SAIDA = os.path.join(OUTPUT_DIR, f"Custos_Consolidado_{DATA_ATUAL}.xlsx")

# ======================================================
# 🌐 SESSION com retry (menos 10054)
# ======================================================
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = build_session()

def post_json(url: str, payload: dict, headers: dict = None, timeout=(10, 60), tag: str = "") -> dict:
    headers = headers or {}
    last_err = None
    for attempt in range(1, 7):
        try:
            r = SESSION.post(url, json=payload, headers=headers, timeout=timeout)
            try:
                data = r.json()
            except Exception:
                data = {"raw": r.text, "http_status": r.status_code}

            if r.status_code >= 400:
                raise requests.HTTPError(f"{tag} HTTP {r.status_code} body={r.text[:400]}")

            return data
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                requests.HTTPError) as e:
            last_err = e
            time.sleep(0.8 * attempt)
    raise RuntimeError(f"{tag} Falhou após retries. Último erro: {last_err}")

def post_multipart(url: str, data: dict, files: dict, headers: dict = None, timeout=(10, 120), tag: str = "") -> dict:
    headers = headers or {}
    last_err = None
    for attempt in range(1, 7):
        try:
            r = SESSION.post(url, data=data, files=files, headers=headers, timeout=timeout)
            try:
                resp = r.json()
            except Exception:
                resp = {"raw": r.text, "http_status": r.status_code}

            if r.status_code >= 400:
                raise requests.HTTPError(f"{tag} HTTP {r.status_code} body={r.text[:400]}")

            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                requests.HTTPError) as e:
            last_err = e
            time.sleep(0.8 * attempt)
    raise RuntimeError(f"{tag} Falhou após retries. Último erro: {last_err}")

def money_br(valor: float) -> str:
    return (
        f"R$ {valor:,.2f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )

def safe_str(x: Any) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def require_env():
    if not APP_ID or not APP_SECRET:
        raise RuntimeError("❌ APP_ID/APP_SECRET não definidos (env).")

def get_webhook_do_coordenador(coord: str) -> str:
    """
    1) tenta achar no dicionário COORDENADOR_WEBHOOKS
    2) se não tiver, usa WEBHOOK_FALLBACK (se você definiu)
    """
    w = (COORDENADOR_WEBHOOKS.get(coord) or "").strip()
    if w:
        return w
    return WEBHOOK_FALLBACK
# =========================
# BLOCO 2/3 — LEITURA + PROCESSAMENTO + IMAGEM (DARK, SEM BARRA)
# =========================

def encontrar_arquivo_entrada(pasta: str) -> str:
    arquivos = [
        f for f in os.listdir(pasta)
        if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")
    ]
    if not arquivos:
        raise FileNotFoundError("❌ Nenhum arquivo Excel encontrado na pasta de entrada.")
    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(pasta, f)), reverse=True)
    return os.path.join(pasta, arquivos[0])

def carregar_excel_auto(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsx":
        return pd.read_excel(path, dtype=str, engine="openpyxl")
    try:
        return pd.read_excel(path, dtype=str, engine="xlrd")  # .xls
    except Exception:
        return pd.read_excel(path, dtype=str)

def to_float_safe(series: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(
            series.astype(str)
            .str.replace(",", ".", regex=False)
            .str.extract(r"(\d+\.?\d*)")[0],
            errors="coerce",
        )
        .fillna(0)
    )

def _chunk_list(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]

def gerar_imagens_todas_as_bases_dark(
    coord: str,
    indicador_nome: str,  # <-- NOVO: vai aparecer no header da imagem
    total_pedidos: int,
    custo_total: float,
    total_bases: int,
    rows_all: List[Tuple[str, int, float]],
    out_dir: str,
    rows_per_page: int = 28,
) -> List[str]:
    """
    ✅ SEM BARRA
    ✅ Custo sempre visível (alinhado à direita)
    ✅ Todas as bases (paginado)
    ✅ Mostra nome do indicador no header
    """
    from PIL import Image, ImageDraw, ImageFont

    os.makedirs(out_dir, exist_ok=True)

    def load_font(size: int, bold: bool = False):
        candidates = [
            ("segoeuib.ttf" if bold else "segoeui.ttf"),
            ("arialbd.ttf" if bold else "arial.ttf"),
            ("calibrib.ttf" if bold else "calibri.ttf"),
        ]
        for name in candidates:
            try:
                return ImageFont.truetype(name, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def rr(draw: ImageDraw.ImageDraw, xy, r, fill, outline=None, width=1):
        try:
            draw.rounded_rectangle(xy, radius=r, fill=fill, outline=outline, width=width)
        except Exception:
            draw.rectangle(xy, fill=fill, outline=outline, width=width)

    # Tema dark
    BG = (12, 14, 18)
    CARD = (22, 26, 34)
    STROKE = (45, 54, 72)
    TXT = (235, 238, 244)
    MUTED = (160, 170, 190)
    Z1 = (18, 22, 30)
    Z2 = (15, 18, 26)
    GREEN1 = (16, 185, 129)
    GREEN2 = (5, 150, 105)

    W = 1500
    padding = 34
    header_h = 130
    cards_h = 110
    gap = 16
    row_h = 46

    pages = _chunk_list(rows_all, rows_per_page)
    total_pages = max(1, len(pages))
    out_paths: List[str] = []

    f_title = load_font(30, bold=True)
    f_sub = load_font(18, bold=False)
    f_sub_bold = load_font(18, bold=True)
    f_card_label = load_font(16, bold=False)
    f_card_value = load_font(22, bold=True)
    f_head = load_font(18, bold=True)
    f_row = load_font(17, bold=False)

    indicador_nome = (indicador_nome or "").strip()
    if not indicador_nome:
        indicador_nome = "Indicador"

    for page_idx, page_rows in enumerate(pages, start=1):
        table_h = 90 + (len(page_rows) * row_h) + 38
        H = padding * 2 + header_h + gap + cards_h + gap + table_h

        img = Image.new("RGB", (W, H), BG)
        draw = ImageDraw.Draw(img)

        rr(draw, (padding, padding, W - padding, H - padding), 26, CARD, outline=STROKE, width=2)

        # header gradiente
        hx1, hy1 = padding + 18, padding + 18
        hx2, hy2 = W - padding - 18, padding + header_h
        for i in range(hy2 - hy1):
            t = i / max(1, (hy2 - hy1))
            c = (
                int(GREEN2[0] + (GREEN1[0] - GREEN2[0]) * t),
                int(GREEN2[1] + (GREEN1[1] - GREEN2[1]) * t),
                int(GREEN2[2] + (GREEN1[2] - GREEN2[2]) * t),
            )
            draw.line([(hx1, hy1 + i), (hx2, hy1 + i)], fill=c)

        # Título: Coordenador
        draw.text((hx1 + 22, hy1 + 14), f"{coord}", fill=(255, 255, 255), font=f_title)

        # Linha do indicador (NOVO)
        draw.text(
            (hx1 + 22, hy1 + 56),
            f"Indicador: {indicador_nome}",
            fill=(230, 255, 245),
            font=f_sub_bold
        )

        # Data + paginação (ajustado para descer)
        draw.text(
            (hx1 + 22, hy1 + 86),
            f"Atualizado: {DATA_HUMANA}   •   Página {page_idx}/{total_pages}",
            fill=(230, 255, 245),
            font=f_sub
        )

        # cards métricas
        cx1 = padding + 18
        cy1 = hy2 + gap
        cwidth = (hx2 - hx1 - 2 * gap) // 3

        def metric(x, label, value):
            rr(draw, (x, cy1, x + cwidth, cy1 + cards_h), 18, (17, 20, 28), outline=STROKE, width=2)
            draw.text((x + 16, cy1 + 14), label, fill=MUTED, font=f_card_label)
            draw.text((x + 16, cy1 + 44), value, fill=TXT, font=f_card_value)

        metric(cx1, "Total de pedidos", f"{total_pedidos:,}".replace(",", "."))
        metric(cx1 + cwidth + gap, "Bases avaliadas", f"{total_bases:,}".replace(",", "."))
        metric(cx1 + (cwidth + gap) * 2, "Custo total", money_br(custo_total))

        # tabela
        tx1 = padding + 18
        ty1 = cy1 + cards_h + gap
        tx2 = W - padding - 18

        draw.text((tx1 + 10, ty1 + 14), "Bases (todas) — ordenado por custo", fill=TXT, font=f_head)
        draw.line((tx1, ty1 + 46, tx2, ty1 + 46), fill=STROKE, width=2)

        col_rank = tx1 + 10
        col_base = tx1 + 90
        col_qtd = tx2 - 360
        col_custo_right = tx2 - 16  # custo alinhado à direita

        draw.text((col_rank, ty1 + 58), "#", fill=MUTED, font=f_head)
        draw.text((col_base, ty1 + 58), "Base", fill=MUTED, font=f_head)
        draw.text((col_qtd, ty1 + 58), "Qtd", fill=MUTED, font=f_head)

        custo_head = "Custo"
        try:
            bbox = draw.textbbox((0, 0), custo_head, font=f_head)
            w_head = bbox[2] - bbox[0]
        except Exception:
            w_head = 60
        draw.text((col_custo_right - int(w_head), ty1 + 58), custo_head, fill=MUTED, font=f_head)

        y = ty1 + 92
        start_rank = (page_idx - 1) * rows_per_page

        for i, (base, qtd, custo) in enumerate(page_rows, start=1):
            bg_row = Z1 if (i % 2 == 1) else Z2
            rr(draw, (tx1, y - 8, tx2, y + row_h - 10), 14, bg_row, outline=None)

            rank = start_rank + i
            base_txt = (base or "")[:62]
            custo_fmt = money_br(float(custo))

            draw.text((col_rank, y), f"{rank:02d}", fill=TXT, font=f_row)
            draw.text((col_base, y), base_txt, fill=TXT, font=f_row)
            draw.text((col_qtd, y), str(int(qtd)), fill=TXT, font=f_row)

            try:
                bbox = draw.textbbox((0, 0), custo_fmt, font=f_row)
                w = bbox[2] - bbox[0]
            except Exception:
                w = 120
            draw.text((col_custo_right - int(w), y), custo_fmt, fill=TXT, font=f_row)

            y += row_h

        draw.line((tx1, H - padding - 58, tx2, H - padding - 58), fill=STROKE, width=2)
        draw.text((tx1, H - padding - 42), f"📁 Pasta: {LINK_PASTA}", fill=MUTED, font=f_sub)

        safe_coord = "".join([c for c in coord if c.isalnum() or c in (" ", "_", "-")]).strip().replace(" ", "_")
        filename = f"Custos_{safe_coord}_{DATA_ATUAL}_p{page_idx:02d}.png"
        out_path = os.path.join(out_dir, filename)
        img.save(out_path, "PNG")
        out_paths.append(out_path)

    return out_paths
# =========================
# BLOCO 3/3 — FEISHU TOKEN+UPLOAD + CARD (SÓ NOME) + MAIN
# =========================

_TOKEN: Dict[str, Any] = {"token": None, "exp": 0}

def get_tenant_access_token() -> str:
    now = int(time.time())
    if _TOKEN["token"] and now < int(_TOKEN["exp"]):
        return _TOKEN["token"]

    url = f"{BASE_DOMAIN}/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    data = post_json(url, payload, timeout=(10, 30), tag="[TOKEN]")

    if data.get("code") not in (0, "0", None):
        raise RuntimeError(f"❌ Token falhou: {json.dumps(data, ensure_ascii=False)}")

    token = data.get("tenant_access_token")
    exp = int(data.get("expire", 0))
    if not token:
        raise RuntimeError(f"❌ Resposta sem tenant_access_token: {data}")

    _TOKEN["token"] = token
    _TOKEN["exp"] = now + max(0, exp - 60)
    return token

def upload_image_get_key(image_path: str) -> str:
    token = get_tenant_access_token()
    url = f"{BASE_DOMAIN}/open-apis/im/v1/images"
    headers = {"Authorization": f"Bearer {token}"}

    with open(image_path, "rb") as f:
        resp = post_multipart(
            url,
            data={"image_type": "message"},
            files={"image": (os.path.basename(image_path), f)},
            headers=headers,
            timeout=(10, 120),
            tag="[UPLOAD]",
        )

    if resp.get("code") not in (0, "0", None):
        if resp.get("code") == 234007:
            raise RuntimeError(
                "❌ Upload falhou (234007): sua APP ainda não está com BOT habilitado/ativo "
                "ou a versão não foi aplicada para teste."
            )
        raise RuntimeError(f"❌ Upload falhou: {json.dumps(resp, ensure_ascii=False, indent=2)}")

    image_key = (resp.get("data") or {}).get("image_key")
    if not image_key:
        raise RuntimeError(f"❌ Upload OK mas sem image_key: {resp}")
    return image_key

def enviar_card_somente_nome_com_imagem(
    webhook: str,
    nome_coord: str,          # header: SOMENTE isso
    indicador_nome: str,      # <-- NOVO (opcional no card)
    total_pedidos: int,
    custo_total: float,
    bases_avaliadas: int,
    data_humana: str,
    img_key: str,
    page_label: str,
) -> dict:
    keyword_block = []
    if FEISHU_KEYWORD:
        keyword_block = [{
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**{FEISHU_KEYWORD}**"}
        }, {"tag": "hr"}]

    indicador_nome = (indicador_nome or "").strip()
    if not indicador_nome:
        indicador_nome = "Custos por Base"

    elementos = [
        *keyword_block,
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"📌 **Indicador:** {indicador_nome}\n"
                    f"📅 **Atualizado em:** {data_humana}\n"
                    f"📦 **Total de pedidos:** {total_pedidos}\n"
                    f"💰 **Custo total:** {money_br(float(custo_total))}\n"
                    f"🏢 **Bases avaliadas:** {bases_avaliadas}\n"
                    f"🧾 **Lista completa:** {page_label}\n"
                ),
            },
        },
        {"tag": "hr"},
        {
            "tag": "img",
            "img_key": img_key,
            "alt": {"tag": "plain_text", "content": "Bases ordenadas por custo"},
            "mode": "fit_horizontal",
            "preview": True
        },
        {"tag": "hr"},
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "📁 Abrir Pasta no OneDrive"},
                    "url": LINK_PASTA,
                    "type": "primary",
                }
            ],
        },
    ]

    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {
                "template": "green",
                "title": {"tag": "plain_text", "content": f"{nome_coord}"},
            },
            "elements": elementos,
        },
    }

    return post_json(webhook, payload, timeout=(10, 45), tag="[WEBHOOK_CARD]")

def main():
    require_env()
    print("🚀 Iniciando consolidação de custos...\n")

    file_path = encontrar_arquivo_entrada(BASE_DIR)
    print(f"📂 Arquivo selecionado: {os.path.basename(file_path)}")

    df = carregar_excel_auto(file_path)
    print(f"📄 Planilha carregada ({len(df):,} linhas)".replace(",", "."))

    # remover remessas com "-"
    if "Remessa" in df.columns:
        antes = len(df)
        df["Remessa"] = df["Remessa"].astype(str).str.strip()
        df = df[~df["Remessa"].str.contains("-", na=False)]
        print(f"🧹 Removidas {antes - len(df)} remessas com sufixo '-XX'.")

    # filtrar regionais
    if "Regional responsável" not in df.columns:
        raise RuntimeError("❌ Coluna 'Regional responsável' não encontrada.")
    df["Regional responsável"] = df["Regional responsável"].astype(str).str.upper().str.strip()
    df = df[df["Regional responsável"].isin(["GP", "GO", "PA"])]

    # vincular coordenadores
    df_coord = pd.read_excel(COORDENADOR_PATH, engine="openpyxl")
    col_coord = "Coordenadores" if "Coordenadores" in df_coord.columns else "Coordenador"
    if col_coord not in df_coord.columns:
        raise RuntimeError("❌ No arquivo coordenador, não achei 'Coordenadores' nem 'Coordenador'.")
    if "Nome da base" not in df_coord.columns:
        raise RuntimeError("❌ No arquivo coordenador, não achei 'Nome da base'.")

    df_coord = df_coord.rename(columns={col_coord: "Coordenadores"}).copy()

    if "Base responsável" not in df.columns:
        raise RuntimeError("❌ Coluna 'Base responsável' não encontrada na base de custos.")

    df["Base responsável"] = df["Base responsável"].astype(str).str.upper().str.strip()
    df_coord["Nome da base"] = df_coord["Nome da base"].astype(str).str.upper().str.strip()

    df = (
        pd.merge(
            df,
            df_coord[["Nome da base", "Coordenadores"]],
            left_on="Base responsável",
            right_on="Nome da base",
            how="left",
        )
        .drop(columns=["Nome da base"], errors="ignore")
    )
    print("👥 Coordenadores vinculados.")

    # custo
    if "Valor a pagar (yuan)" in df.columns:
        df["Custo_R$"] = to_float_safe(df["Valor a pagar (yuan)"])
    else:
        df["Custo_R$"] = 0.0

    # pastas
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(ARQUIVO_MORTO, exist_ok=True)
    os.makedirs(IMAGENS_DIR, exist_ok=True)

    # mover antigos
    for arquivo in os.listdir(OUTPUT_DIR):
        if arquivo.lower().endswith(".xlsx") and arquivo.startswith("Custos_Consolidado_"):
            os.replace(os.path.join(OUTPUT_DIR, arquivo), os.path.join(ARQUIVO_MORTO, arquivo))

    # salvar excel
    with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Base_Processada")
    print(f"💾 Arquivo salvo em:\n{ARQUIVO_SAIDA}\n")

    # por coordenador -> manda no webhook dele
    print("📤 Enviando por coordenador (cada um no seu webhook)...\n")
    coords = sorted(df["Coordenadores"].dropna().astype(str).unique())

    enviados = 0
    falhas = 0

    for coord in coords:
        coord = safe_str(coord)
        if not coord:
            continue

        webhook_coord = get_webhook_do_coordenador(coord)
        if not webhook_coord:
            print(f"⚠️ Sem webhook para: {coord} (defina em COORDENADOR_WEBHOOKS ou FEISHU_WEBHOOK_URL)")
            continue

        try:
            df_c = df[df["Coordenadores"].astype(str) == coord].copy()
            if df_c.empty:
                continue

            total_pedidos = len(df_c)
            custo_total = float(df_c["Custo_R$"].sum())
            total_bases = int(df_c["Base responsável"].nunique(dropna=True))

            tbl_all = (
                df_c.groupby("Base responsável", dropna=False)
                    .agg(Qtd=("Base responsável", "size"), Custo=("Custo_R$", "sum"))
                    .reset_index()
                    .sort_values("Custo", ascending=False)
            )

            rows_all: List[Tuple[str, int, float]] = [
                (safe_str(r["Base responsável"]), int(r["Qtd"]), float(r["Custo"]))
                for _, r in tbl_all.iterrows()
            ]

            img_paths = gerar_imagens_todas_as_bases_dark(
                coord=coord,
                indicador_nome=INDICADOR_NOME,  # <-- AQUI entra o nome do indicador
                total_pedidos=total_pedidos,
                custo_total=custo_total,
                total_bases=total_bases,
                rows_all=rows_all,
                out_dir=IMAGENS_DIR,
                rows_per_page=ROWS_PER_PAGE
            )

            total_pages = len(img_paths)

            for idx, img_path in enumerate(img_paths, start=1):
                img_key = upload_image_get_key(img_path)
                page_label = f"Página {idx}/{total_pages}"

                resp = enviar_card_somente_nome_com_imagem(
                    webhook=webhook_coord,
                    nome_coord=coord,
                    indicador_nome=INDICADOR_NOME,  # <-- opcional no card
                    total_pedidos=total_pedidos,
                    custo_total=custo_total,
                    bases_avaliadas=total_bases,
                    data_humana=DATA_HUMANA,
                    img_key=img_key,
                    page_label=page_label
                )
                print(f"✅ {coord} -> {page_label} | retorno: {resp}")
                time.sleep(SLEEP_ENTRE_PAGINAS)

            enviados += 1
            time.sleep(SLEEP_ENTRE_COORDS)

        except Exception as e:
            falhas += 1
            print(f"❌ Falhou ({coord}): {e}")

    print(f"\n🏁 Finalizado! Coordenadores enviados: {enviados} | Falhas: {falhas}")

if __name__ == "__main__":
    main()
