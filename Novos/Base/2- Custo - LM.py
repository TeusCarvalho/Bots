# =========================
# BLOCO 1/3 — IMPORTS + CONFIG + HTTP (RETRY)
# =========================
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import warnings
import unicodedata
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ======================================================
# ⚙️ CONFIGURAÇÕES (AJUSTE AQUI)
# ======================================================
BASE_DIR = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\02 - Custo - Coordenador"
COORDENADOR_PATH = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\01 - Coordenador\Base_Atualizada.xlsx"

OUTPUT_DIR = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Custo - Coordenador LM"
ARQUIVO_MORTO = os.path.join(OUTPUT_DIR, "Arquivo Morto")
IMAGENS_DIR = os.path.join(OUTPUT_DIR, "Imagens_Coordenadores")

LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/IgC-eggsBQa8RaipUTMncPN2AV5655MizZg4mN_qzMkx0-Q?e=hcKSwg"
)

# ======================================================
# 🏷️ NOME DO INDICADOR (VAI APARECER NA IMAGEM)
# ======================================================
INDICADOR_NOME = "Custos e Ressarcimento"

# ======================================================
# ✅ WEBHOOKS POR COORDENADOR
# ======================================================
COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/755a43fe-24b6-42db-89c3-dbf3a0e1e391",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/b448a316-f146-49d0-9f0a-90b1f086b8a7",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/4cfd01be-defa-4adb-936e-6bfbee5326a6",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/e14d0307-c6d6-472b-bea1-d83a5573dc1b",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/e3e31e14-79ab-4a95-8a2d-be99e1fc9b10",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/9ce83b77-04ad-4558-ab83-39929b30f092",
    "Fabio Souza": "https://open.feishu.cn/open-apis/bot/v2/hook/2490eb81-2b2e-4854-b260-430e2e467926",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/eb777d25-f454-4db7-9364-edf95ee37063",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/99557a7f-ca4e-4ede-b9e5-ccd7ad85b96a",
    "Ana Cunha": "https://open.feishu.cn/open-apis/bot/v2/hook/d39b486f-93d2-4c22-b1cb-46d26e488118",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/7b9fc992-ba9c-4d1d-9c2c-91493f05d4e2",
}

WEBHOOK_FALLBACK = os.getenv("FEISHU_WEBHOOK_URL", "").strip()

# ======================================================
# 🔐 FEISHU OPEN API (upload -> image_key)
# ======================================================
BASE_DOMAIN = "https://open.feishu.cn"
APP_ID = os.getenv("APP_ID", "cli_a906d2d682f8dbd8").strip()
APP_SECRET = os.getenv("APP_SECRET", "Fzh1cr6K55a3oQUBV9wCZd6AWiZH5ONw").strip()
FEISHU_KEYWORD = os.getenv("FEISHU_KEYWORD", "").strip()

# ======================================================
# 🎛️ AJUSTES
# ======================================================
ROWS_PER_PAGE = 28
SLEEP_ENTRE_PAGINAS = 0.4
SLEEP_ENTRE_COORDS = 0.8

DATA_ATUAL = datetime.now().strftime("%Y%m%d_%H%M%S")
DATA_HUMANA = datetime.now().strftime("%d/%m/%Y %H:%M")
ARQUIVO_SAIDA = os.path.join(OUTPUT_DIR, f"Custos_Consolidado_{DATA_ATUAL}.xlsx")

REGIONAIS_PERMITIDAS = {"GP", "GO", "PA"}

# (Opcional) debug rápido dos valores (0=off, 1=on)
PARSE_DEBUG = False

# ======================================================
# 🔧 HELPERS
# ======================================================
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

def norm_key(s: Any) -> str:
    s = safe_str(s)
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().casefold()
    return s

COORD_WEBHOOKS_NORM = {norm_key(k): v.strip() for k, v in COORDENADOR_WEBHOOKS.items() if v}

def require_env():
    if not APP_ID or not APP_SECRET:
        raise RuntimeError("❌ APP_ID/APP_SECRET não definidos nas variáveis de ambiente.")

def get_webhook_do_coordenador(coord: str) -> str:
    w = (COORD_WEBHOOKS_NORM.get(norm_key(coord)) or "").strip()
    if w:
        return w
    return WEBHOOK_FALLBACK

# ======================================================
# 🌐 SESSION com retry
# ======================================================
def build_session() -> requests.Session:
    s = requests.Session()
    try:
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
    except TypeError:
        retry = Retry(
            total=6,
            connect=6,
            read=6,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            method_whitelist=frozenset(["GET", "POST"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = build_session()

def post_json(url: str, payload: dict, headers: Optional[dict] = None, timeout=(10, 60), tag: str = "") -> dict:
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
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.HTTPError,
        ) as e:
            last_err = e
            time.sleep(0.8 * attempt)
    raise RuntimeError(f"{tag} Falhou após retries. Último erro: {last_err}")

def post_multipart(url: str, data: dict, files: dict, headers: Optional[dict] = None, timeout=(10, 120), tag: str = "") -> dict:
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
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.HTTPError,
        ) as e:
            last_err = e
            time.sleep(0.8 * attempt)
    raise RuntimeError(f"{tag} Falhou após retries. Último erro: {last_err}")

# =========================
# BLOCO 2/3 — LEITURA + PROCESSAMENTO + IMAGEM (LAYOUT ESTILO RELATÓRIO J&T + DESTAQUES)
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
    """
    Parser robusto:
    - "1.234,56" -> 1234.56
    - "1,234.56" -> 1234.56
    - "1234.56"  -> 1234.56
    - "1234,56"  -> 1234.56
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(r"[^\d,.\-]", "", regex=True)

    has_comma = s.str.contains(",", regex=False)
    has_dot = s.str.contains(r"\.", regex=True)

    both = has_comma & has_dot
    only_comma = has_comma & ~has_dot
    only_dot = has_dot & ~has_comma

    out = pd.Series([None] * len(s), index=s.index, dtype="float64")

    sb = s[both]
    comma_decimal = sb.str.rfind(",") > sb.str.rfind(".")
    sb1 = sb[comma_decimal].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    sb2 = sb[~comma_decimal].str.replace(",", "", regex=False)
    out.loc[sb1.index] = pd.to_numeric(sb1, errors="coerce")
    out.loc[sb2.index] = pd.to_numeric(sb2, errors="coerce")

    sc = s[only_comma].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    out.loc[sc.index] = pd.to_numeric(sc, errors="coerce")

    sd = s[only_dot]
    out.loc[sd.index] = pd.to_numeric(sd, errors="coerce")

    sn = s[~has_comma & ~has_dot]
    out.loc[sn.index] = pd.to_numeric(sn, errors="coerce")

    return out.fillna(0.0)


def _chunk_list(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def gerar_imagens_todas_as_bases_dark(
    coord: str,
    indicador_nome: str,
    total_pedidos: int,
    custo_total: float,
    total_bases: int,
    rows_all: List[Tuple[str, int, float]],
    out_dir: str,
    rows_per_page: int = 28,
) -> List[str]:
    """
    Layout inspirado na referência:
    - fundo vermelho institucional
    - cabeçalho centralizado
    - painel branco arredondado
    - cards de destaque para Total de pedidos / Bases avaliadas / Custo total
    - tabela com cabeçalho vermelho
    """
    from PIL import Image, ImageDraw, ImageFont

    os.makedirs(out_dir, exist_ok=True)

    def load_font(size: int, bold: bool = False):
        candidates = [
            ("segoeuib.ttf" if bold else "segoeui.ttf"),
            ("arialbd.ttf" if bold else "arial.ttf"),
            ("calibrib.ttf" if bold else "calibri.ttf"),
            ("msyhbd.ttc" if bold else "msyh.ttc"),
            ("simhei.ttf" if bold else "simsun.ttc"),
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

    def _measure(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> Tuple[int, int]:
        text = text or ""
        try:
            b = draw.textbbox((0, 0), text, font=font)
            return int(b[2] - b[0]), int(b[3] - b[1])
        except Exception:
            try:
                w, h = draw.textsize(text, font=font)  # type: ignore[attr-defined]
                return int(w), int(h)
            except Exception:
                return int(len(text) * 8), 18

    def _ellipsize(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_w: int) -> str:
        text = text or ""
        w, _ = _measure(draw, text, font)
        if w <= max_w:
            return text

        ell = "..."
        lo, hi = 0, len(text)
        best = ell

        while lo <= hi:
            mid = (lo + hi) // 2
            cand = (text[:mid].rstrip() + ell)
            if _measure(draw, cand, font)[0] <= max_w:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1

        return best

    def _fit_font(
        draw: ImageDraw.ImageDraw,
        text: str,
        start_size: int,
        min_size: int,
        bold: bool,
        max_w: int,
    ):
        size = start_size
        while size >= min_size:
            f = load_font(size, bold=bold)
            if _measure(draw, text, f)[0] <= max_w:
                return f
            size -= 1
        return load_font(min_size, bold=bold)

    def _wrap_lines(
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.ImageFont,
        max_w: int,
        max_lines: int = 2,
    ) -> List[str]:
        text = (text or "").strip()
        if not text:
            return [""]

        words = text.split()
        lines: List[str] = []
        cur = ""

        for w in words:
            cand = (cur + " " + w).strip() if cur else w
            if _measure(draw, cand, font)[0] <= max_w:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                cur = w
                if len(lines) >= max_lines - 1:
                    break

        if cur:
            lines.append(cur)

        if len(lines) > max_lines:
            lines = lines[:max_lines]

        if lines:
            lines[-1] = _ellipsize(draw, lines[-1], font, max_w)

        return lines

    def _draw_centered_line(
        draw: ImageDraw.ImageDraw,
        text: str,
        y: int,
        font: ImageFont.ImageFont,
        fill,
        max_w: int,
        center_x: int,
    ) -> int:
        txt = _ellipsize(draw, text, font, max_w)
        w, h = _measure(draw, txt, font)
        draw.text((center_x - w // 2, y), txt, fill=fill, font=font)
        return y + h

    def fmt_int(n: int) -> str:
        return f"{int(n):,}".replace(",", ".")

    def draw_metric_card(
        draw: ImageDraw.ImageDraw,
        x1: int,
        y1: int,
        x2: int,
        y2: int,
        label: str,
        value: str,
        fill,
        label_fill,
        value_fill,
        border,
        highlight: bool = False,
    ):
        rr(draw, (x1, y1, x2, y2), 18, fill, outline=border, width=2)

        label_font = load_font(19, bold=False)
        value_font = load_font(30 if highlight else 27, bold=True)

        inner_w = (x2 - x1) - 24
        label = _ellipsize(draw, label, label_font, inner_w)
        value = _ellipsize(draw, value, value_font, inner_w)

        lw, lh = _measure(draw, label, label_font)
        vw, vh = _measure(draw, value, value_font)

        draw.text((x1 + ((x2 - x1) - lw) // 2, y1 + 16), label, fill=label_fill, font=label_font)
        draw.text((x1 + ((x2 - x1) - vw) // 2, y1 + 48), value, fill=value_fill, font=value_font)

    RED_BG = (227, 6, 19)
    RED_HDR = (235, 0, 0)
    RED_STRONG = (212, 0, 0)
    RED_SOFT = (255, 238, 240)
    WHITE = (255, 255, 255)
    PANEL_BG = (255, 255, 255)
    ROW_A = (252, 252, 252)
    ROW_B = (245, 245, 245)
    GRID = (226, 226, 226)
    TEXT = (51, 51, 51)
    MUTED = (105, 105, 105)

    W = 1600
    OUTER = 18
    TOP_AREA_H = 142

    PANEL_PAD = 22
    SUMMARY_H = 112
    SUMMARY_GAP = 16
    TABLE_HEAD_H = 68
    ROW_H = 42
    PANEL_BOTTOM = 22
    FOOTER_RED_H = 62

    coord = safe_str(coord) or "Sem Coordenador"
    indicador_nome = safe_str(indicador_nome) or "Indicador"

    pages = _chunk_list(rows_all, rows_per_page)
    if not pages:
        pages = [[]]

    total_pages = len(pages)
    out_paths: List[str] = []

    for page_idx, page_rows in enumerate(pages, start=1):
        row_count = len(page_rows)

        panel_x1 = OUTER
        panel_x2 = W - OUTER
        panel_y1 = TOP_AREA_H + 10

        panel_h = (
            PANEL_PAD
            + SUMMARY_H
            + SUMMARY_GAP
            + TABLE_HEAD_H
            + (row_count * ROW_H)
            + PANEL_BOTTOM
        )
        panel_y2 = panel_y1 + panel_h

        H = panel_y2 + FOOTER_RED_H + 18

        img = Image.new("RGB", (W, H), RED_BG)
        draw = ImageDraw.Draw(img)

        center_x = W // 2

        # =========================
        # Topo vermelho
        # =========================
        f_logo_big = load_font(42, bold=True)
        f_logo_small = load_font(20, bold=True)
        draw.text((22, 24), "J&T", fill=WHITE, font=f_logo_big)
        draw.text((106, 43), "EXPRESS", fill=WHITE, font=f_logo_small)

        max_center_w = W - 380

        titulo = f"Relatório de {indicador_nome}"
        subtitulo = f"Coordenador: {coord}"
        linha_info = (
            f"Atualizado: {DATA_HUMANA}   •   "
            f"Página {page_idx}/{total_pages}"
        )

        f_title = _fit_font(draw, titulo, start_size=35, min_size=24, bold=True, max_w=max_center_w)
        f_sub = _fit_font(draw, subtitulo, start_size=26, min_size=18, bold=True, max_w=max_center_w)
        f_meta = load_font(18, bold=False)

        y = 18
        y = _draw_centered_line(draw, titulo, y, f_title, WHITE, max_center_w, center_x) + 5
        y = _draw_centered_line(draw, subtitulo, y, f_sub, WHITE, max_center_w, center_x) + 7

        meta_lines = _wrap_lines(draw, linha_info, f_meta, max_center_w, max_lines=2)
        for line in meta_lines:
            y = _draw_centered_line(draw, line, y, f_meta, WHITE, max_center_w, center_x) + 2

        # =========================
        # Painel branco
        # =========================
        rr(draw, (panel_x1, panel_y1, panel_x2, panel_y2), 20, PANEL_BG, outline=None, width=1)

        inner_x1 = panel_x1 + PANEL_PAD
        inner_x2 = panel_x2 - PANEL_PAD
        inner_y1 = panel_y1 + PANEL_PAD
        inner_w = inner_x2 - inner_x1

        # =========================
        # Cards de destaque
        # =========================
        card_gap = 16
        card_w = (inner_w - (2 * card_gap)) // 3

        c1_x1 = inner_x1
        c1_x2 = c1_x1 + card_w

        c2_x1 = c1_x2 + card_gap
        c2_x2 = c2_x1 + card_w

        c3_x1 = c2_x2 + card_gap
        c3_x2 = inner_x2

        c_y1 = inner_y1
        c_y2 = c_y1 + SUMMARY_H

        draw_metric_card(
            draw,
            c1_x1,
            c_y1,
            c1_x2,
            c_y2,
            "Total de pedidos",
            fmt_int(total_pedidos),
            fill=WHITE,
            label_fill=MUTED,
            value_fill=RED_STRONG,
            border=(230, 230, 230),
            highlight=True,
        )

        draw_metric_card(
            draw,
            c2_x1,
            c_y1,
            c2_x2,
            c_y2,
            "Bases avaliadas",
            fmt_int(total_bases),
            fill=WHITE,
            label_fill=MUTED,
            value_fill=TEXT,
            border=(230, 230, 230),
            highlight=False,
        )

        draw_metric_card(
            draw,
            c3_x1,
            c_y1,
            c3_x2,
            c_y2,
            "Custo total",
            money_br(custo_total),
            fill=RED_SOFT,
            label_fill=RED_STRONG,
            value_fill=RED_STRONG,
            border=(245, 190, 195),
            highlight=True,
        )

        # =========================
        # Tabela
        # =========================
        table_y1 = c_y2 + SUMMARY_GAP

        w_rank = 90
        w_qtd = 210
        w_custo = 260
        w_base = inner_w - w_rank - w_qtd - w_custo

        col1_x1 = inner_x1
        col1_x2 = col1_x1 + w_rank

        col2_x1 = col1_x2
        col2_x2 = col2_x1 + w_base

        col3_x1 = col2_x2
        col3_x2 = col3_x1 + w_qtd

        col4_x1 = col3_x2
        col4_x2 = inner_x2

        rr(draw, (inner_x1, table_y1, inner_x2, table_y1 + TABLE_HEAD_H), 14, RED_HDR, outline=None, width=1)

        for x in [col1_x2, col2_x2, col3_x2]:
            draw.line((x, table_y1, x, table_y1 + TABLE_HEAD_H), fill=WHITE, width=2)

        f_th = load_font(18, bold=True)

        headers = [
            ("Rank", col1_x1, col1_x2),
            ("Base de Entrega", col2_x1, col2_x2),
            ("Qtd de Pedidos", col3_x1, col3_x2),
            ("Custo Total", col4_x1, col4_x2),
        ]

        for text, x1, x2 in headers:
            fw, fh = _measure(draw, text, f_th)
            tx = x1 + ((x2 - x1) - fw) // 2
            ty = table_y1 + ((TABLE_HEAD_H - fh) // 2)
            draw.text((tx, ty), text, fill=WHITE, font=f_th)

        f_row = load_font(18, bold=False)
        start_y = table_y1 + TABLE_HEAD_H

        for i, (base, qtd, custo) in enumerate(page_rows, start=1):
            y1 = start_y + ((i - 1) * ROW_H)
            y2 = y1 + ROW_H

            fill_row = ROW_A if i % 2 == 1 else ROW_B
            draw.rectangle((inner_x1, y1, inner_x2, y2), fill=fill_row)

            draw.line((inner_x1, y2, inner_x2, y2), fill=GRID, width=1)
            for x in [col1_x2, col2_x2, col3_x2]:
                draw.line((x, y1, x, y2), fill=GRID, width=1)

            rank_txt = f"{((page_idx - 1) * rows_per_page) + i}"
            base_txt = _ellipsize(draw, safe_str(base), f_row, (w_base - 28))
            qtd_txt = fmt_int(int(qtd))
            custo_txt = money_br(float(custo))

            rw, rh = _measure(draw, rank_txt, f_row)
            draw.text(
                (col1_x1 + ((w_rank - rw) // 2), y1 + ((ROW_H - rh) // 2)),
                rank_txt,
                fill=TEXT,
                font=f_row,
            )

            bw, bh = _measure(draw, base_txt, f_row)
            draw.text(
                (col2_x1 + 14, y1 + ((ROW_H - bh) // 2)),
                base_txt,
                fill=TEXT,
                font=f_row,
            )

            qw, qh = _measure(draw, qtd_txt, f_row)
            draw.text(
                (col3_x1 + ((w_qtd - qw) // 2), y1 + ((ROW_H - qh) // 2)),
                qtd_txt,
                fill=TEXT,
                font=f_row,
            )

            cw, ch = _measure(draw, custo_txt, f_row)
            draw.text(
                (col4_x2 - cw - 16, y1 + ((ROW_H - ch) // 2)),
                custo_txt,
                fill=RED_STRONG if float(custo) > 0 else TEXT,
                font=f_row,
            )

        total_table_h = TABLE_HEAD_H + (row_count * ROW_H)
        draw.rounded_rectangle(
            (inner_x1, table_y1, inner_x2, table_y1 + total_table_h),
            radius=14,
            outline=GRID,
            width=1,
        )

        # =========================
        # Rodapé vermelho
        # =========================
        f_footer = load_font(15, bold=False)
        footer_y = panel_y2 + 16

        footer_txt_1 = f"Pasta compartilhada: {LINK_PASTA}"
        footer_txt_2 = f"J&T Express • {indicador_nome}"

        footer_txt_1 = _ellipsize(draw, footer_txt_1, f_footer, W - 120)

        w1, h1 = _measure(draw, footer_txt_1, f_footer)
        w2, h2 = _measure(draw, footer_txt_2, f_footer)

        draw.text((center_x - w1 // 2, footer_y), footer_txt_1, fill=WHITE, font=f_footer)
        draw.text((center_x - w2 // 2, footer_y + h1 + 4), footer_txt_2, fill=WHITE, font=f_footer)

        safe_coord = "".join([c for c in coord if c.isalnum() or c in (" ", "_", "-")]).strip().replace(" ", "_")
        filename = f"Custos_{safe_coord}_{DATA_ATUAL}_p{page_idx:02d}.png"
        out_path = os.path.join(out_dir, filename)

        img.save(out_path, "PNG")
        out_paths.append(out_path)

    return out_paths

# =========================
# BLOCO 3/3 — FEISHU TOKEN+UPLOAD + CARD + MAIN
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
    nome_coord: str,
    indicador_nome: str,
    total_pedidos: int,
    custo_total: float,
    bases_avaliadas: int,
    data_humana: str,
    img_key: str,
    page_label: str,
) -> dict:
    keyword_block = []
    if FEISHU_KEYWORD:
        keyword_block = [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**{FEISHU_KEYWORD}**"}},
            {"tag": "hr"},
        ]

    indicador_nome = (indicador_nome or "").strip() or "Custos por Base"

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
            "preview": True,
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
                "template": "red",
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

    # remover remessas no formato: 888001568747917-001 (número + hífen + 3 dígitos)
    if "Remessa" in df.columns:
        s = df["Remessa"].astype(str).str.strip()

        # casa a STRING INTEIRA (evita falso-positivo em textos)
        # aceita hífen normal "-" e também “–” / “—”
        padrao = r"^\d{6,}[-–—]\s*\d{3}$"

        mask = s.str.match(padrao, na=False)
        df = df[~mask]

        print(f"🧹 Removidas {int(mask.sum())} remessas no padrão 'numero-000' (ex: 888...-001).")

    # filtrar regionais
    if "Regional responsável" not in df.columns:
        raise RuntimeError("❌ Coluna 'Regional responsável' não encontrada.")
    df["Regional responsável"] = df["Regional responsável"].fillna("").astype(str).str.upper().str.strip()
    df = df[df["Regional responsável"].isin(REGIONAIS_PERMITIDAS)]

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

    df["Base responsável"] = df["Base responsável"].fillna("").astype(str).str.upper().str.strip()
    df_coord["Nome da base"] = df_coord["Nome da base"].fillna("").astype(str).str.upper().str.strip()

    # ✅ CORREÇÃO: evitar duplicação no merge (many-to-many)
    dup_count = df_coord["Nome da base"].duplicated().sum()
    if dup_count > 0:
        print(f"⚠️ Atenção: {dup_count} bases duplicadas em Base_Atualizada.xlsx (usando a 1ª ocorrência).")
        df_coord = df_coord.drop_duplicates(subset=["Nome da base"], keep="first")

    antes_merge = len(df)
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
    depois_merge = len(df)
    print("👥 Coordenadores vinculados.")
    if depois_merge != antes_merge:
        print(f"⚠️ Linhas antes merge: {antes_merge} | depois merge: {depois_merge} (verifique duplicidade no coordenador).")

    # custo
    if "Valor a pagar (yuan)" in df.columns:
        if PARSE_DEBUG:
            exemplos = df["Valor a pagar (yuan)"].dropna().astype(str).head(10).tolist()
            print("🔎 Exemplos crus (Valor a pagar):", exemplos)

        df["Custo_R$"] = to_float_safe(df["Valor a pagar (yuan)"])

        if PARSE_DEBUG:
            conv = df["Custo_R$"].head(10).tolist()
            print("🔎 Convertidos (float):", conv)
            print("🔎 Max Custo_R$:", float(df["Custo_R$"].max()))
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
                indicador_nome=INDICADOR_NOME,
                total_pedidos=total_pedidos,
                custo_total=custo_total,
                total_bases=total_bases,
                rows_all=rows_all,
                out_dir=IMAGENS_DIR,
                rows_per_page=ROWS_PER_PAGE,
            )

            total_pages = len(img_paths)

            for idx, img_path in enumerate(img_paths, start=1):
                img_key = upload_image_get_key(img_path)
                page_label = f"Página {idx}/{total_pages}"

                resp = enviar_card_somente_nome_com_imagem(
                    webhook=webhook_coord,
                    nome_coord=coord,
                    indicador_nome=INDICADOR_NOME,
                    total_pedidos=total_pedidos,
                    custo_total=custo_total,
                    bases_avaliadas=total_bases,
                    data_humana=DATA_HUMANA,
                    img_key=img_key,
                    page_label=page_label,
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
