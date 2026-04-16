# =========================================================
# ARQUIVO COMPLETO — INDICADOR NA IMAGEM + CARD (CORES J&T)
# Layout novo no mesmo estilo da imagem anterior
# =========================================================
# -*- coding: utf-8 -*-

# =========================
# BLOCO 1/3 — IMPORTS + CONFIG
# =========================
import os
import json
import time
import hashlib
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

# =========================
# CONFIG
# =========================

# 🏷️ Nome do indicador (vai aparecer na IMAGEM e no CARD)
INDICADOR_NOME = "Multas 5+ dias — Bases por quantidade (Δ vs relatório anterior)"

COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/67153e53-3623-4e5b-9560-0cb6f0ef609e",
    "João Melo GO": "https://open.feishu.cn/open-apis/bot/v2/hook/5c2bb460-1971-4770-9b37-98b6e4ba3cd9",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/431d1984-6f09-49a3-904b-cc01a7889608",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/20a61c63-6db7-4e83-9e44-ae6b545495cc",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/16414836-5020-49bd-b3d3-ded4f34878ab",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/4e5333c7-bc23-4a50-b647-56b952381bce",
    "Emerson Silva - AM e RR": "https://open.feishu.cn/open-apis/bot/v2/hook/7616db98-d225-4b46-915e-dc73bde24284",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/e502bc10-3cb3-4b46-872e-eb73ef1c5ee0",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/db18d309-8f26-41b5-b911-1a9f27449c83",
    "Ana Cunha": "https://open.feishu.cn/open-apis/bot/v2/hook/ffc8420d-a317-498a-b3b5-d83432311677",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/5ea0a62d-3e94-47d0-8914-59f722feff5b",
}

REPORTS_FOLDER_PATH = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Sem_Movimentação - LM"
HASH_FILE = os.path.join(os.path.dirname(__file__), "../Demais/ultimo_relatorio.json")

LINK_RELATORIO = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/IgBjYhTX-imSQKw7j2RKp9M6AZjMTBT-NFfPY-u-WPCgzkU?e=tfVi3V"
)

# Onde salvar as imagens geradas
IMAGES_OUT_DIR = os.path.join(os.path.dirname(__file__), "../imagens_bases")
os.makedirs(IMAGES_OUT_DIR, exist_ok=True)

# Paginação da imagem
ROWS_PER_PAGE = 22

# Feishu OpenAPI
FEISHU_BASE_DOMAIN = os.getenv("FEISHU_BASE_DOMAIN", "https://open.feishu.cn").rstrip("/")
APP_ID = os.getenv("FEISHU_APP_ID", "cli_a906d2d682f8dbd8").strip()
APP_SECRET = os.getenv("FEISHU_APP_SECRET", "Fzh1cr6K55a3oQUBV9wCZd6AWiZH5ONw").strip()

# =========================
# 🎨 PALETA J&T
# =========================
JT_RED_MAIN = (227, 6, 19)      # #E30613
JT_RED_SOFT = (196, 39, 46)     # #C4272E
JT_BG_GRAY  = (242, 242, 242)   # #F2F2F2
JT_TEXT     = (51, 51, 51)      # #333333
JT_WHITE    = (255, 255, 255)   # #FFFFFF

JT_STROKE   = (210, 210, 210)
JT_MUTED    = (120, 120, 120)
JT_ROW_ALT  = (248, 248, 248)

GOOD_GREEN  = (16, 185, 129)

# =========================
# BLOCO 2/3 — UTIL + FEISHU TOKEN/UPLOAD + IMAGENS (PIL)
# =========================

def format_currency_brl(value: float) -> str:
    try:
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def format_int_br(value: int) -> str:
    try:
        return f"{int(value):,}".replace(",", ".")
    except Exception:
        return "0"


def calcular_hash_md5(file_path: str) -> str:
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def carregar_snapshot_antigo() -> Optional[Dict[str, Any]]:
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def salvar_snapshot(snapshot: Dict[str, Any]) -> None:
    with open(HASH_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=4)


def requests_post_with_retry(
    url: str,
    *,
    json_payload: dict,
    timeout: int = 15,
    retries: int = 5,
    backoff: float = 1.6,
) -> Tuple[bool, str]:
    """Retorna (ok, response_text)"""
    last_err = ""
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, json=json_payload, timeout=timeout)
            if r.status_code == 200:
                return True, r.text
            last_err = f"HTTP {r.status_code}: {r.text}"
        except Exception as e:
            last_err = repr(e)

        sleep_s = (backoff ** (attempt - 1))
        time.sleep(min(sleep_s, 12))

    return False, last_err


def safe_filename(text: str) -> str:
    if text is None:
        return "SEM_NOME"
    ok = []
    for c in str(text).strip():
        if c.isalnum() or c in (" ", "-", "_"):
            ok.append(c)
    s = "".join(ok).strip().replace(" ", "_")
    return s or "SEM_NOME"


# =========================
# FEISHU TOKEN + UPLOAD IMAGE
# =========================

def feishu_enabled() -> bool:
    return bool(APP_ID and APP_SECRET)


def get_tenant_access_token() -> str:
    url = f"{FEISHU_BASE_DOMAIN}/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}

    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()

    if data.get("code") != 0:
        raise RuntimeError(f"Falha token | code={data.get('code')} msg={data.get('msg')} raw={data}")

    return data["tenant_access_token"]


def upload_image_get_key(image_path: str, token: str, image_type: str = "message") -> str:
    url = f"{FEISHU_BASE_DOMAIN}/open-apis/im/v1/images"
    headers = {"Authorization": f"Bearer {token}"}

    with open(image_path, "rb") as f:
        files = {"image": (os.path.basename(image_path), f)}
        data = {"image_type": image_type}
        r = requests.post(url, headers=headers, data=data, files=files, timeout=30)

    r.raise_for_status()
    resp = r.json()

    if resp.get("code") != 0:
        code = resp.get("code")
        msg = resp.get("msg")
        raise RuntimeError(f"Upload falhou | code={code} msg={msg} raw={resp}")

    return resp["data"]["image_key"]


# =========================
# FONTES
# =========================

def _load_font(size: int, bold: bool = False):
    from PIL import ImageFont
    candidates = []
    if bold:
        candidates += [
            r"C:\Windows\Fonts\arialbd.ttf",
            r"C:\Windows\Fonts\calibrib.ttf",
            r"C:\Windows\Fonts\segoeuib.ttf",
        ]
    candidates += [
        r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\calibri.ttf",
        r"C:\Windows\Fonts\segoeui.ttf",
    ]

    for p in candidates:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size=size)
            except Exception:
                pass
    return ImageFont.load_default()


# =========================
# IMAGEM NOVA — MESMO ESTILO DO OUTRO RELATÓRIO
# =========================

def gerar_imagens_bases(
    coordenador: str,
    indicador_nome: str,
    total_pacotes: int,
    total_multa: float,
    total_bases: int,
    bases_atuais: Dict[str, int],
    bases_antigas: Dict[str, int],
    out_dir: str,
    rows_per_page: int = ROWS_PER_PAGE,
) -> List[str]:
    """
    Gera imagens no mesmo padrão visual do relatório anterior:
    - fundo vermelho J&T
    - header centralizado
    - painel branco arredondado
    - cards de destaque
    - tabela com cabeçalho vermelho
    - coluna Δ mantida
    """
    from PIL import Image, ImageDraw

    os.makedirs(out_dir, exist_ok=True)

    def rr(draw: ImageDraw.ImageDraw, xy, r, fill, outline=None, width=1):
        try:
            draw.rounded_rectangle(xy, radius=r, fill=fill, outline=outline, width=width)
        except Exception:
            draw.rectangle(xy, fill=fill, outline=outline, width=width)

    def _measure(draw: ImageDraw.ImageDraw, text: str, font) -> Tuple[int, int]:
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

    def _ellipsize(draw: ImageDraw.ImageDraw, text: str, font, max_w: int) -> str:
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
            f = _load_font(size, bold=bold)
            if _measure(draw, text, f)[0] <= max_w:
                return f
            size -= 1
        return _load_font(min_size, bold=bold)

    def _wrap_lines(
        draw: ImageDraw.ImageDraw,
        text: str,
        font,
        max_w: int,
        max_lines: int = 2,
    ) -> List[str]:
        text = (text or "").strip()
        if not text:
            return [""]

        words = text.split()
        lines: List[str] = []
        cur = ""

        for word in words:
            cand = (cur + " " + word).strip() if cur else word
            if _measure(draw, cand, font)[0] <= max_w:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                cur = word
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
        font,
        fill,
        max_w: int,
        center_x: int,
    ) -> int:
        txt = _ellipsize(draw, text, font, max_w)
        w, h = _measure(draw, txt, font)
        draw.text((center_x - w // 2, y), txt, fill=fill, font=font)
        return y + h

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

        label_font = _load_font(19, bold=False)
        value_font = _load_font(30 if highlight else 27, bold=True)

        inner_w = (x2 - x1) - 24
        label = _ellipsize(draw, label, label_font, inner_w)
        value = _ellipsize(draw, value, value_font, inner_w)

        lw, lh = _measure(draw, label, label_font)
        vw, vh = _measure(draw, value, value_font)

        draw.text((x1 + ((x2 - x1) - lw) // 2, y1 + 16), label, fill=label_fill, font=label_font)
        draw.text((x1 + ((x2 - x1) - vw) // 2, y1 + 48), value, fill=value_fill, font=value_font)

    coordenador = (coordenador or "").strip() or "Sem Coordenador"
    indicador_nome = (indicador_nome or "").strip() or "Indicador"

    items = sorted((bases_atuais or {}).items(), key=lambda x: x[1], reverse=True)
    if not items:
        return []

    total_pages = (len(items) + rows_per_page - 1) // rows_per_page
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")

    RED_BG = JT_RED_MAIN
    RED_HDR = (235, 0, 0)
    RED_STRONG = (212, 0, 0)
    RED_SOFT_BG = (255, 238, 240)
    WHITE = JT_WHITE
    PANEL_BG = JT_WHITE
    ROW_A = (252, 252, 252)
    ROW_B = (245, 245, 245)
    GRID = (226, 226, 226)
    TEXT = JT_TEXT
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

    out_paths: List[str] = []

    for page_idx in range(1, total_pages + 1):
        chunk = items[(page_idx - 1) * rows_per_page: page_idx * rows_per_page]
        row_count = len(chunk)

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
        f_logo_big = _load_font(42, bold=True)
        f_logo_small = _load_font(20, bold=True)
        draw.text((22, 24), "J&T", fill=WHITE, font=f_logo_big)
        draw.text((106, 43), "EXPRESS", fill=WHITE, font=f_logo_small)

        max_center_w = W - 380

        titulo = f"Relatório de {indicador_nome}"
        subtitulo = f"Coordenador: {coordenador}"
        linha_info = f"Atualizado: {ts}   •   Página {page_idx}/{total_pages}"

        f_title = _fit_font(draw, titulo, start_size=35, min_size=24, bold=True, max_w=max_center_w)
        f_sub = _fit_font(draw, subtitulo, start_size=26, min_size=18, bold=True, max_w=max_center_w)
        f_meta = _load_font(18, bold=False)

        y = 18
        y = _draw_centered_line(draw, titulo, y, f_title, WHITE, max_center_w, center_x) + 5
        y = _draw_centered_line(draw, subtitulo, y, f_sub, WHITE, max_center_w, center_x) + 7

        meta_lines = _wrap_lines(draw, linha_info, f_meta, max_center_w, max_lines=2)
        for line_txt in meta_lines:
            y = _draw_centered_line(draw, line_txt, y, f_meta, WHITE, max_center_w, center_x) + 2

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
            c1_x1, c_y1, c1_x2, c_y2,
            "Qtd de Pacotes",
            format_int_br(total_pacotes),
            fill=WHITE,
            label_fill=MUTED,
            value_fill=RED_STRONG,
            border=(230, 230, 230),
            highlight=True,
        )

        draw_metric_card(
            draw,
            c2_x1, c_y1, c2_x2, c_y2,
            "Bases Avaliadas",
            format_int_br(total_bases),
            fill=WHITE,
            label_fill=MUTED,
            value_fill=TEXT,
            border=(230, 230, 230),
            highlight=False,
        )

        draw_metric_card(
            draw,
            c3_x1, c_y1, c3_x2, c_y2,
            "Multa Atual",
            format_currency_brl(total_multa),
            fill=RED_SOFT_BG,
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
        w_diff = 260
        w_base = inner_w - w_rank - w_qtd - w_diff

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

        f_th = _load_font(18, bold=True)

        headers = [
            ("Rank", col1_x1, col1_x2),
            ("Base de Entrega", col2_x1, col2_x2),
            ("Qtd", col3_x1, col3_x2),
            ("Δ vs anterior", col4_x1, col4_x2),
        ]

        for txt, x1, x2 in headers:
            fw, fh = _measure(draw, txt, f_th)
            tx = x1 + ((x2 - x1) - fw) // 2
            ty = table_y1 + ((TABLE_HEAD_H - fh) // 2)
            draw.text((tx, ty), txt, fill=WHITE, font=f_th)

        f_row = _load_font(18, bold=False)
        start_y = table_y1 + TABLE_HEAD_H

        for i, (base, qtd) in enumerate(chunk, start=1):
            y1 = start_y + ((i - 1) * ROW_H)
            y2 = y1 + ROW_H

            fill_row = ROW_A if i % 2 == 1 else ROW_B
            draw.rectangle((inner_x1, y1, inner_x2, y2), fill=fill_row)

            draw.line((inner_x1, y2, inner_x2, y2), fill=GRID, width=1)
            for x in [col1_x2, col2_x2, col3_x2]:
                draw.line((x, y1, x, y2), fill=GRID, width=1)

            rank_txt = f"{((page_idx - 1) * rows_per_page) + i}"
            base_txt = _ellipsize(draw, str(base), f_row, (w_base - 28))
            qtd_txt = format_int_br(int(qtd))

            diff = int(qtd) - int((bases_antigas or {}).get(base, 0))
            if diff > 0:
                diff_txt = f"+{diff}"
                diff_color = JT_RED_MAIN
            elif diff < 0:
                diff_txt = f"{diff}"
                diff_color = GOOD_GREEN
            else:
                diff_txt = "0"
                diff_color = MUTED

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

            dw, dh = _measure(draw, diff_txt, f_row)
            draw.text(
                (col4_x1 + ((w_diff - dw) // 2), y1 + ((ROW_H - dh) // 2)),
                diff_txt,
                fill=diff_color,
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
        f_footer = _load_font(15, bold=False)
        footer_y = panel_y2 + 16

        footer_txt_1 = f"Relatório completo: {LINK_RELATORIO}"
        footer_txt_2 = f"J&T Express • {indicador_nome}"

        footer_txt_1 = _ellipsize(draw, footer_txt_1, f_footer, W - 120)

        w1, h1 = _measure(draw, footer_txt_1, f_footer)
        w2, h2 = _measure(draw, footer_txt_2, f_footer)

        draw.text((center_x - w1 // 2, footer_y), footer_txt_1, fill=WHITE, font=f_footer)
        draw.text((center_x - w2 // 2, footer_y + h1 + 4), footer_txt_2, fill=WHITE, font=f_footer)

        fname = f"bases_{safe_filename(coordenador)}_p{page_idx:02d}.png"
        out_path = os.path.join(out_dir, fname)
        img.save(out_path, "PNG")
        out_paths.append(out_path)

    return out_paths


# =========================
# BLOCO 3/3 — PROCESSAR RELATÓRIO + SNAPSHOT + CARD + MAIN
# =========================

def process_report_file(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.strip()
    df.rename(columns={"运单号": "Remessa", "Coordenador": "Coordenadores"}, inplace=True)
    return df


def gerar_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    snapshot = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "coordenadores": {}}
    for coord in df["Coordenadores"].dropna().unique():
        dfc = df[df["Coordenadores"] == coord]
        total_pacotes = dfc["Remessa"].nunique()
        total_multa = dfc["Multa (R$)"].sum() if "Multa (R$)" in dfc.columns else 0

        bases = (
            dfc.groupby("Unidade responsável")["Remessa"]
            .nunique()
            .sort_values(ascending=False)
            .to_dict()
        )

        snapshot["coordenadores"][coord] = {
            "total_pacotes": int(total_pacotes),
            "total_multa": float(total_multa),
            "bases": {str(k): int(v) for k, v in bases.items()},
        }
    return snapshot


def comparar_coordenador(snapshot_atual, snapshot_antigo, coord) -> Dict[str, Any]:
    atual = snapshot_atual["coordenadores"].get(coord, {})
    antigo = snapshot_antigo["coordenadores"].get(coord, {}) if snapshot_antigo else {}

    total_pacotes = int(atual.get("total_pacotes", 0))
    total_multa = float(atual.get("total_multa", 0))
    bases_atuais = atual.get("bases", {}) or {}
    bases_antigas = antigo.get("bases", {}) or {}

    diff_pacotes = total_pacotes - int(antigo.get("total_pacotes", 0))
    diff_multa = total_multa - float(antigo.get("total_multa", 0))

    if diff_pacotes > 0:
        var_pacotes = f"📈 Aumentou {diff_pacotes} pedidos"
    elif diff_pacotes < 0:
        var_pacotes = f"📉 Diminuiu {abs(diff_pacotes)} pedidos"
    else:
        var_pacotes = "➖ Sem alteração"

    if diff_multa > 0:
        var_multa = f"📈 Aumentou {format_currency_brl(diff_multa)}"
    elif diff_multa < 0:
        var_multa = f"📉 Diminuiu {format_currency_brl(abs(diff_multa))}"
    else:
        var_multa = "➖ Sem alteração"

    return {
        "total_pacotes": total_pacotes,
        "total_multa": total_multa,
        "var_pacotes": var_pacotes,
        "var_multa": var_multa,
        "bases_atuais": bases_atuais,
        "bases_antigas": bases_antigas,
    }


# =========================
# CARD (COM IMAGEM) — + INDICADOR NO CORPO
# =========================

def create_feishu_payload(
    coordenador: str,
    data: Dict[str, Any],
    image_keys: Optional[List[str]] = None,
    indicador_nome: str = "",
) -> Dict[str, Any]:
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    indicador_nome = (indicador_nome or "").strip() or "Indicador"

    elements: List[Dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": f"📌 **Indicador:** {indicador_nome}" }},
        {"tag": "hr"},
        {
            "tag": "div",
            "fields": [
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Atualizado:**\n{ts}\n\n"
                            f"**Qtd de Pacotes:**\n{data['total_pacotes']}\n"
                            f"**Variação:**\n{data['var_pacotes']}"
                        ),
                    },
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Multa Atual:**\n{format_currency_brl(data['total_multa'])}\n"
                            f"**Variação:**\n{data['var_multa']}"
                        ),
                    },
                },
            ],
        },
        {"tag": "hr"},
    ]

    if image_keys:
        for k in image_keys:
            elements.append(
                {
                    "tag": "img",
                    "img_key": k,
                    "alt": {"tag": "plain_text", "content": "Tabela de bases"},
                    "mode": "fit_horizontal",
                    "preview": True,
                }
            )
            elements.append({"tag": "hr"})
    else:
        bases = data.get("bases_atuais", {}) or {}
        top = sorted(bases.items(), key=lambda x: x[1], reverse=True)[:5]
        txt = (
            "**Bases (Top 5 por qtd):**\n" + "\n".join([f"- **{b}**: {q}" for b, q in top])
            if top
            else "Sem bases."
        )
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": txt}})
        elements.append({"tag": "hr"})

    elements.append(
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "📎 Abrir Relatório Completo"},
                    "url": LINK_RELATORIO,
                    "type": "primary",
                }
            ],
        }
    )

    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"{coordenador}"},
                "template": "red",
            },
            "elements": elements,
        },
    }


def send_to_feishu(webhook_url: str, payload: Dict[str, Any]) -> None:
    ok, err = requests_post_with_retry(webhook_url, json_payload=payload, timeout=15, retries=5)
    if ok:
        print(f"✅ Enviado → {webhook_url[:55]}...")
    else:
        print(f"❌ Erro ao enviar → {webhook_url[:55]}... | {err}")


# =========================
# MAIN
# =========================

def find_latest_report(folder: str) -> Optional[str]:
    if not os.path.isdir(folder):
        return None

    arquivos = [
        f for f in os.listdir(folder)
        if f.lower().endswith(".xlsx")
        and ("5+ dias" in f.lower())
        and (not f.startswith("~"))
    ]
    if not arquivos:
        return None

    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
    return os.path.join(folder, arquivos[0])


def run_main_task():
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Procurando relatórios em {REPORTS_FOLDER_PATH}")
    full_path = find_latest_report(REPORTS_FOLDER_PATH)
    if not full_path:
        print("⚠️ Nenhum relatório encontrado.")
        return

    file_name = os.path.basename(full_path)
    print(f"📄 Último relatório detectado: {file_name}")

    file_hash = calcular_hash_md5(full_path)
    snapshot_antigo = carregar_snapshot_antigo()

    print("⚠️ Reenvio forçado do relatório atual.")

    df = process_report_file(full_path)
    snapshot_atual = gerar_snapshot(df)
    snapshot_atual["file_hash"] = file_hash

    token = ""
    if feishu_enabled():
        try:
            token = get_tenant_access_token()
        except Exception as e:
            print(f"⚠️ Falha ao obter tenant_access_token (vai enviar sem imagem): {e}")
            token = ""
    else:
        print("⚠️ FEISHU_APP_ID/FEISHU_APP_SECRET não definidos. Vai enviar SEM imagem.")

    for coord, webhook in COORDENADOR_WEBHOOKS.items():
        if coord not in snapshot_atual["coordenadores"]:
            continue

        data = comparar_coordenador(snapshot_atual, snapshot_antigo, coord)

        image_keys: List[str] = []
        try:
            img_paths = gerar_imagens_bases(
                coordenador=coord,
                indicador_nome=INDICADOR_NOME,
                total_pacotes=data["total_pacotes"],
                total_multa=data["total_multa"],
                total_bases=len(data["bases_atuais"]),
                bases_atuais=data["bases_atuais"],
                bases_antigas=data["bases_antigas"],
                out_dir=IMAGES_OUT_DIR,
                rows_per_page=ROWS_PER_PAGE,
            )

            if img_paths and token:
                for p in img_paths:
                    try:
                        k = upload_image_get_key(p, token=token, image_type="message")
                        image_keys.append(k)
                    except Exception as e:
                        msg = str(e)
                        print(f"⚠️ Upload imagem falhou ({coord}) — enviando sem imagem. Motivo: {msg}")
                        if "234007" in msg:
                            print("   ➜ Habilite o recurso BOT no app: Add Features → Bot → Add, publique e instale no tenant.")
                        image_keys = []
                        break
            else:
                image_keys = []

        except Exception as e:
            print(f"⚠️ Falha ao gerar imagens ({coord}) — enviando sem imagem: {e}")
            image_keys = []

        payload = create_feishu_payload(
            coord,
            data,
            image_keys=image_keys,
            indicador_nome=INDICADOR_NOME,
        )
        send_to_feishu(webhook, payload)
        time.sleep(1)

    salvar_snapshot(snapshot_atual)
    print(f"📂 Relatório mantido em {REPORTS_FOLDER_PATH}")
    print("✅ Processo concluído!")


if __name__ == "__main__":
    run_main_task()