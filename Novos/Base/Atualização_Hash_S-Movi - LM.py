# =========================================================
# ARQUIVO COMPLETO — INDICADOR NA IMAGEM + CARD (CORES J&T)
# Cole TUDO no mesmo .py
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
INDICADOR_NOME = "Multas 5+ dias — Bases por quantidade (Δ vs relatório anterior)"  # <-- ajuste aqui

COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/1d9bbacf-79ed-4eb3-8046-26d7480893c3",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/5c2bb460-1971-4770-9b37-98b6e4ba3cd9",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/914ce9f9-35ab-4869-860f-d2bef7d933fb",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/20a61c63-6db7-4e83-9e44-ae6b545495cc",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/16414836-5020-49bd-b3d3-ded4f34878ab",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/62cd648c-ecd5-406a-903d-b596944c1919",
    "Fabio Souza": "https://open.feishu.cn/open-apis/bot/v2/hook/7616db98-d225-4b46-915e-dc73bde24284",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/e502bc10-3cb3-4b46-872e-eb73ef1c5ee0",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/db18d309-8f26-41b5-b911-1a9f27449c83",
    "Ana Cunha": "https://open.feishu.cn/open-apis/bot/v2/hook/ffc8420d-a317-498a-b3b5-d83432311677",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/5ea0a62d-3e94-47d0-8914-59f722feff5b",
}

REPORTS_FOLDER_PATH = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Sem_Movimentação - LM"
HASH_FILE = os.path.join(os.path.dirname(__file__), "../ultimo_relatorio.json")

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
# 🎨 PALETA J&T (HEX -> RGB)
# =========================
JT_RED_MAIN = (227, 6, 19)      # #E30613
JT_RED_SOFT = (196, 39, 46)     # #C4272E
JT_BG_GRAY  = (242, 242, 242)   # #F2F2F2
JT_TEXT     = (51, 51, 51)      # #333333
JT_WHITE    = (255, 255, 255)   # #FFFFFF

JT_STROKE   = (210, 210, 210)
JT_MUTED    = (120, 120, 120)
JT_ROW_ALT  = (248, 248, 248)

# (Opcional) cor “boa” para queda (não faz parte da paleta, mas melhora leitura)
GOOD_GREEN  = (16, 185, 129)
# =========================
# BLOCO 2/3 — UTIL + FEISHU TOKEN/UPLOAD + IMAGENS (PIL)
# =========================

def format_currency_brl(value: float) -> str:
    try:
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


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
    """Evita problemas com caracteres especiais no nome do arquivo."""
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
    """POST /open-apis/auth/v3/tenant_access_token/internal"""
    url = f"{FEISHU_BASE_DOMAIN}/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}

    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()

    if data.get("code") != 0:
        raise RuntimeError(f"Falha token | code={data.get('code')} msg={data.get('msg')} raw={data}")

    return data["tenant_access_token"]


def upload_image_get_key(image_path: str, token: str, image_type: str = "message") -> str:
    """POST /open-apis/im/v1/images (multipart)"""
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
# GERAR IMAGEM “BONITINHA” (PILLOW) — SEM BARRAS
# + INDICADOR NO HEADER (CORES J&T)
# =========================

def _load_font(size: int, bold: bool = False):
    from PIL import ImageFont
    candidates = []
    if bold:
        candidates += [r"C:\Windows\Fonts\arialbd.ttf", r"C:\Windows\Fonts\calibrib.ttf", r"C:\Windows\Fonts\segoeuib.ttf"]
    candidates += [r"C:\Windows\Fonts\arial.ttf", r"C:\Windows\Fonts\calibri.ttf", r"C:\Windows\Fonts\segoeui.ttf"]

    for p in candidates:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size=size)
            except Exception:
                pass
    return ImageFont.load_default()


def gerar_imagens_bases(
    coordenador: str,
    indicador_nome: str,
    bases_atuais: Dict[str, int],
    bases_antigas: Dict[str, int],
    out_dir: str,
    rows_per_page: int = ROWS_PER_PAGE,
) -> List[str]:
    """
    Retorna lista de caminhos de imagens (paginadas) com todas as bases, ordenadas por qtd desc.
    ✅ Mostra INDICADOR no header
    ✅ CORES J&T
    ✅ NÃO CORTA texto (auto-fit + wrap + reticências)
    """
    from PIL import Image, ImageDraw

    indicador_nome = (indicador_nome or "").strip() or "Indicador"

    items = sorted((bases_atuais or {}).items(), key=lambda x: x[1], reverse=True)
    if not items:
        return []

    total_pages = (len(items) + rows_per_page - 1) // rows_per_page
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")

    # ===== Tema J&T =====
    bg = JT_BG_GRAY
    card = JT_WHITE
    line = JT_STROKE
    text = JT_TEXT
    muted = JT_MUTED
    row_alt = JT_ROW_ALT

    # layout
    W = 1400
    pad = 36

    # ✅ AUMENTEI para caber indicador em 2 linhas
    header_h = 190

    table_top = header_h + 26
    row_h = 44
    head_h = 44
    footer_h = 24

    out_paths: List[str] = []

    def rr(draw: ImageDraw.ImageDraw, xy, r, fill, outline=None, width=1):
        try:
            draw.rounded_rectangle(xy, radius=r, fill=fill, outline=outline, width=width)
        except Exception:
            draw.rectangle(xy, fill=fill, outline=outline, width=width)

    # ===== Helpers: medir / ellipsis / auto-fit / wrap =====
    def _measure(draw: ImageDraw.ImageDraw, text_: str, font_) -> Tuple[int, int]:
        text_ = text_ or ""
        try:
            b = draw.textbbox((0, 0), text_, font=font_)
            return int(b[2] - b[0]), int(b[3] - b[1])
        except Exception:
            try:
                w_, h_ = draw.textsize(text_, font=font_)  # type: ignore[attr-defined]
                return int(w_), int(h_)
            except Exception:
                return int(len(text_) * 8), 18

    def _ellipsize(draw: ImageDraw.ImageDraw, text_: str, font_, max_w: int) -> str:
        text_ = text_ or ""
        if _measure(draw, text_, font_)[0] <= max_w:
            return text_
        ell = "…"
        lo, hi = 0, len(text_)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = (text_[:mid].rstrip() + ell)
            if _measure(draw, cand, font_)[0] <= max_w:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def _fit_font(draw: ImageDraw.ImageDraw, text_: str, start_size: int, min_size: int, bold: bool, max_w: int):
        size = start_size
        while size >= min_size:
            f = _load_font(size, bold=bold)
            if _measure(draw, text_, f)[0] <= max_w:
                return f
            size -= 1
        return _load_font(min_size, bold=bold)

    def _wrap_lines(draw: ImageDraw.ImageDraw, text_: str, font_, max_w: int, max_lines: int = 2) -> List[str]:
        text_ = (text_ or "").strip()
        if not text_:
            return [""]

        words = text_.split()
        lines: List[str] = []
        cur = ""

        for w_ in words:
            cand = (cur + " " + w_).strip() if cur else w_
            if _measure(draw, cand, font_)[0] <= max_w:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                cur = w_
                if len(lines) >= max_lines - 1:
                    break

        if cur:
            lines.append(cur)

        if len(lines) > max_lines:
            lines = lines[:max_lines]

        if lines:
            lines[-1] = _ellipsize(draw, lines[-1], font_, max_w)

        return lines

    for page in range(1, total_pages + 1):
        chunk = items[(page - 1) * rows_per_page: page * rows_per_page]
        H = table_top + head_h + (len(chunk) * row_h) + footer_h + pad

        img = Image.new("RGB", (W, H), bg)
        draw = ImageDraw.Draw(img)

        # card externo
        rr(draw, (18, 18, W - 18, H - 18), 20, card, outline=line, width=2)

        # header gradiente (vermelho soft -> vermelho main)
        hx1, hy1 = 22, 22
        hx2, hy2 = W - 22, 22 + header_h
        rr(draw, (hx1, hy1, hx2, hy2), 18, JT_RED_SOFT, outline=None, width=1)
        for i in range(hy2 - hy1):
            t = i / max(1, (hy2 - hy1))
            c = (
                int(JT_RED_SOFT[0] + (JT_RED_MAIN[0] - JT_RED_SOFT[0]) * t),
                int(JT_RED_SOFT[1] + (JT_RED_MAIN[1] - JT_RED_SOFT[1]) * t),
                int(JT_RED_SOFT[2] + (JT_RED_MAIN[2] - JT_RED_SOFT[2]) * t),
            )
            draw.line([(hx1 + 1, hy1 + i), (hx2 - 1, hy1 + i)], fill=c)

        # ✅ HEADER SEM CORTE
        left = pad
        inner_w = (hx2 - left) - 24  # margem direita interna
        y = hy1 + 14

        title = (coordenador or "").strip()
        font_title = _fit_font(draw, title, start_size=32, min_size=18, bold=True, max_w=inner_w)
        title_draw = _ellipsize(draw, title, font_title, inner_w)
        draw.text((left, y), title_draw, fill=JT_WHITE, font=font_title)
        y += _measure(draw, title_draw, font_title)[1] + 8

        ind_full = f"Indicador: {indicador_nome}".strip()
        font_ind = _fit_font(draw, ind_full, start_size=18, min_size=13, bold=True, max_w=inner_w)
        ind_lines = _wrap_lines(draw, ind_full, font_ind, inner_w, max_lines=2)
        for line_txt in ind_lines:
            draw.text((left, y), line_txt, fill=JT_WHITE, font=font_ind)
            y += _measure(draw, line_txt, font_ind)[1] + 2
        y += 6

        sub_full = f"Atualizado: {ts}  •  Página {page}/{total_pages}"
        font_sub = _fit_font(draw, sub_full, start_size=18, min_size=12, bold=False, max_w=inner_w)
        sub_draw = _ellipsize(draw, sub_full, font_sub, inner_w)
        draw.text((left, y), sub_draw, fill=JT_WHITE, font=font_sub)

        # table header
        x0 = pad
        yth = table_top
        col_base = 720
        col_qtd = 200
        col_diff = 200

        font_head = _load_font(18, bold=True)
        font_row = _load_font(18, bold=False)

        draw.text((x0, yth), "Base", fill=muted, font=font_head)
        draw.text((x0 + col_base, yth), "Qtd", fill=muted, font=font_head)
        draw.text((x0 + col_base + col_qtd, yth), "Δ", fill=muted, font=font_head)
        draw.line((pad, yth + 34, W - pad, yth + 34), fill=line, width=2)

        # rows
        y_row = yth + head_h
        for idx, (base, qtd) in enumerate(chunk, 1):
            if idx % 2 == 0:
                rr(
                    draw,
                    (pad - 10, y_row - 6, W - pad + 10, y_row + row_h - 6),
                    12,
                    row_alt,
                    outline=None,
                    width=1
                )

            diff = int(qtd) - int((bases_antigas or {}).get(base, 0))
            if diff > 0:
                diff_txt = f"+{diff}"
                diff_color = JT_RED_MAIN  # aumento = vermelho
            elif diff < 0:
                diff_txt = f"{diff}"
                diff_color = GOOD_GREEN   # queda = verde (opcional)
            else:
                diff_txt = "0"
                diff_color = muted

            base_txt = str(base)
            if len(base_txt) > 40:
                base_txt = base_txt[:37] + "..."

            draw.text((x0, y_row + 8), base_txt, fill=text, font=font_row)
            draw.text((x0 + col_base, y_row + 8), f"{int(qtd)}", fill=text, font=font_row)
            draw.text((x0 + col_base + col_qtd, y_row + 8), diff_txt, fill=diff_color, font=font_row)

            y_row += row_h

        # footer
        draw.text((pad, H - 44), "Bases (todas) — ordenado por quantidade", fill=muted, font=font_sub)

        fname = f"bases_{safe_filename(coordenador)}_p{page:02d}.png"
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

    # Imagens (páginas)
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
                "template": "red",  # ✅ J&T (antes: green)
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

    # token (uma vez)
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

        # gerar imagens (todas as bases) — passa indicador
        image_keys: List[str] = []
        try:
            img_paths = gerar_imagens_bases(
                coordenador=coord,
                indicador_nome=INDICADOR_NOME,
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
