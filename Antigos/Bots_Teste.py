import requests
import json
from datetime import datetime, timedelta
import time
import hmac
import hashlib
import base64
import os
import pandas as pd
from typing import Dict, Any, Optional, List

# --- Optional Dependency: Plyer for Desktop Notifications ---
try:
    from plyer import notification
    PLYER_AVAILABLE = True
except ImportError:
    PLYER_AVAILABLE = False
    print("WARNING: 'plyer' library not found. Desktop notifications are disabled.")

# --- CONFIGURATION BLOCK ---
WEBHOOKS: List[Dict[str, Optional[str]]] = [
    {
        "name": "Indicadores Operacionais | Qualidade e Redes",
        "url": os.getenv("FEISHU_WEBHOOK_URL_1", "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"),
        "secret": os.getenv("FEISHU_SECRET_KEY_1", "576mFMGpGywoOTyFqvaGOeMKgcUANnDp")
    }
]

REPORTS_FOLDER_PATH = os.getenv("REPORTS_FOLDER_PATH", r"C:\Users\JT-244\Desktop\Testes\Teste Base\Sem Movimentação")
ARCHIVE_FOLDER_PATH = os.path.join(REPORTS_FOLDER_PATH, "Arquivo Morto")
REPORTS_SHAREABLE_LINK = os.getenv("REPORTS_SHAREABLE_LINK", "LINK_DA_PASTA_COMPARTILHADA")

# --- HELPER FUNCTIONS ---
def send_desktop_notification(title: str, message: str):
    if not PLYER_AVAILABLE:
        return
    try:
        notification.notify(title=title, message=message, app_name='Reports Scheduler', timeout=15)
    except Exception as e:
        print(f"WARNING: Failed to send desktop notification. Error: {e}")

def format_currency_brl(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def generate_feishu_signature(secret: str, timestamp: int) -> str:
    string_to_sign = f'{timestamp}\n{secret}'
    hmac_code = hmac.new(string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
    return base64.b64encode(hmac_code).decode('utf-8')

def generate_fines_summary_by_branch(df: pd.DataFrame) -> str:
    required_columns = ['Filial', 'Unidade responsável', 'Multa (R$)']
    if not all(col in df.columns for col in required_columns):
        return ""
    df_filtered = df[df['Filial'] != 'Não encontrada'].copy()
    if df_filtered.empty or df_filtered['Multa (R$)'].sum() == 0:
        return ""
    branch_summary = df_filtered.groupby('Filial').agg(total_fine=('Multa (R$)', 'sum'), order_count=('Multa (R$)', 'size'))
    summary_text = "\n\n--- **Resumo de Multas por Filial** ---"
    for branch, data in branch_summary.sort_values(by='total_fine', ascending=False).iterrows():
        total_fine = data['total_fine']
        order_count = data['order_count']
        summary_text += f"\n\n**📍 Filial: {branch}**\n"
        summary_text += f"Qtd de Pedidos: {order_count}\n"
        summary_text += f"Total da Filial: {format_currency_brl(total_fine)}\n"
        df_current_branch = df_filtered[df_filtered['Filial'] == branch]
        base_summary = df_current_branch.groupby('Unidade responsável').agg(base_total_fine=('Multa (R$)', 'sum'), base_order_count=('Multa (R$)', 'size'))
        top_3_bases = base_summary.nlargest(3, 'base_total_fine')
        if not top_3_bases.empty:
            summary_text += "**Top 3 Bases (Multa):**\n"
            for base, base_data in top_3_bases.iterrows():
                fine_value = base_data['base_total_fine']
                base_order_count = base_data['base_order_count']
                summary_text += f"- {base}: {base_order_count} pedido(s) - {format_currency_brl(fine_value)}\n"
        else:
            summary_text += "Nenhuma base com multa encontrada para esta filial.\n"
    return summary_text

def generate_overdue_warning_by_coordinator(df: pd.DataFrame, previous_count: Optional[int] = None) -> str:
    overdue_days_column = 'Dias Parado'
    coordinator_column = 'Coordenador'
    fine_column = 'Multa (R$)'
    if not all(col in df.columns for col in [overdue_days_column, coordinator_column, fine_column]):
        return ""
    df_copy = df.copy()
    df_copy[overdue_days_column] = pd.to_numeric(df_copy[overdue_days_column], errors='coerce')
    df_copy.dropna(subset=[overdue_days_column], inplace=True)
    overdue_orders = df_copy[df_copy[overdue_days_column] > 15]
    if overdue_orders.empty:
        return ""
    warning_text = "\n\n--- ⚠️ AVISO: PEDIDOS HÁ MAIS DE 15 DIAS ---\n"
    coordinators = overdue_orders[coordinator_column].unique()
    for coord in coordinators:
        coord_df = overdue_orders[overdue_orders[coordinator_column] == coord]
        total_pedidos = len(coord_df)
        total_multa = coord_df[fine_column].sum()
        warning_text += f"\n**Coordenador: {coord}**\n"
        warning_text += f"Qtd de Pedidos: {total_pedidos}\n"
        warning_text += f"Valor Total: {format_currency_brl(total_multa)}\n"
        for _, row in coord_df.iterrows():
            warning_text += f"- Pedido: {row.get('Pedido', 'N/A')} - Valor: {format_currency_brl(row.get(fine_column, 0))}\n"
    if previous_count is not None:
        warning_text += f"\nContagem anterior: {previous_count} pedidos.\n"
    return warning_text

def prepare_report_data(df_current: pd.DataFrame, df_old: Optional[pd.DataFrame], report_title: str) -> Dict[str, Any]:
    total_current = len(df_current)
    metrics_text = ""
    observation_text = ""
    final_title = ""
    previous_count = len(df_old) if df_old is not None else None

    if df_old is not None:
        total_old = len(df_old)
        difference = total_current - total_old
        emoji, var_text = ("📉", f"Diminuiu {abs(difference)} pedidos") if difference < 0 else ("📈", f"Aumentou {difference} pedidos") if difference > 0 else ("➖", "Sem alteração")
        metrics_text = f"**Qtd de Pedidos Atual:** {total_current}\n**Variação:** {emoji} {var_text}"
        observation_text = f"Contagem anterior: {total_old} pedidos."
        final_title = f"🔄 Atualização: {report_title}"
    else:
        metrics_text = f"**Qtd de Pedidos:** {total_current}\n\n"
        if 'Unidade responsável' in df_current.columns:
            top_5_bases = df_current['Unidade responsável'].value_counts().nlargest(5)
            bases_text = "**Top 5 Bases (Pedidos):**\n"
            for unit, count in top_5_bases.items():
                bases_text += f"- {unit}: {count} remessas\n"
            metrics_text += bases_text
        observation_text = "Este é o primeiro resumo do dia para este relatório."
        final_title = f"📊 Relatório Inicial: {report_title}"

    if '6+ dias' in report_title and 'Multa (R$)' in df_current.columns:
        current_fines = df_current['Multa (R$)'].sum()
        old_fines = df_old['Multa (R$)'].sum() if df_old is not None and 'Multa (R$)' in df_old.columns else 0
        fines_diff = current_fines - old_fines
        emoji_f, var_f_text = ("📉", f"Diminuiu {format_currency_brl(abs(fines_diff))}") if fines_diff < 0 else ("📈", f"Aumentou {format_currency_brl(fines_diff)}") if fines_diff > 0 else ("➖", "Sem alteração")
        metrics_text += f"\n\n**Total Multas Atual:** {format_currency_brl(current_fines)}\n**Variação Multas:** {emoji_f} {var_f_text}"
        metrics_text += generate_fines_summary_by_branch(df_current)

    metrics_text += generate_overdue_warning_by_coordinator(df_current, previous_count)
    return {"title": final_title, "metrics_text": metrics_text, "observation": observation_text}

def create_feishu_card_payload(report_data: Dict[str, Any], secret_key: Optional[str]) -> Dict[str, Any]:
    elements = [
        {"tag": "div", "fields": [
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**Data de Geração:**\n{report_data.get('date', 'N/A')}" }},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**Status Geral:**\n{report_data.get('status', 'N/A')}" }}
        ]},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": report_data.get("metrics_text", "Nenhuma métrica fornecida.")}},
        {"tag": "note", "elements": [{"tag": "plain_text", "content": report_data.get("observation", "Este é um relatório gerado automaticamente.")}]}
    ]
    if report_data.get("link_excel"):
        elements.append({
            "tag": "action",
            "actions": [{"tag": "button", "text": {"tag": "plain_text", "content": "Acessar Pasta de Relatórios"},
                         "url": report_data["link_excel"], "type": "primary"}]
        })
    card_payload = {"msg_type": "interactive", "card": {"header": {"title": {"tag": "plain_text", "content": report_data.get("title", "📈 Relatório Automático")}, "template": "blue"}, "elements": elements}}
    if secret_key:
        timestamp = int(time.time())
        card_payload['timestamp'] = timestamp
        card_payload['sign'] = generate_feishu_signature(secret_key, timestamp)
    return card_payload

def send_report_to_feishu(webhook_url: str, secret_key: Optional[str], report_data: Dict[str, Any]):
    card_payload = create_feishu_card_payload(report_data, secret_key)
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(webhook_url, headers=headers, data=json.dumps(card_payload))
        response.raise_for_status()
        result = response.json()
        if result.get("StatusCode") == 0:
            print(f"-> Report '{report_data.get('title')}' sent successfully to Feishu!")
        else:
            print(f"-> Error sending report. Code: {result.get('StatusCode')}, Message: {result.get('StatusMessage')}")
    except Exception as e:
        print(f"-> Error sending report: {e}")

def process_report_file(file_path: str) -> Optional[Dict[str, Any]]:
    try:
        df_current = pd.read_excel(file_path)
    except Exception as e:
        print(f"ERROR: Cannot read file {file_path}: {e}")
        return None
    file_name = os.path.basename(file_path)
    archive_path = os.path.join(ARCHIVE_FOLDER_PATH, file_name)
    df_old = None
    if os.path.exists(archive_path):
        try:
            df_old = pd.read_excel(archive_path)
        except:
            pass
    return prepare_report_data(df_current, df_old, os.path.splitext(file_name)[0])

def run_main_task():
    if not os.path.exists(REPORTS_FOLDER_PATH):
        print(f"ERROR: Folder '{REPORTS_FOLDER_PATH}' not found.")
        return
    for file_name in os.listdir(REPORTS_FOLDER_PATH):
        if file_name.endswith('.xlsx') and not file_name.startswith('~'):
            full_path = os.path.join(REPORTS_FOLDER_PATH, file_name)
            report_data = process_report_file(full_path)
            if report_data:
                report_data["date"] = datetime.now().strftime("%d/%m/%Y %H:%M")
                report_data["status"] = "Atualização"
                report_data["link_excel"] = REPORTS_SHAREABLE_LINK
                for webhook in WEBHOOKS:
                    send_report_to_feishu(webhook["url"], webhook["secret"], report_data)

def start_scheduler(interval_seconds: int = 2*60*60, reminder_seconds: int = 20*60):
    while True:
        run_main_task()
        print(f"INFO: Waiting {interval_seconds} seconds until next execution...")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    start_scheduler()
