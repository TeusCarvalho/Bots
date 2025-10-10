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
    print("To enable, run: pip install plyer")

# --- CONFIGURATION BLOCK ---

# A consolidated list of webhooks for easier management.
# Add as many webhooks as you need to this list.
WEBHOOKS: List[Dict[str, Optional[str]]] = [
    {
        "name": "Indicadores Operacionais | Qualidade e Redes",
        "url": os.getenv("FEISHU_WEBHOOK_URL_1",
                         "https://open.feishu.cn/open-apis/bot/v2/hook/27a6f2d5-0fe3-4bd4-8f8e-84573b9324bc"),
        # Dados GO
        "secret": os.getenv("FEISHU_SECRET_KEY_1", "576mFMGpGywoOTyFqvaGOeMKgcUANnDp")
    },
    #{
    #    "name": "Grupo de Dados GO",
    #    "url": os.getenv("FEISHU_WEBHOOK_URL_SLA_2",
    #                     "https://open.feishu.cn/open-apis/bot/v2/hook/7b9ae3c3-e645-4367-85e5-e8e8aa11d808"),
    #    "secret": os.getenv("FEISHU_SECRET_KEY_SLA_2", "GOreUcd9cTaWTVE4IHiAbh")
    #},

    # Esse é o formato de como é o envio do Feishu:
    # {
    #   "url": "URL_FOR_THIRD_WEBHOOK",
    #   "secret": "SECRET_KEY_FOR_THIRD_WEBHOOK"
    # },
]

# Path to the folder containing the reports to be processed.
REPORTS_FOLDER_PATH = os.getenv("REPORTS_FOLDER_PATH",
                                r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Jt - Relatórios")
# Path where processed reports will be archived for comparison.
ARCHIVE_FOLDER_PATH = os.path.join(REPORTS_FOLDER_PATH, "Arquivo Morto")
# A shareable link to the reports folder for the Feishu card button.
REPORTS_SHAREABLE_LINK = os.getenv("REPORTS_SHAREABLE_LINK",
                                   "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/Ek3KdqMIdX5EodE-3JwCQnsBAMiJ574BsxAR--oYBNN0-g?e=dfqBzT")


# --- HELPER FUNCTIONS ---

def send_desktop_notification(title: str, message: str):
    """Sends a desktop notification if the 'plyer' library is available."""
    if not PLYER_AVAILABLE:
        return
    try:
        notification.notify(
            title=title,
            message=message,
            app_name='Reports Scheduler',
            timeout=15
        )
        print(f"INFO: Desktop notification sent: '{title}'")
    except Exception as e:
        print(f"WARNING: Failed to send desktop notification. Error: {e}")


def format_currency_brl(value: float) -> str:
    """Formats a float value into the Brazilian currency format (R$)."""
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def generate_feishu_signature(secret: str, timestamp: int) -> str:
    """Generates the required signature for Feishu webhook security verification."""
    string_to_sign = f'{timestamp}\n{secret}'
    hmac_code = hmac.new(string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
    return base64.b64encode(hmac_code).decode('utf-8')


def generate_fines_summary_by_branch(df: pd.DataFrame) -> str:
    """Generates a formatted text summary of fines by branch and the top 3 bases within each."""
    required_columns = ['Filial', 'Unidade responsável', 'Multa (R$)']
    if not all(col in df.columns for col in required_columns):
        print("WARNING: Required columns for fines summary not found. Skipping.")
        return ""

    # Filter out entries where 'Filial' is not found and create a copy to avoid SettingWithCopyWarning
    df_filtered = df[df['Filial'] != 'Não encontrada'].copy()
    if df_filtered.empty or df_filtered['Multa (R$)'].sum() == 0:
        return ""

    # Aggregate by Filial to get total fines and order count
    branch_summary = df_filtered.groupby('Filial').agg(
        total_fine=('Multa (R$)', 'sum'),
        order_count=('Multa (R$)', 'size')  # 'size' counts all rows in the group
    )

    summary_text = "\n\n--- **Resumo de Multas por Filial** ---"

    # Sort by total fine in descending order
    for branch, data in branch_summary.sort_values(by='total_fine', ascending=False).iterrows():
        total_fine = data['total_fine']
        order_count = data['order_count']

        summary_text += f"\n\n**📍 Filial: {branch}**\n"
        summary_text += f"**Qtd de Pedidos:** {order_count}\n"
        summary_text += f"**Total da Filial:** {format_currency_brl(total_fine)}\n"

        df_current_branch = df_filtered[df_filtered['Filial'] == branch]

        # Aggregate by 'Unidade responsável' to get total fines and order count for each base
        base_summary = df_current_branch.groupby('Unidade responsável').agg(
            base_total_fine=('Multa (R$)', 'sum'),
            base_order_count=('Multa (R$)', 'size')
        )

        # Get the top 3 bases based on the total fine
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


def generate_overdue_warning(df: pd.DataFrame) -> str:
    """
    Generates a warning for the top 5 bases with orders older than 15 days using the 'Dias Parado' column.
    Assumes the DataFrame contains 'Dias Parado' and 'Unidade responsável' columns.
    """
    overdue_days_column = 'Dias Parado'
    base_column = 'Unidade responsável'

    # Check if necessary columns exist
    if not all(col in df.columns for col in [overdue_days_column, base_column]):
        return ""  # Silently skip if columns aren't present

    # Create a copy to avoid modifying the original DataFrame
    df_copy = df.copy()

    # Ensure the 'Dias Parado' column is numeric, converting non-numeric to NaN
    df_copy[overdue_days_column] = pd.to_numeric(df_copy[overdue_days_column], errors='coerce')
    df_copy.dropna(subset=[overdue_days_column], inplace=True)

    # Filter for orders older than 15 days
    overdue_orders = df_copy[df_copy[overdue_days_column] > 15]

    if overdue_orders.empty:
        return ""  # No overdue orders found

    # Count overdue orders by base and get the top 5
    overdue_counts = overdue_orders[base_column].value_counts().nlargest(5)

    # Build the warning message
    warning_text = "\n\n--- ⚠️ **AVISO: PEDIDOS HÁ MAIS DE 15 DIAS** ---\n"
    warning_text += "**As 5 piores bases com pedidos parados são:**\n"

    for base, count in overdue_counts.items():
        warning_text += f"- {base}: {count} pedido(s)\n"

    return warning_text


# --- CORE REPORTING LOGIC ---

def prepare_report_data(df_current: pd.DataFrame, df_old: Optional[pd.DataFrame], report_title: str) -> Dict[str, Any]:
    """
    Prepares the report data dictionary by comparing the current and old dataframes.
    Handles both initial reports (df_old is None) and update reports.
    """
    total_current = len(df_current)
    metrics_text = ""
    observation_text = ""
    final_title = ""

    if df_old is not None:
        # --- Generate an Update Report ---
        total_old = len(df_old)
        difference = total_current - total_old

        if difference < 0:
            emoji, var_text = ("📉", f"Diminuiu {abs(difference)} pedidos")
        elif difference > 0:
            emoji, var_text = ("📈", f"Aumentou {difference} pedidos")
        else:
            emoji, var_text = ("➖", "Sem alteração")

        metrics_text = f"**Qtd de Pedidos Atual:** {total_current}\n**Variação:** {emoji} {var_text}"
        observation_text = f"Contagem anterior: {total_old} pedidos."
        final_title = f"🔄 Atualização: {report_title}"
    else:
        # --- Generate an Initial Report ---
        metrics_text = f"**Qtd de Pedidos:** {total_current}\n\n"
        if 'Unidade responsável' in df_current.columns:
            top_5_bases = df_current['Unidade responsável'].value_counts().nlargest(5)
            bases_text = "**Top 5 Bases (Pedidos):**\n"
            if not top_5_bases.empty:
                for unit, count in top_5_bases.items():
                    bases_text += f"- {unit}: {count} remessas\n"
            else:
                bases_text += "Nenhuma unidade encontrada.\n"
            metrics_text += bases_text
        else:
            metrics_text += "Coluna 'Unidade responsável' não encontrada.\n"

        observation_text = "Este é o primeiro resumo do dia para este relatório."
        final_title = f"📊 Relatório Inicial: {report_title}"

    # --- Add Fines Summary (if applicable) ---
    if '6+ dias' in report_title and 'Multa (R$)' in df_current.columns:
        current_fines = df_current['Multa (R$)'].sum()
        if df_old is not None and 'Multa (R$)' in df_old.columns:
            old_fines = df_old['Multa (R$)'].sum()
            fines_diff = current_fines - old_fines

            if fines_diff < 0:
                emoji_f, var_f_text = ("📉", f"Diminuiu {format_currency_brl(abs(fines_diff))}")
            elif fines_diff > 0:
                emoji_f, var_f_text = ("📈", f"Aumentou {format_currency_brl(fines_diff)}")
            else:
                emoji_f, var_f_text = ("➖", "Sem alteração")

            metrics_text += f"\n\n**Total Multas Atual:** {format_currency_brl(current_fines)}\n**Variação Multas:** {emoji_f} {var_f_text}"
        else:
            metrics_text += f"\n**Valor Total das Multas:** {format_currency_brl(current_fines)}"

        metrics_text += generate_fines_summary_by_branch(df_current)

    # Add the overdue orders warning to the report metrics
    metrics_text += generate_overdue_warning(df_current)

    return {
        "title": final_title,
        "metrics_text": metrics_text,
        "observation": observation_text,
    }


def create_feishu_card_payload(report_data: Dict[str, Any], secret_key: Optional[str]) -> Dict[str, Any]:
    """Builds the complete Feishu interactive card payload from the report data."""
    elements = [
        {"tag": "div", "fields": [
            {"is_short": True,
             "text": {"tag": "lark_md", "content": f"**Data de Geração:**\n{report_data.get('date', 'N/A')}"}},
            {"is_short": True,
             "text": {"tag": "lark_md", "content": f"**Status Geral:**\n{report_data.get('status', 'N/A')}"}}
        ]},
        {"tag": "hr"},
        {"tag": "div",
         "text": {"tag": "lark_md", "content": report_data.get("metrics_text", "Nenhuma métrica fornecida.")}},
        {"tag": "note", "elements": [{"tag": "plain_text", "content": report_data.get("observation",
                                                                                      "Este é um relatório gerado automaticamente.")}]}
    ]

    if report_data.get("link_excel"):
        elements.append({
            "tag": "action",
            "actions": [{"tag": "button", "text": {"tag": "plain_text", "content": "Acessar Pasta de Relatórios"},
                         "url": report_data["link_excel"], "type": "primary"}]
        })

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": report_data.get("title", "📈 Relatório Automático")},
                "template": "blue"
            },
            "elements": elements
        }
    }

    if secret_key:
        timestamp = int(time.time())
        card_payload['timestamp'] = timestamp
        card_payload['sign'] = generate_feishu_signature(secret_key, timestamp)

    return card_payload


def send_report_to_feishu(webhook_url: str, secret_key: Optional[str], report_data: Dict[str, Any]):
    """Sends a formatted report as an Interactive Card to a Feishu webhook."""
    if not webhook_url or "open.feishu.cn" not in webhook_url:
        print(f"ERROR: Webhook URL seems invalid or is not configured. URL: '{webhook_url}'")
        return

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
    except requests.exceptions.RequestException as e:
        print(f"-> A network error occurred while trying to send the report: {e}")
    except json.JSONDecodeError:
        print(f"-> Failed to decode the server's response. Response received: {response.text}")
    except Exception as e:
        print(f"-> An unexpected error occurred while sending the report: {e}")


def process_report_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Reads a report file, compares it with its archived version,
    and returns the processed data ready for sending.
    """
    file_name = os.path.basename(file_path)
    report_title = os.path.splitext(file_name)[0]

    try:
        df_current = pd.read_excel(file_path)
    except FileNotFoundError:
        print(f"ERROR: File '{file_name}' not found at '{file_path}'.")
        return None
    except Exception as e:
        print(f"ERROR: Failed to read Excel file '{file_name}'. Error: {e}")
        return None

    archive_path = os.path.join(ARCHIVE_FOLDER_PATH, file_name)
    df_old = None
    if os.path.exists(archive_path):
        try:
            print(f"INFO: Found comparison file: {archive_path}")
            df_old = pd.read_excel(archive_path)
        except Exception as e:
            print(f"WARNING: Failed to read comparison file '{file_name}'. Generating initial report. Error: {e}")

    return prepare_report_data(df_current, df_old, report_title)


# --- MAIN EXECUTION AND SCHEDULER ---

def run_main_task():
    """Main function that orchestrates the script's execution."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting report check in: {REPORTS_FOLDER_PATH}")
    if not os.path.isdir(REPORTS_FOLDER_PATH):
        print("CRITICAL ERROR: The reports directory was not found. Exiting.")
        return

    reports_to_send = []
    for file_name in os.listdir(REPORTS_FOLDER_PATH):
        if file_name.endswith('.xlsx') and not file_name.startswith('~'):
            full_path = os.path.join(REPORTS_FOLDER_PATH, file_name)
            print(f"\n--- Processing file: {file_name} ---")
            processed_data = process_report_file(full_path)
            if processed_data:
                processed_data["date"] = datetime.now().strftime("%d/%m/%Y %H:%M")
                processed_data["status"] = "Atualização"
                processed_data["link_excel"] = REPORTS_SHAREABLE_LINK
                reports_to_send.append(processed_data)

    if reports_to_send:
        print("\n--- STARTING TO SEND REPORTS TO WEBHOOKS ---")
        for report in reports_to_send:
            print(f"\nDispatching report: '{report.get('title')}'")
            for webhook in WEBHOOKS:
                if not webhook["url"] or "URL_DO_SEU" in webhook["url"]:
                    print(f"WARNING: Skipping unconfigured webhook: {webhook['url']}")
                    continue

                url_display = webhook["url"][:60] + "..." if len(webhook["url"]) > 60 else webhook["url"]
                print(f"  -> Sending to: {url_display}")
                send_report_to_feishu(webhook["url"], webhook["secret"], report)
                time.sleep(1)  # Pause between each send to avoid rate limiting
        print("\n--- SENDING COMPLETE ---")
    else:
        print("\nNo valid reports were processed for sending.")


def start_scheduler(interval_seconds: int, reminder_seconds: int):
    """Starts the infinite loop for scheduled execution."""
    if interval_seconds <= reminder_seconds:
        raise ValueError("The execution interval must be greater than the reminder time.")

    main_wait_time = interval_seconds - reminder_seconds

    print("Scheduler started. Running the first execution manually...")
    run_main_task()
    print("\nFirst execution complete.")

    try:
        while True:
            next_run_time = datetime.now() + timedelta(seconds=interval_seconds)
            notification_message = f"Next check scheduled for {next_run_time.strftime('%H:%M:%S on %d/%m/%Y')}."

            send_desktop_notification(
                title='Next Update Scheduled',
                message=notification_message
            )
            print(f"\n{notification_message}")

            print(f"Waiting for {main_wait_time / 60:.0f} minutes before the reminder...")
            time.sleep(main_wait_time)

            reminder_message = f"The report check will start in {reminder_seconds // 60} minutes."
            print(f"\nREMINDER: {reminder_message}")
            send_desktop_notification(
                title='Update Reminder',
                message=reminder_message
            )

            print("Waiting for the final reminder period...")
            time.sleep(reminder_seconds)

            run_main_task()

    except KeyboardInterrupt:
        print("\nScheduler interrupted by user. Shutting down.")
    except Exception as e:
        print(f"\nA fatal error occurred in the main loop: {e}")


if __name__ == "__main__":
    # --- Scheduler Configuration ---
    EXECUTION_INTERVAL_SECONDS = 2 * 60 * 60  # 2 hours
    REMINDER_BEFORE_EXECUTION_SECONDS = 20 * 60  # 20 minutes

    start_scheduler(
        interval_seconds=EXECUTION_INTERVAL_SECONDS,
        reminder_seconds=REMINDER_BEFORE_EXECUTION_SECONDS
    )