# Importando as bibliotecas necessárias.
import pandas as pd
import tkinter as tk
from tkinter import messagebox
from pathlib import Path
import matplotlib.pyplot as plt
import os
import requests

# --- CONFIGURAÇÕES ---
# Caminhos fixos para automação
PASTA_RELATORIOS = Path(r"C:\Users\JT-244\OneDrive - Speed Rabbit Express Ltda\Jt - Relatórios")
PASTA_SAIDA = Path(r"C:\Users\JT-244\Desktop\Testes\SLA - D1")

# Palavra-chave para identificar o arquivo e a aba
PALAVRA_CHAVE_6_MAIS_DIAS = "6+"
PALAVRA_CHAVE_NA_ABA = "Relatório"

# Colunas necessárias para o relatório
COLUNAS_6_MAIS_DIAS = ["Remessa", "Unidade responsável", "Coordenadores", "Multa (R$)", "Dias Parado"]


def criar_janela_principal():
    """Cria e esconde a janela principal do Tkinter."""
    root = tk.Tk()
    root.withdraw()
    return root


def excluir_arquivos_antigos(pasta):
    """Exclui arquivos .xlsx e .png da pasta de saída antes de gerar novos."""
    print(f"\nLimpando pasta de saída: {pasta}")
    if not pasta.exists():
        print("Pasta de saída não existe, nada a limpar.")
        return

    for arquivo in pasta.glob('*.xlsx'):
        try:
            arquivo.unlink()
            print(f"  - Deletado: {arquivo.name}")
        except OSError as e:
            print(f"  - Erro ao deletar {arquivo.name}: {e}")

    for arquivo in pasta.glob('*.png'):
        try:
            arquivo.unlink()
            print(f"  - Deletado: {arquivo.name}")
        except OSError as e:
            print(f"  - Erro ao deletar {arquivo.name}: {e}")
    print("Limpeza concluída.")


def categorizar_aging_agrupado(dias):
    """Categoriza o aging nas faixas de dias para o relatório 6+."""
    if not isinstance(dias, (int, float)) or dias < 0:
        return "Inválido"
    dias = int(dias)
    if dias < 7:
        return f"{dias} Dias" if dias != 1 else "1 Dia"
    elif dias <= 9:
        return "7 a 9 dias"
    elif dias <= 13:
        return "10 a 13 dias"
    elif dias <= 29:
        return "14 a 29 dias"
    else:
        return "30+ dias"


def processar_planilha_6_mais_dias(caminho_entrada):
    """Lê e processa os dados brutos da planilha de 6+ dias, retornando um DataFrame limpo."""
    print(f"Lendo o arquivo: {caminho_entrada.name}...")

    excel_file = pd.ExcelFile(caminho_entrada)
    aba_encontrada = next((nome for nome in excel_file.sheet_names if PALAVRA_CHAVE_NA_ABA.lower() in nome.lower()),
                          excel_file.sheet_names[0])

    if not aba_encontrada:
        raise ValueError(f"Nenhuma aba com a palavra-chave '{PALAVRA_CHAVE_NA_ABA}' foi encontrada.")

    print(f"Lendo a aba: '{aba_encontrada}'")
    df_bruto = pd.read_excel(caminho_entrada, sheet_name=aba_encontrada)

    print("Processando os dados...")
    if 'Coordenadores' in df_bruto.columns:
        df_bruto = df_bruto[df_bruto['Coordenadores'] != 'NÃO ENCONTRADO']

    colunas_faltantes = [col for col in COLUNAS_6_MAIS_DIAS if col not in df_bruto.columns]
    if colunas_faltantes:
        raise ValueError(
            f"As seguintes colunas não foram encontradas no arquivo {caminho_entrada.name}: {', '.join(colunas_faltantes)}")

    df_filtrado = df_bruto[COLUNAS_6_MAIS_DIAS].copy()
    df_filtrado['Dias Parado'] = pd.to_numeric(df_filtrado['Dias Parado'], errors='coerce').fillna(0)
    df_filtrado['Categoria Dias'] = df_filtrado['Dias Parado'].apply(categorizar_aging_agrupado)

    return df_filtrado


def salvar_dados_excel(df, caminho_saida):
    """Salva o DataFrame processado em uma aba 'Dados' de um novo arquivo Excel."""
    print(f"Salvando dados processados em '{caminho_saida}'...")
    with pd.ExcelWriter(caminho_saida, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Dados", index=False)


def gerar_grafico_completo_png(df_dados, caminho_saida_imagem):
    """Gera o gráfico de barras empilhadas (Quantidade e Multa) para o relatório 6+."""
    print(f"Gerando gráfico completo em '{caminho_saida_imagem}'...")
    try:
        pivot_qtd = df_dados.pivot_table(index='Coordenadores', columns='Categoria Dias', values='Remessa',
                                         aggfunc='count', fill_value=0)
        pivot_valor = df_dados.pivot_table(index='Coordenadores', columns='Categoria Dias', values='Multa (R$)',
                                           aggfunc='sum', fill_value=0)

        def sort_category_columns(df):
            category_order = ["0 Dias", "1 Dia", "2 Dias", "3 Dias", "4 Dias", "5 Dias", "6 Dias", "7 a 9 dias",
                              "10 a 13 dias", "14 a 29 dias", "30+ dias"]
            order_map = {cat: i for i, cat in enumerate(category_order)}
            present_cols = [col for col in df.columns if col in order_map]
            return df[sorted(present_cols, key=lambda x: order_map[x])]

        pivot_qtd = sort_category_columns(pivot_qtd)
        pivot_valor = sort_category_columns(pivot_valor)

        total_order = pivot_qtd.sum(axis=1).sort_values(ascending=True).index
        pivot_qtd = pivot_qtd.loc[total_order]
        pivot_valor = pivot_valor.loc[total_order]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(22, 12))
        fig.suptitle('Resumo por Coordenador e Faixa de Dias (6+ dias)', fontsize=20, weight='bold')

        pivot_qtd.plot(kind='barh', stacked=True, ax=ax1, colormap='Blues_r')
        ax1.set_title('Quantidade de Remessas', fontsize=16)
        for i, total in enumerate(pivot_qtd.sum(axis=1)):
            ax1.text(total, i, f' {int(total)}', va='center', weight='bold')

        pivot_valor.plot(kind='barh', stacked=True, ax=ax2, colormap='Greys_r')
        ax2.set_title('Valor da Multa (R$)', fontsize=16)
        for i, total in enumerate(pivot_valor.sum(axis=1)):
            ax2.text(total, i, f' R$ {total:,.2f}'.replace(',', 'v').replace('.', ',').replace('v', '.'), va='center',
                     weight='bold')

        plt.tight_layout(rect=[0, 0.03, 0.9, 0.95])
        plt.savefig(caminho_saida_imagem, bbox_inches='tight')
        plt.close()
        print("Gráfico salvo com sucesso.")
    except Exception as e:
        raise Exception(f"Erro ao gerar gráfico completo: {e}")


def main():
    """Função principal que orquestra a execução do script."""
    root = criar_janela_principal()

    try:
        excluir_arquivos_antigos(PASTA_SAIDA)
        PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
        arquivos_processados = []

        print(f"Procurando relatórios em: {PASTA_RELATORIOS}")
        for arquivo in PASTA_RELATORIOS.glob('*.xlsx'):
            nome_arquivo = arquivo.name.lower()

            if PALAVRA_CHAVE_6_MAIS_DIAS in nome_arquivo:
                print(f"\n--- Processando relatório tipo '6+': {arquivo.name} ---")

                df_dados = processar_planilha_6_mais_dias(arquivo)

                nome_base = f"{arquivo.stem}_PROCESSADO"
                caminho_excel = PASTA_SAIDA / f"{nome_base}.xlsx"
                caminho_imagem = PASTA_SAIDA / f"{nome_base}.png"

                salvar_dados_excel(df_dados, caminho_excel)
                gerar_grafico_completo_png(df_dados, caminho_imagem)
                # A linha send_feishu_notifications(df_dados) foi removida.

                arquivos_processados.append(arquivo.name)

        if not arquivos_processados:
            messagebox.showwarning("Atenção",
                                   f"Nenhum relatório com a palavra-chave '{PALAVRA_CHAVE_6_MAIS_DIAS}' foi encontrado em:\n{PASTA_RELATORIOS}")
        else:
            messagebox.showinfo("Sucesso!", f"Processamento concluído!\n\nArquivos gerados em:\n{PASTA_SAIDA}")

    except Exception as e:
        print(f"ERRO: {e}")
        messagebox.showerror("Erro", f"Ocorreu um erro inesperado:\n{e}")
    finally:
        root.destroy()


if __name__ == "__main__":
    main()