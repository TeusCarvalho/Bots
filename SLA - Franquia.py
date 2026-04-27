import re
import warnings
from pathlib import Path
from datetime import datetime
from collections import Counter

import pandas as pd


# =========================
# CONFIGURAÇÕES
# =========================
PASTA_ENTRADA = Path(r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\06-  SLA Entrega Realizada Franquia")
PASTA_SAIDA = Path(r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\Franquas - SLA")

NOME_COLUNA_ALVO = "Base de entrega"

# Quantidade máxima de linhas de dados por aba
# (deixei 1.000.000 para ficar abaixo do limite do Excel)
MAX_LINHAS_POR_ABA = 1_000_000

# Se quiser manter a coluna com o arquivo de origem
ADICIONAR_ARQUIVO_ORIGEM = True

BASES_PERMITIDAS_TEXTO = """
F AGB 02-MT
F AGB-MT
F AGL-GO
F AGT-TO
F ALV-AM
F AMB-MS
F ANA-PA
F ANP 02-GO
F ANP-GO
F APG - GO
F APG 02-GO
F APG 03-GO
F ARQ - RO
F ARQ 02-RO
F BAO-PA
F BBG-MT
F BGA-MT
F BRV-PA
F BSB 02-DF
F BSB-DF
F BSL-AC
F BTS-RO
F BVB-RR
F BZL-DF
F CAC-RO
F CAM-PA
F CCR-MT
F CDN 02-AM
F CDN 03-AM
F CDN-AM
F CEI-DF
F CGR - MS
F CGR 02-MS
F CGR 03-MS
F CGR 04-MS
F CGR 05-MS
F CHR-AM
F CMV-MT
F CNA-PA
F CNC-PA
F CNF-MT
F CNP-MT
F CRH-PA
F CRJ-RO
F CRX-GO
F CST-PA
F CTL-GO
F DOM -PA
F DOU 02-MS
F DOU-MS
F ELD-PA
F EMA-DF
F FMA-GO
F GAI-TO
F GAM-DF
F GFN-PA
F GNA-GO
F GNS-PA
F GRP-TO
F GUA 02-DF
F GUA-DF
F GYN - GO
F GYN 02-GO
F GYN 03-GO
F GYN 04-GO
F GYN 06-GO
F GYN 07-GO
F GYN 08-GO
F HMT-AM
F IGA-PA
F IGM-PA
F IPX-PA
F ITI-PA
F ITT-PA
F JAU-RO
F JCD-PA
F JPN 02-RO
F JPN 03-RO
F JPN 04-RO
F JPN-RO
F JRG-GO
F JRT-PA
F JTI-GO
F MAC-AP
F MCP 02-AP
F MCP 04-AP
F MCP-AP
F MDO-RO
F MDR-PA
F MDT-MT
F MRL-AM
F MTB-PA
F NDI-MS
F NMB-PA
F OCD-GO
F ORL-PA
F PAZ-AM
F PCA-PA
F PDP-PA
F PDR-GO
F PDR-PA
F PDT-TO
F PGM-PA
F PLA-GO
F PLN-DF
F PMG-GO
F PMW-TO
F PNA-TO
F PON-GO
F POS-GO
F PPA-MS
F PTD-MT
F PVH 02-RO
F PVH 03-RO
F PVH 04-RO
F PVH-RO
F PVL 02-MT
F PVL-MT
F QUI-GO
F RBR 02-AC
F RBR-AC
F RDC -PA
F RDM-RO
F RFI-DF
F RLB-MS
F ROO-MT
F RVD - GO
F SAM-DF
F SBN-DF
F SBS-DF
F SBZ-PA
F SEN-GO
F SFX-PA
F SJA-GO
F SNP-MT
F SVC 02-RR
F SVC-RR
F TCP-TO
F TGA-MT
F TGT 02-DF
F TGT-DF
F TLA-PA
F TNN-PA
F TPN-PA
F TRD-GO
F TUR-PA
F UNA-PA
F VHL-RO
F VLP-GO
F XIG 02-PA
F XIG-PA
"""


# =========================
# AVISOS
# =========================
warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style, apply openpyxl's default"
)


# =========================
# FUNÇÕES AUXILIARES
# =========================
def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    valor = str(valor).replace("\xa0", " ").upper().strip()
    valor = re.sub(r"\s+", " ", valor)
    valor = re.sub(r"\s*-\s*", "-", valor)
    return valor


def normalizar_nome_coluna(nome):
    if nome is None:
        return ""
    nome = str(nome).replace("\xa0", " ").strip().lower()
    nome = re.sub(r"\s+", " ", nome)
    return nome


def encontrar_coluna(df, nome_desejado):
    alvo = normalizar_nome_coluna(nome_desejado)
    for col in df.columns:
        if normalizar_nome_coluna(col) == alvo:
            return col
    return None


def ler_arquivo(caminho):
    extensao = caminho.suffix.lower()

    if extensao in [".xlsx", ".xlsm", ".xls"]:
        return pd.read_excel(caminho, dtype=str)

    if extensao == ".csv":
        for enc in ["utf-8", "utf-8-sig", "latin1"]:
            try:
                return pd.read_csv(caminho, dtype=str, encoding=enc, low_memory=False)
            except Exception:
                continue
        raise ValueError(f"Não foi possível ler o CSV: {caminho.name}")

    return None


def quebrar_e_adicionar_no_buffer(df, buffer_dfs, linhas_buffer, max_linhas):
    """
    Adiciona o dataframe ao buffer respeitando o limite de linhas por aba.
    Retorna:
    - lista de blocos prontos para gravação
    - novo buffer_dfs
    - novo linhas_buffer
    """
    blocos_prontos = []

    restante = df

    while not restante.empty:
        espaco = max_linhas - linhas_buffer

        if espaco <= 0:
            blocos_prontos.append(pd.concat(buffer_dfs, ignore_index=True, sort=False))
            buffer_dfs = []
            linhas_buffer = 0
            espaco = max_linhas

        if len(restante) <= espaco:
            buffer_dfs.append(restante)
            linhas_buffer += len(restante)
            restante = restante.iloc[0:0]
        else:
            parte = restante.iloc[:espaco].copy()
            buffer_dfs.append(parte)
            linhas_buffer += len(parte)

            blocos_prontos.append(pd.concat(buffer_dfs, ignore_index=True, sort=False))
            buffer_dfs = []
            linhas_buffer = 0

            restante = restante.iloc[espaco:].copy()

    return blocos_prontos, buffer_dfs, linhas_buffer


def salvar_bloco(writer, df, numero_aba, controle_abas):
    nome_aba = f"Consolidado_{numero_aba:03d}"
    df.to_excel(writer, sheet_name=nome_aba, index=False)

    controle_abas.append({
        "Tipo": "Consolidado",
        "Nome_aba": nome_aba,
        "Linhas": len(df)
    })


# =========================
# PROCESSAMENTO PRINCIPAL
# =========================
def main():
    bases_permitidas = {
        normalizar_texto(linha)
        for linha in BASES_PERMITIDAS_TEXTO.splitlines()
        if linha.strip()
    }

    print(f"Total de bases permitidas: {len(bases_permitidas)}")

    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

    arquivos = [
        arq for arq in PASTA_ENTRADA.iterdir()
        if arq.is_file()
        and not arq.name.startswith("~$")
        and arq.suffix.lower() in [".xlsx", ".xlsm", ".xls", ".csv"]
    ]

    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em: {PASTA_ENTRADA}")

    print(f"Total de arquivos encontrados: {len(arquivos)}")

    nome_saida = f"Franquias_SLA_Filtrado_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    caminho_saida = PASTA_SAIDA / nome_saida

    resumo_contador = Counter()
    arquivos_sem_coluna = []
    arquivos_com_erro = []
    arquivos_sem_registro = []

    buffer_dfs = []
    linhas_buffer = 0
    numero_aba = 1
    total_linhas_filtradas = 0
    controle_abas = []

    with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
        # Aba pequena para garantir que o arquivo sempre tenha pelo menos uma aba válida
        pd.DataFrame({"Status": ["Processamento iniciado"]}).to_excel(
            writer, sheet_name="Info_Processamento", index=False
        )

        for i, arquivo in enumerate(sorted(arquivos), start=1):
            print(f"[{i}/{len(arquivos)}] Lendo: {arquivo.name}")

            try:
                df = ler_arquivo(arquivo)

                if df is None or df.empty:
                    print(f"  -> Arquivo vazio ou não suportado")
                    continue

                col_base = encontrar_coluna(df, NOME_COLUNA_ALVO)

                if not col_base:
                    arquivos_sem_coluna.append(arquivo.name)
                    print(f"  -> Coluna '{NOME_COLUNA_ALVO}' não encontrada")
                    continue

                base_normalizada = df[col_base].map(normalizar_texto)
                mascara = base_normalizada.isin(bases_permitidas)

                if not mascara.any():
                    arquivos_sem_registro.append(arquivo.name)
                    print(f"  -> Nenhum registro das bases desejadas")
                    continue

                df_filtrado = df.loc[mascara].copy()
                df_filtrado[col_base] = base_normalizada.loc[mascara]

                if ADICIONAR_ARQUIVO_ORIGEM:
                    df_filtrado["__arquivo_origem__"] = arquivo.name

                contagem_bases = df_filtrado[col_base].value_counts(dropna=False)
                for base, qtd in contagem_bases.items():
                    resumo_contador[base] += int(qtd)

                total_linhas_filtradas += len(df_filtrado)
                print(f"  -> Linhas aproveitadas: {len(df_filtrado):,}".replace(",", "."))

                blocos_prontos, buffer_dfs, linhas_buffer = quebrar_e_adicionar_no_buffer(
                    df_filtrado,
                    buffer_dfs,
                    linhas_buffer,
                    MAX_LINHAS_POR_ABA
                )

                for bloco in blocos_prontos:
                    salvar_bloco(writer, bloco, numero_aba, controle_abas)
                    print(f"  -> Aba Consolidado_{numero_aba:03d} salva com {len(bloco):,} linhas".replace(",", "."))
                    numero_aba += 1

            except Exception as e:
                arquivos_com_erro.append((arquivo.name, str(e)))
                print(f"  -> Erro: {e}")

        # grava o restante do buffer
        if buffer_dfs:
            df_final = pd.concat(buffer_dfs, ignore_index=True, sort=False)
            salvar_bloco(writer, df_final, numero_aba, controle_abas)
            print(f"  -> Aba Consolidado_{numero_aba:03d} salva com {len(df_final):,} linhas".replace(",", "."))
            numero_aba += 1

        # resumo
        if resumo_contador:
            resumo_df = pd.DataFrame(
                sorted(resumo_contador.items(), key=lambda x: x[0]),
                columns=[NOME_COLUNA_ALVO, "Quantidade"]
            )
        else:
            resumo_df = pd.DataFrame(columns=[NOME_COLUNA_ALVO, "Quantidade"])

        resumo_df.to_excel(writer, sheet_name="Resumo_Bases", index=False)

        # controle
        controle_arquivos = []

        for nome in arquivos_sem_coluna:
            controle_arquivos.append({
                "Arquivo": nome,
                "Status": "Sem coluna Base de entrega"
            })

        for nome in arquivos_sem_registro:
            controle_arquivos.append({
                "Arquivo": nome,
                "Status": "Sem bases da lista"
            })

        for nome, erro in arquivos_com_erro:
            controle_arquivos.append({
                "Arquivo": nome,
                "Status": f"Erro: {erro}"
            })

        if not controle_arquivos:
            controle_arquivos.append({
                "Arquivo": "",
                "Status": "Todos os arquivos processados sem ocorrências"
            })

        pd.DataFrame(controle_arquivos).to_excel(writer, sheet_name="Controle_Arquivos", index=False)
        pd.DataFrame(controle_abas).to_excel(writer, sheet_name="Controle_Abas", index=False)

    print("\nProcesso finalizado com sucesso!")
    print(f"Total de linhas filtradas: {total_linhas_filtradas:,}".replace(",", "."))
    print(f"Arquivo salvo em:\n{caminho_saida}")


if __name__ == "__main__":
    main()