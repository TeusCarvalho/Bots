from pathlib import Path
import pandas as pd

# Caminho da pasta
PASTA = Path(r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\QUALIDADE_ FILIAL GO - BASE DE DADOS\01. OPERAÇÃO CD\11. ENTREGA REALIZADA - LISTA")

# Tipos de arquivos que vamos procurar
EXTENSOES = ["*.xlsx", "*.xls", "*.csv"]

arquivos = []
for ext in EXTENSOES:
    arquivos.extend(PASTA.glob(ext))

if not arquivos:
    print("❌ Nenhum arquivo Excel ou CSV encontrado na pasta.")
else:
    print(f"📁 Arquivos encontrados: {len(arquivos)}\n")

for arquivo in arquivos:
    print("=" * 100)
    print(f"📄 Arquivo: {arquivo.name}")

    try:
        if arquivo.suffix.lower() in [".xlsx", ".xls"]:
            excel = pd.ExcelFile(arquivo)

            for aba in excel.sheet_names:
                print(f"\n📌 Aba: {aba}")

                df = pd.read_excel(
                    arquivo,
                    sheet_name=aba,
                    nrows=0
                )

                for i, coluna in enumerate(df.columns, start=1):
                    print(f"{i}. {coluna}")

        elif arquivo.suffix.lower() == ".csv":
            df = pd.read_csv(
                arquivo,
                nrows=0,
                sep=None,
                engine="python",
                encoding="utf-8"
            )

            print("\n📌 CSV")

            for i, coluna in enumerate(df.columns, start=1):
                print(f"{i}. {coluna}")

    except Exception as e:
        print(f"❌ Erro ao ler o arquivo {arquivo.name}: {e}")