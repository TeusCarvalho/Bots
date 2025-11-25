import polars as pl
import os

pasta = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.1 - Base Retidos(Lista)"

for f in os.listdir(pasta):
    if f.lower().endswith((".xls", ".xlsx")):
        print("\nArquivo:", f)
        df = pl.read_excel(os.path.join(pasta, f))
        print(df.columns)
        break   # mostra só o primeiro arquivo
