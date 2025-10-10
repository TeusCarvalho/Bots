# 📦 Bots J&T Express Brasil

Este repositório contém diversos **scripts em Python** para automação e análise de relatórios logísticos utilizados na J&T Express Brasil.  
Os códigos foram desenvolvidos para auxiliar no monitoramento de **bases, coordenadores, SLA (T-0), pedidos sem movimentação, custos e arbitragem**.

---

## 🚀 Estrutura dos Principais Scripts

### 🔴 Sem Movimentação
- **Objetivo:** Processar relatórios de pedidos parados (5+ dias).
- **Funcionalidades:**
  - Compara com o último relatório no **Arquivo Morto**.
  - Destaca as **piores e melhores bases** (cores no card Feishu).
  - Envia cards automáticos para os coordenadores via **webhook**.

### 💰 Custo e Arbitragem
- **Objetivo:** Consolidar e enviar relatórios de custos.
- **Funcionalidades:**
  - Formata valores em **R$ BRL**.
  - Opção de envio para **Franquias** ou **Coordenadores específicos**.
  - Integração com webhooks Feishu.

### ⏱️ T-0 (Prazo de Entrega)
- **Objetivo:** Analisar o cumprimento do prazo de assinatura T-0.
- **Funcionalidades:**
  - Leitura de relatórios de SLA.
  - Cálculo de entregas dentro/fora do prazo.
  - Geração de resumos semanais em planilhas.

### 📊 ShippingTime
- **Objetivo:** Medir tempos médios de coleta, expedição e entrega.
- **Funcionalidades:**
  - Leitura de múltiplas abas de Excel.
  - Filtros por base/coordenador.
  - Exportação de arquivos filtrados por pasta.

### ✅ Entrega Realizada – Dia
- **Objetivo:** Monitorar entregas previstas vs realizadas.
- **Funcionalidades:**
  - Leitura de pedidos por base de entrega.
  - Cálculo de percentual entregue no prazo.
  - Exportação em `.xlsx`.

---

## 🛠️ Pré-requisitos

- Python **3.13** (padrão do ambiente)
- Bibliotecas principais:
  - `pandas`
  - `openpyxl`
  - `requests`
  - `numpy`
  - `logging`
  - `tqdm`

Instale tudo com:

```bash
pip install -r requirements.txt
