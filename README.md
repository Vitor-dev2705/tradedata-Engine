#  TradeData-Engine | Modern Data Stack para Trading

O **TradeData-Engine** é um ecossistema de dados *end-to-end* projetado para capturar, processar e analisar ativos do mercado financeiro em tempo real. Utilizando o conceito de **Modern Data Stack (MDS)** e a **Arquitetura de Medalhão**, o projeto transforma dados brutos em inteligência acionável via alertas automatizados e visualização analítica.



---

##  1. Arquitetura e Engenharia de Dados

O projeto utiliza a **Medallion Architecture**, um padrão de design que organiza os dados em camadas de refinamento, garantindo a rastreabilidade e a qualidade da informação. Tudo é centralizado no **DuckDB**, um banco de dados analítico (OLAP) de alta performance.

###  Camada Bronze (Raw)
* **Fonte:** Dados extraídos via API `yfinance`.
* **Processo:** Os dados são salvos em arquivos JSON com *timestamps* de extração, mantendo o histórico imutável dos dados originais (**Single Source of Truth**).

###  Camada Silver (Cleaned)
* **Processo:** O motor de transformação lê os JSONs, normaliza os tipos de dados (Floats, Timestamps), trata valores nulos e converte para o formato tabular no DuckDB.
* **Objetivo:** Oferecer uma base de dados limpa e pronta para cálculos estatísticos.

###  Camada Gold (Curated/Analytics)
* **Processo:** Aplicação do algoritmo de **Price Action**. O sistema identifica níveis de Suporte e Resistência através de janelas de volatilidade dinâmica.
* **Cálculo de Score de Força:** Atribui um peso (Strength) baseado no volume de toques históricos. Quanto mais vezes o preço respeitou o nível, maior a relevância da zona.



---

## 🛠️ 2. Tecnologias & Escolhas Técnicas

* **Python 3.12+:** Core do sistema, utilizando processamento vetorizado para performance.
* **DuckDB:** Escolhido por ser orientado a colunas (Columnar Storage), permitindo que as queries analíticas na camada Gold sejam processadas em milissegundos.
* **Telegram Bot API:** Integração assíncrona para entrega de alertas críticos sem latência humana.
* **Streamlit:** Interface reativa para visualização das zonas de liquidez e métricas do mercado.

---

##  3. O "Cérebro" do Sistema: Monitoramento & Alertas

O diferencial do TradeData-Engine é o seu loop de **Sentinela Automática** (`run_system.py`), que orquestra o pipeline continuamente:

1.  **Ingestão:** Aciona o `extract.py` para capturar os preços atuais (OHLCV).
2.  **Processamento:** O DuckDB atualiza as tabelas de métricas diárias.
3.  **Análise de Zonas:** O `MarketBrain` varre o histórico em busca de zonas de força.
4.  **Lógica de Proximidade:** O sistema calcula a distância percentual em tempo real:
    $$distancia = \frac{|PrecoAtual - PrecoZona|}{PrecoZona}$$
5.  **Gatilho:** Se $distancia \le 0.005$ (0.5%) e $Strength \ge 10$, o alerta é disparado via Webhook para o Telegram.



---

##  4. Como Executar o Ecossistema

### Passo 1: Configuração do Ambiente
```bash
# Clone o repositório
git clone [https://github.com/seu-usuario/tradedata-engine.git](https://github.com/seu-usuario/tradedata-engine.git)

# Instale as dependências
pip install -r requirements.txt
```
### Passo 2: O Motor de Dados (Backend)
Rode o sistema principal. Ele executará o ciclo completo de extração e análise em loop.
```
python run_system.py
```

### Passo 3: Visualização Analítica (Frontend)

Abra o dashboard para visualizar as zonas de suporte e resistência plotadas no gráfico.

```
streamlit run src/dashboard.py
```
