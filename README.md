# 📊 Fort Tech - Dashboard de Comissões e Vendas

Este projeto implementa um pipeline de dados completo para automatizar o fluxo de vendas da empresa fictícia **Fort Tech**, com ingestão via RDS, transformação em S3 e visualização final no Power BI.

---

## 🔁 Etapas do Pipeline

### 1. **Ingestão de Dados**
- Origem: PostgreSQL (RDS)
- Verificação de duplicidade por `id_venda`
- Salva novos dados na **camada Bronze** (S3, formato Parquet)

### 2. **Transformação (Silver)**
- Limpeza de dados
- Padronização e anonimização (se necessário)
- Salva dados tratados na **camada Silver**

### 3. **Carga (Gold/Redshift)**
- Exporta dados para o Redshift
- Visualização via Power BI

---

## 🛠️ Tecnologias Usadas

- **Python** (pandas, boto3, s3fs, pyarrow)
- **AWS RDS** – Origem de dados (PostgreSQL)
- **AWS S3** – Armazenamento Bronze / Silver
- **Amazon Redshift** – Camada final (Gold)
- **Power BI** – Visualização e análise
- **GitHub** – Repositório de versionamento
- **GitHub Actions / Lambda** (futuramente) – Agendamento automático

## 👨‍💻 Autor

Danillo • Especialista em BI, Engenheiro de Dados, Dev Python  
[LinkedIn](www.linkedin.com/in/danillo-r-8561192a2) • [GitHub](https://github.com/Dan-2912)
