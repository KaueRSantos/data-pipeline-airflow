# data-pipeline-airflow

## Visão Geral
Este projeto implementa um **pipeline de dados moderno** utilizando **Google Cloud Platform (GCP)**, com foco em ingestão, transformação e disponibilização de dados para análises de negócio.  



O pipeline integra dados de diferentes fontes (**Postgres CRM, Google Analytics 4, etc.**) e os disponibiliza de forma tratada e padronizada no **BigQuery**, seguindo as camadas:  
- **RAW** → ingestão bruta dos dados.  
- **ETL** → limpeza, padronização e enriquecimento.  
- **DELIVERY** → métricas, visões analíticas e dashboards.  

---

## Principais Componentes

- **Cloud Functions (Gen2)**  
  Extrai dados do Postgres (CRM) em *chunks* → grava Parquet no GCS → carrega no BigQuery.  

- **Cloud Composer (Airflow)**  
  Orquestra o pipeline:  
  1. Executa a Cloud Function.  
  2. Move dados de GCS → BigQuery RAW.  
  3. Executa queries de transformação (ETL).  
  4. Cria/atualiza views em Delivery.  

- **BigQuery**  
  - Dataset **RAW**: ingestão bruta.  
  - Dataset **ETL**: staging e padronização.  
  - Dataset **DELIVERY**: visões analíticas para dashboards.  

- **Data Catalog & Lineage**  
  Catálogo de metadados e rastreabilidade das tabelas e colunas.  

- **GitHub Actions (CI/CD)**  
  - Deploy automático da Cloud Function.  
  - Deploy automático das DAGs e SQLs do Composer.  
  - Variáveis sensíveis armazenadas em **GitHub Secrets**.  

---

## Estrutura de Pastas

├── dags/
│ ├── scr/
│ │ ├── main.py # Código da Cloud Function
│ │ └── requirements.txt # Dependências da função
│ ├── dag_crm_to_bq.py # DAG para ingestão Postgres → BigQuery
│ └── sql/
│ ├── etl/ # Queries de transformação ETL
│ └── delivery/ # Queries de views analíticas
├── .github/
│ └── workflows/
│ ├── deploy.yml # CI/CD para Cloud Function
│ └── sync-dags.yml # CI/CD para DAGs do Composer
├── README.md
