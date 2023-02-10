# kedro-pipeline

Esse repositório contém uma aplicação Kedro que executa um pipeline de ETL dos dados. O projeto contém nodes que executam as seguintes tarefas:

- Download e extração dos arquivos para .csv (10 arquivos);
- Agregação de 8 dos 10 arquivos (semelhante a um UNION ALL);
- Performance de um OneHotEncoding para os tipos de ativos dos fundos de investimento; e
- Carregamento num banco PostgreSQL.

Confira um preview do pipeline:

![preview](https://github.com/ocamposfaria/kedro-pipeline/blob/main/pipeline_preview.jpg?raw=true)

