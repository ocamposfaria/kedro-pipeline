# kedro-pipeline

Esse repositório contém uma aplicação Kedro que executa um pipeline de extração e carregamento de dados. O projeto contém nodes que executam as seguintes tarefas:

- Download e extração dos arquivos para .csv (10 arquivos);
- Agregação de 8 dos 10 arquivos (semelhante a um UNION ALL);
- Performance de um OneHotEncoding para os tipos de ativos dos fundos de investimento; e
- Carregamento num banco PostgreSQL.

Confira um preview do pipeline:

![preview](https://github.com/ocamposfaria/kedro-pipeline/blob/main/pipeline_preview.jpg?raw=true)

A aplicação foi containerizada com Docker. Para executá-la, é necessário ativar o virtual environment. Para isso, na raiz do cmd:

```
cd .\env\Scripts\
.\activate
cd ..
cd ..
```

Em seguida, execute o container:

```
cd .\data-engineering-pipeline\
kedro docker run
```

**ATENÇÃO:** a aplicação só irá executar após as credencias serem atualizadas no caminho a seguir!  
[\data-engineering-pipeline\conf\local\credentials.yml](\data-engineering-pipeline\conf\local\credentials.yml)
