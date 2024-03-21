# kedro-pipeline

Esse repositório contém uma aplicação Kedro que executa um pipeline de extração e carregamento de dados. O projeto contém nodes que executam as seguintes tarefas:

- Download e extração dos arquivos para .csv (10 arquivos);
- Agregação de 8 dos 10 arquivos (semelhante a um UNION ALL);
- Performance de um OneHotEncoding para os tipos de ativos dos fundos de investimento; e
- Carregamento num banco PostgreSQL.

![alt text](/pipeline_preview_new.jpg)


A aplicação foi containerizada utilizando a biblioteca kedro-docker. Para executá-la, é necessário ativar o virtual environment e, em seguida, executar o container.

```
cd ./env/Scripts/ && ./activate && cd ../../data-engineering-pipeline/ && kedro docker run
```

**ATENÇÃO:** a aplicação só irá executar após as credencias serem atualizadas no caminho a seguir!  
[\data-engineering-pipeline\conf\local\credentials.yml](https://github.com/ocamposfaria/kedro-pipeline/tree/main/data-engineering-pipeline/conf/local)
