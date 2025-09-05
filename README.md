# Projeto Datalake Movies 

Esse projeto tem o intuito de analisar informações sobre filmes que estão em cartaz no cinema. Informando sobre,  os gêneros de filmes que estão disponíveis neste momento no cinema, popularidade desses filmes, quantidade de idiomas disponíveis e quantidade de filmes em cartaz. Os dados foram extraídos da api do [tmdb](https://www.themoviedb.org/). 

Nesse projeto, consegui aplicar processamento de dados utilizando o PySpark, orquestração de eventos com o Airflow e construção de containers utilizando o Docker 

Para acessar o dashboard [clique aqui](https://app.powerbi.com/view?r=eyJrIjoiY2U3YzM1NDEtOTMyYy00ZmRjLTkwZGEtMTIzMDBmMTg0YjhhIiwidCI6IjQzZTMwMDFiLTU2YWItNGMwNC04NGI1LTQ2NjVlMjBiNDU2MCJ9)

# Como rodar o projeto

Primeiro, instale o [Docker](https://www.docker.com/) em sua máquina.

Agora, garanta que a pasta /spark-data tenha permissão do seu sistema operacional para realizar leitura e escrita de arquivos, se estiver utilizando o Linux faça o seguinte procedimento: 
Digite o comando: sudo chown -R $USER:$USER ./spark-data  
Depois que o comando anterior for executado, digite esse comando: chmod -R 777 ./spark-data

Com isso checado, digite o seguinte comando no terminal: docker-compose up --build.  

Com os containainers criados, entre no terminal do container spark_master e digite o seguinte comando: docker exec -it spark_master pip install requests  

Faça o mesmo para o container spark_worker_1: docker exec -it spark_worker_1 pip install requests 

Para o spark_worker_2: docker exec -it spark_worker_2 pip install requests
