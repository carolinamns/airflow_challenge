# Airflow Challenge

Este repositório contém uma solução para um desafio de conhecimentos básicos sobre Orquestração de Dados em Airflow. O objetivo é extrair dados de uma tabela em um banco de dados SQLite, fazer uma junção com outra tabela, calcular a soma de uma coluna com base em uma condição e exportar o resultado final.

## Instalação

>As instruções fornecidas neste arquivo foram testadas e funcionam corretamente no sistema operacional Ubuntu. Se você estiver usando um sistema operacional diferente, pode ser necessário fazer ajustes nas instruções para garantir a compatibilidade.

Para instalar e executar o Airflow, siga os seguintes passos:

1. Clone este repositório em sua máquina local.
```
git clone https://github.com/carolinamns/airflow_challenge.git
```

2. Acesse a pasta do projeto:
```
cd airflow_challenge_carolina
```

3. Crie um ambiente virtual.
```
virtualenv venv -p python3
```

4. Ative o ambiente virtual:
```
source venv/bin/activate
```

5. Instale as dependências do projeto:
```
pip install -r requirements.txt
```

6. Execute o script de instalação do Airflow:
```
bash install.sh
```

6. Inicie o Airflow.
```
airflow standalone
```

7. Acesse o Airflow em seu navegador através do endereço 'http://localhost:8080' e verifique se a interface está funcionando corretamente.

## Como usar

1. Crie uma conexão com a base de dados SQLite. Para isso, acesse a interface do Airflow e vá em Admin > Connections. Clique em "Create" e preencha os campos conforme as informações abaixo:

- Conn Id: sqlite
- Conn Type: SQLite
- Host: /path/to/northwind_small.sqlite (substitua pelo caminho completo para o arquivo northwind_small.sqlite em sua máquina)
- Schema: deixe em branco
- Login: deixe em branco
- Password: deixe em branco
- Port: deixe em branco
- Clique em "Save".

2. Ative o DAG "DesafioAirflow" na interface do Airflow. Ele deve estar na aba "DAGs" do menu lateral.

3. Execute o DAG clicando em "Trigger DAG" na interface do Airflow.

4. Verifique o arquivo final_output.txt para obter o resultado final.

## Arquivos

- `install.sh`: script que instala o Airflow e cria o banco de dados.
- `requirements.txt`: arquivo que contém as dependências do projeto.
- `/data/northwind_small.sqlite`: banco de dados SQLite utilizado no desafio.
- `/airflow-data/dags/airflow_challenge.py`: arquivo Python que define a DAG.
- `/airflow-data/dags/extract_and_count.py`: arquivo Python com as funções utilizadas na DAG.
- `final_output.txt`: arquivo de saída gerado pelo DAG contendo o resultado final.