# dashboard/app.py
import streamlit as st
import pandas as pd
import time
import subprocess
import sys
import os

# Adiciona o diretório raiz ao path para que possamos importar os módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importa as funções de análise de cada solução
from solution_multiprocessing.processor import run_analysis as run_multiprocessing_analysis
from solution_message_broker.processor import run_analysis as run_broker_analysis
from solution_spark.processor import run_spark_analysis

# --- Configuração da Página ---
st.set_page_config(page_title="Dashboard de Computação Escalável", layout="wide")
st.title("Experimento de Comparação de Modelos de Paralelismo")

# --- Controles do Experimento na Barra Lateral ---
with st.sidebar:
    st.header("Parâmetros do Experimento")
    num_events = st.number_input("Número de Eventos a Gerar", min_value=1000, max_value=100000, value=10000, step=1000)
    parallelism_degrees = st.multiselect("Graus de Paralelismo a Testar", options=[1, 2,3, 4,5,6,7, 8,9,10,11,12], default=[1, 2, 4])
    start_button = st.button("Iniciar Experimento", type="primary")

# --- Área de Exibição dos Resultados ---
st.header("Resultados do Experimento")
status_placeholder = st.empty()
results_table_placeholder = st.empty()
chart_placeholder = st.empty()

# --- Lógica do Experimento ---
if start_button:
    # Garante que os containers necessários estão rodando
    status_placeholder.info("Verificando e iniciando a infraestrutura (Docker)...")
    subprocess.run(['docker-compose', 'up', '-d'])
    time.sleep(5) # Espera os serviços subirem

    status_placeholder.info(f"Gerando {num_events} eventos sintéticos...")
    # (Idealmente, o generator.py seria adaptado para aceitar 'num_events' como argumento)
    subprocess.run(['python', '-m', 'data_generator.generator'])
    
    results_data = []
    # Cria um DataFrame vazio para ir populando
    df_results = pd.DataFrame(columns=["Abordagem", "Grau de Paralelismo", "Tempo (s)"])

    data_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'synthetic_data.csv')

    for degree in sorted(parallelism_degrees):
        status_placeholder.info(f"Executando testes com grau de paralelismo: {degree}...")
        
        # Multiprocessing
        st.write(f"- Testando Multiprocessing com {degree} processo(s)...")
        mp_time = run_multiprocessing_analysis(data_file_path, degree)
        new_row = {"Abordagem": "Multiprocessing", "Grau de Paralelismo": degree, "Tempo (s)": mp_time}
        results_data.append(new_row)
        df_results = pd.DataFrame(results_data)
        results_table_placeholder.dataframe(df_results, use_container_width=True)
        chart_placeholder.line_chart(df_results.pivot(index="Grau de Paralelismo", columns="Abordagem", values="Tempo (s)"))

        # Message Broker
        st.write(f"- Testando Message Broker com {degree} worker(s)...")
        mb_time = run_broker_analysis(data_file_path, degree)
        new_row = {"Abordagem": "Message Broker", "Grau de Paralelismo": degree, "Tempo (s)": mb_time}
        results_data.append(new_row)
        df_results = pd.DataFrame(results_data)
        results_table_placeholder.dataframe(df_results, use_container_width=True)
        chart_placeholder.line_chart(df_results.pivot(index="Grau de Paralelismo", columns="Abordagem", values="Tempo (s)"))

        # Spark
        st.write(f"- Testando Spark com {degree} core(s)...")
        spark_time = run_spark_analysis(data_file_path, degree)
        new_row = {"Abordagem": "Apache Spark", "Grau de Paralelismo": degree, "Tempo (s)": spark_time}
        results_data.append(new_row)
        df_results = pd.DataFrame(results_data)
        results_table_placeholder.dataframe(df_results, use_container_width=True)
        chart_placeholder.line_chart(df_results.pivot(index="Grau de Paralelismo", columns="Abordagem", values="Tempo (s)"))

    status_placeholder.success("Experimento concluído!")
    st.balloons()