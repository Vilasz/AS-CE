import streamlit as st
import pandas as pd
import time
import subprocess
import sys
import os
import json

st.set_page_config(page_title="Dashboard de Computação Escalável", layout="wide")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from solution_multiprocessing.processor import run_analysis as run_multiprocessing_analysis
from solution_message_broker.processor import run_analysis as run_broker_analysis
from solution_spark.processor import run_spark_analysis as run_spark_analysis

def calculate_correctness(ground_truth: list, found: list) -> dict:
    """Calcula apenas o número de anomalias geradas e encontradas."""
    gt_set = { (d['timestamp'], d['station_id']) for d in ground_truth }
    found_set = { (d['timestamp'], d['station_id']) for d in found }

    return {
        "Anomalias Geradas": len(gt_set),
        "Anomalias Encontradas": len(found_set)
    }

st.title(" Experimento de Comparação de Modelos de Paralelismo")

with st.sidebar:
    st.header(" Parâmetros do Experimento")
    
    num_events = st.number_input(
        "Número de Eventos a Gerar", 
        min_value=1000, max_value=500000, value=10000, step=1000, 
        help="Define o volume de dados a ser processado."
    )
    
    anomaly_perc = st.slider(
        "Percentual de Anomalias (%)", 
        min_value=0.0, max_value=50.0, value=5.0, step=1.0, 
        help="Percentual de eventos que serão gerados como anomalias."
    )
    
    parallelism_degrees = st.multiselect(
        "Graus de Paralelismo a Testar", 
        options=[1, 2, 4, 8, 12, 16], 
        default=[1, 2, 4],
        help="Unidades de processamento (processos/workers/cores) para cada teste."
    )

    start_button = st.button(" Iniciar Experimento", type="primary", use_container_width=True)

status_placeholder = st.empty()
tab1, tab2 = st.tabs([" Desempenho (Tempo de Execução)", " Corretude das Anomalias"])

with tab1:
    st.header("Gráfico de Desempenho")
    chart_placeholder = st.empty()
    st.header("Tabela de Tempos (segundos)")
    results_table_placeholder = st.empty() 

with tab2:
    st.header("Métricas de Corretude por Execução")
    correctness_placeholder = st.empty()

if start_button:
    # Limpa os resultados e placeholders da tela
    status_placeholder.empty(); chart_placeholder.empty(); results_table_placeholder.empty(); correctness_placeholder.empty()

    # --- Etapa de Geração de Dados (Não-Bloqueante) ---
    command = [
        'python', '-m', 'data_generator.generator',
        '--events', str(num_events),
        '--anomaly_perc', str(anomaly_perc)
    ]
    
    # Inicia o processo gerador em segundo plano com Popen
    generator_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Usa st.spinner para mostrar um feedback visual enquanto espera
    with st.spinner(f"Gerando {num_events} eventos... Isso pode levar um momento."):
        # Espera o processo terminar, verificando seu status sem bloquear o Streamlit
        while generator_process.poll() is None:
            time.sleep(1) # Pequena pausa para não sobrecarregar o CPU
            
    # Verifica se o processo terminou com erro
    stdout, stderr = generator_process.communicate()
    if generator_process.returncode != 0:
        status_placeholder.error("Ocorreu um erro durante a geração de dados:")
        st.code(stderr)
        st.stop() # Interrompe a execução se a geração falhou
    else:
        status_placeholder.info("Geração de dados concluída com sucesso.")
        print(stdout) # Opcional: mostra a saída do gerador no console

    # --- Continuação do Experimento ---
    
    anomaly_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'generated_anomalies.json')
    try:
        with open(anomaly_file_path, 'r') as f:
            ground_truth_anomalies = json.load(f)
    except FileNotFoundError:
        status_placeholder.error("Arquivo de anomalias não encontrado. A geração de dados falhou.")
        st.stop()

    performance_data, correctness_data = [], []
    data_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'synthetic_data.csv')
    solutions = {
        "Multiprocessing": run_multiprocessing_analysis,
        "Message Broker": run_broker_analysis,
        "Apache Spark": run_spark_analysis
    }

    # O resto do loop de execução permanece o mesmo...
    for degree in sorted(parallelism_degrees):
        for name, analysis_func in solutions.items():
            status_placeholder.info(f"Executando '{name}' com grau de paralelismo {degree}...")
            
            exec_time, found_anomalies = analysis_func(data_file_path, degree)
            
            # --- Atualiza Desempenho ---
            performance_data.append({"Abordagem": name, "Grau de Paralelismo": degree, "Tempo (s)": exec_time})
            df_performance = pd.DataFrame(performance_data).pivot(index="Grau de Paralelismo", columns="Abordagem", values="Tempo (s)")
            with tab1:
                results_table_placeholder.dataframe(df_performance, use_container_width=True)
                chart_placeholder.line_chart(df_performance)

            # --- Atualiza Corretude ---
            correctness_metrics = calculate_correctness(ground_truth_anomalies, found_anomalies)
            correctness_metrics.update({"Abordagem": name, "Grau de Paralelismo": degree})
            correctness_data.append(correctness_metrics)
            df_correctness = pd.DataFrame(correctness_data)[
                ["Abordagem", "Grau de Paralelismo", "Anomalias Geradas", "Anomalias Encontradas"]
            ]
            with tab2:
                correctness_placeholder.dataframe(df_correctness, use_container_width=True, hide_index=True)

    status_placeholder.success("✅ Experimento concluído!")