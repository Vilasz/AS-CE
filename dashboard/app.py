# dashboard/app.py
import streamlit as st
import pandas as pd
import time
import subprocess
import sys
import os
import json

# --- Configuração Inicial ---
st.set_page_config(page_title="Dashboard de Computação Escalável", layout="wide")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Funções de Análise e Helpers ---
from solution_multiprocessing.processor import run_analysis as run_multiprocessing_analysis
from solution_message_broker.processor import run_analysis as run_broker_analysis
from solution_spark.processor import run_spark_analysis as run_spark_analysis

def calculate_correctness(ground_truth: list, found: list) -> dict:
    """Calcula a precisão e recall da detecção de anomalias."""
    gt_set = { (d['timestamp'], d['station_id']) for d in ground_truth }
    found_set = { (d['timestamp'], d['station_id']) for d in found }
    
    return {
        "Anomalias Geradas": len(gt_set), "Anomalias Encontradas": len(found_set)
    }

# --- Interface do Usuário (UI) ---
st.title("🔬 Experimento de Comparação de Modelos de Paralelismo")

with st.sidebar:
    st.header("⚙️ Parâmetros do Experimento")
    
    # REQUISITO: Input para o número de eventos [cite: 58]
    num_events = st.number_input(
        "Número de Eventos a Gerar", 
        min_value=1000, max_value=500000, value=10000, step=1000, 
        help="Define o volume de dados a ser processado."
    )
    
    # REQUISITO: Input para o percentual de anomalias [cite: 42]
    anomaly_perc = st.slider(
        "Percentual de Anomalias (%)", 
        min_value=0.0, max_value=50.0, value=5.0, step=1.0, 
        help="Percentual de eventos que serão gerados como anomalias."
    )
    
    # REQUISITO: Input para os graus de paralelismo [cite: 56]
    parallelism_degrees = st.multiselect(
        "Graus de Paralelismo a Testar", 
        options=[1, 2, 4, 8, 12, 16], 
        default=[1, 2, 4],
        help="Unidades de processamento (processos/workers/cores) para cada teste."
    )

    # REQUISITO: Botão para iniciar o experimento [cite: 59]
    start_button = st.button("🚀 Iniciar Experimento", type="primary", use_container_width=True)

# --- Área de Exibição dos Resultados ---
status_placeholder = st.empty()
tab1, tab2 = st.tabs(["📊 Desempenho (Tempo de Execução)", "🎯 Corretude das Anomalias"])

with tab1:
    st.header("Gráfico de Desempenho")
    chart_placeholder = st.empty() # REQUISITO: Gráfico de paralelismo vs. tempo [cite: 61]
    st.header("Tabela de Tempos (segundos)")
    results_table_placeholder = st.empty() # REQUISITO: Tabela de tempos [cite: 60]

with tab2:
    st.header("Métricas de Corretude por Execução")
    correctness_placeholder = st.empty()

# --- Lógica Principal do Experimento ---
if start_button:
    status_placeholder.empty(); chart_placeholder.empty(); results_table_placeholder.empty(); correctness_placeholder.empty()

    status_placeholder.info("Infraestrutura pronta. Iniciando geração de dados...")
    time.sleep(1)

    # REQUISITO: Gerar dados com os parâmetros do usuário [cite: 47]
    status_placeholder.info(f"Gerando {num_events} eventos com {anomaly_perc:.1f}% de anomalias...")
    subprocess.run([
        'python', '-m', 'data_generator.generator',
        '--events', str(num_events),
        '--anomaly_perc', str(anomaly_perc)
    ])
    
    anomaly_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'generated_anomalies.json')
    with open(anomaly_file_path, 'r') as f:
        ground_truth_anomalies = json.load(f)

    performance_data, correctness_data = [], []
    data_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'synthetic_data.csv')
    solutions = {
        "Multiprocessing": run_multiprocessing_analysis,
        "Message Broker": run_broker_analysis,
        "Apache Spark": run_spark_analysis
    }

    # REQUISITO: Executar sequencialmente cada solução para cada grau de paralelismo [cite: 47]
    for degree in sorted(parallelism_degrees):
        for name, analysis_func in solutions.items():
            status_placeholder.info(f"Executando '{name}' com grau de paralelismo {degree}...")
            
            # REQUISITO: Computar tempo e comparar anomalias [cite: 48]
            exec_time, found_anomalies = analysis_func(data_file_path, degree)
            
            # REQUISITO: Atualizar informações em tempo real [cite: 62]
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
                ["Abordagem", "Grau de Paralelismo", "Precisão", "Recall", "Anomalias Geradas", 
                 "Anomalias Encontradas", "Verdadeiros Positivos", "Falsos Positivos", "Falsos Negativos"]
            ]
            with tab2:
                correctness_placeholder.dataframe(df_correctness, use_container_width=True, hide_index=True)

    status_placeholder.success("✅ Experimento concluído!")