import os
import json
import time
import logging
from typing import Dict, List
import requests
import pandas as pd
from dotenv import load_dotenv

# =================================================================================
# CONFIGURAÇÃO DE LOG
# =================================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="log_criador_cc_erpnext.txt",
    filemode="w",
)

console = logging.getLogger("console_cc_erpnext")
console.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))

# Garantir que o handler de log esteja configurado
if not console.handlers:
    console.addHandler(console_handler)

# =================================================================================
# CONFIGURAÇÃO DO ERPNext
# =================================================================================
# Carregar variáveis de ambiente
load_dotenv()

# Obter variáveis do arquivo .env
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")

# Verificar se as variáveis estão configuradas corretamente
if not all([ERPNext_URL, API_KEY, API_SECRET]):
    console.error("!!! ERRO CRÍTICO: Variáveis ERPNext (URL, API_KEY, API_SECRET) ausentes no arquivo .env !!!")
    exit(1)

console.info(f"Conectando ao ERPNext: {ERPNext_URL}")

# Cabeçalhos para requisições HTTP no ERPNext
HEADERS_ERPNext = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Connection": "keep-alive",
    "User-Agent": "CDC-CC-Prep/1.1",
}

COMPANY_NAME = "CDC"
PARENT_COST_CENTER = "CDC - CDC"
ARQUIVO_MAPEAMENTO = r"C:\Users\Santos\Desktop\DataPath\EXTRATOR - ERP NEXT\centro_de_custo_armazen.csv"

# Controlo de espera
SHORT_WAIT_CREATE = 15  # Tempo para verificação otimista (segundos)
VERIFY_INTERVAL = 3     # Intervalo entre tentativas de verificação

# =================================================================================
# FUNÇÕES UTILITÁRIAS
# =================================================================================
def validar_variaveis_ambiente():
    """Valida se as variáveis de ambiente necessárias estão carregadas."""
    console.info("--- Validando variáveis de ambiente (ERPNext) ---")
    if not all([ERPNext_URL, API_KEY, API_SECRET]):
        msg = "ERRO CRÍTICO: Variáveis ERPNext (URL, API_KEY, API_SECRET) ausentes!"
        console.error(msg)
        logging.critical(msg)
        raise SystemExit(1)
    console.info("-> Variáveis de ambiente OK.")

def carregar_mapeamento() -> pd.DataFrame:
    """Carrega o mapeamento de centros de custo a partir do arquivo CSV."""
    console.info(f"--- Carregando mapeamento de '{os.path.basename(ARQUIVO_MAPEAMENTO)}' ---")
    try:
        if not os.path.exists(ARQUIVO_MAPEAMENTO):
            raise FileNotFoundError(f"Arquivo não encontrado: {ARQUIVO_MAPEAMENTO}")

        console.info(f"Lendo arquivo: {ARQUIVO_MAPEAMENTO}")
        df = pd.read_csv(ARQUIVO_MAPEAMENTO, sep=";", dtype=str, encoding="latin-1")
        df.columns = ["centro_custo", "nome_armazem"]
        df = df.dropna(subset=["centro_custo", "nome_armazem"])
        df["centro_custo"] = df["centro_custo"].str.strip()
        df["nome_armazem"] = df["nome_armazem"].str.strip()
        df = df[df["centro_custo"] != ""]
        df = df[df["nome_armazem"] != ""]
        df["nome_cc_erpnext"] = df["centro_custo"] + " - " + df["nome_armazem"]
        df = df.drop_duplicates(subset=["nome_cc_erpnext"])
        console.info(f"-> {len(df)} centros de custo únicos carregados.")
        return df
    except FileNotFoundError as e:
        msg = f"ERRO CRÍTICO: {e}"
        console.error(msg)
        logging.critical(msg)
        raise SystemExit(1)
    except Exception as e:
        msg = f"Erro ao ler mapeamento: {e}"
        console.error(msg)
        logging.critical(msg)
        raise SystemExit(1)

def erp_request(method: str, path: str, params: Dict = None, payload: Dict = None, timeout=60) -> requests.Response:
    """Função genérica para enviar requisições ao ERPNext."""
    url = f"{ERPNext_URL.rstrip('/')}/{path.lstrip('/')}"
    if not path.startswith("api/resource/"):
        url = f"{ERPNext_URL.rstrip('/')}/api/resource/{path.lstrip('/')}"
    
    try:
        response = requests.request(method, url, headers=HEADERS_ERPNext, params=params, json=payload, timeout=timeout)
        return response
    except requests.exceptions.RequestException as e:
        msg = f"Erro de conexão com o ERPNext: {e}"
        console.error(f"!!! {msg}")
        logging.error(msg)
        response = requests.Response()
        response.status_code = 503
        return response

def erp_doc_exists(doctype: str, docname: str) -> bool:
    """Verifica se o documento existe no ERPNext."""
    path = f"api/resource/{doctype}/{docname}"
    r = erp_request("GET", path)
    if r.status_code == 404:
        return False
    elif r.status_code == 200:
        return True
    else:
        filters = [["name", "=", docname]]
        params = {"filters": json.dumps(filters), "limit_page_length": 1}
        r_filter = erp_request("GET", f"api/resource/{doctype}", params=params)
        if r_filter.status_code == 200 and r_filter.json().get("data"):
            return True
        logging.warning(f"Erro inesperado ao verificar {doctype} '{docname}': {r.status_code}")
        return False

def ensure_erp_doc_com_verificacao_otimista(doctype: str, docname: str, payload: Dict) -> bool:
    """Tenta criar o documento e faz uma verificação otimista após o POST."""
    if erp_doc_exists(doctype, docname):
        console.info(f"    -> {doctype} '{docname}' já existe.")
        return True

    console.info(f"    -> Criando {doctype}: '{docname}'")
    create_r = erp_request("POST", f"api/resource/{doctype}", payload=payload)

    if create_r.status_code in [200, 201, 409, 417]:
        console.info(f"       ... Comando POST enviado (status {create_r.status_code}). Tentando verificar por {SHORT_WAIT_CREATE}s...")
        t0 = time.time()
        verified = False
        while time.time() - t0 < SHORT_WAIT_CREATE:
            time.sleep(VERIFY_INTERVAL)
            if erp_doc_exists(doctype, docname):
                console.info("       -> Confirmado!")
                verified = True
                break
        if not verified:
            msg = f"AVISO: Confirmação da criação de {doctype} '{docname}' demorou mais de {SHORT_WAIT_CREATE}s. Assumindo sucesso baseado no POST."
            console.warning(f"       ... {msg}")
            logging.warning(msg)
        return True
    else:
        msg = f"Falha ao enviar comando de criação para {doctype} '{docname}': {create_r.status_code} {create_r.text[:300]}"
        console.error(f"    !!! {msg}")
        logging.error(msg)
        return False

# =================================================================================
# LÓGICA PRINCIPAL
# =================================================================================
def criar_centros_custo(df_mapeamento: pd.DataFrame) -> bool:
    """Cria centros de custo no ERPNext com base no mapeamento fornecido."""
    console.info("\n--- Iniciando criação de Centros de Custo ---")
    sucesso_geral = True

    # Garantir o Centro de Custo Pai
    console.info("\n--- Garantindo Centro de Custo Pai ---")
    payload_cc_pai = {"cost_center_name": PARENT_COST_CENTER, "company": COMPANY_NAME, "is_group": 1}
    if not ensure_erp_doc_com_verificacao_otimista("Cost Center", PARENT_COST_CENTER, payload_cc_pai):
        console.error("!!! Falha crítica ao garantir o Centro de Custo Pai. Abortando.")
        sucesso_geral = False
        return sucesso_geral

    # Garantir Centros de Custo filhos
    console.info("\n--- Garantindo Centros de Custo (filhos) ---")
    criados_count, falha_count, ja_existia_count = 0, 0, 0
    total_linhas = len(df_mapeamento)

    for index, row in df_mapeamento.iterrows():
        cc_codigo = row["centro_custo"]
        cc_armazem = row["nome_armazem"]
        cc_nome_erpnext = f"{cc_codigo} - {cc_armazem}"

        console.info(f"\n({index + 1}/{total_linhas}) Processando CC: '{cc_nome_erpnext}'")

        # Verifica se já existe
        if erp_doc_exists("Cost Center", cc_nome_erpnext):
            console.info(f"    -> Centro de Custo '{cc_nome_erpnext}' já existe.")
            ja_existia_count += 1
            continue

        # Se não existe, tenta criar com verificação otimista
        payload_cc = {
            "cost_center_name": cc_nome_erpnext,
            "parent_cost_center": PARENT_COST_CENTER,
            "company": COMPANY_NAME,
            "is_group": 0,
        }
        if ensure_erp_doc_com_verificacao_otimista("Cost Center", cc_nome_erpnext, payload_cc):
            criados_count += 1
        else:
            falha_count += 1
            sucesso_geral = False

    console.info("\n--- Resumo da Criação ---")
    console.info(f"Centros de Custo criados: {criados_count}")
    console.info(f"Centros de Custo já existentes: {ja_existia_count}")
    console.info(f"Falhas no comando POST: {falha_count}")

    if sucesso_geral:
        console.info("\n--- Criação de Centros de Custo concluída ---")
    else:
        console.warning("\n!!! Criação de Centros de Custo concluída com erros. Verifique o log.")

    return sucesso_geral

# =================================================================================
# EXECUÇÃO PRINCIPAL
# =================================================================================
if __name__ == "__main__":
    console.info("\n====== INICIANDO CRIADOR DE CENTROS DE CUSTO ERPNEXT ======")
    validar_variaveis_ambiente()
    mapeamento_df = carregar_mapeamento()
    iniciar = time.time()
    sucesso = False
    try:
        sucesso = criar_centros_custo(mapeamento_df)
    except SystemExit:
        console.error("\n!!! Execução abortada devido a erros críticos iniciais.")
    except Exception as e:
        console.error(f"\n!!! Erro inesperado: {e}")
        logging.exception("Erro inesperado na execução principal")
    finally:
        if sucesso:
            console.info(f"\n====== CRIAÇÃO DE CENTROS DE CUSTO CONCLUÍDA em {time.time() - iniciar:.1f}s ======")
        else:
            console.error(f"\n====== CRIAÇÃO DE CENTROS DE CUSTO FINALIZADA com erros em {time.time() - iniciar:.1f}s ======")
