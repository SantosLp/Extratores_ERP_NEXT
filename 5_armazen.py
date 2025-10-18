import os
import json
import time
import logging
from typing import Dict, List, Optional
import requests
import pandas as pd
from dotenv import load_dotenv

# =================================================================================
# CONFIGURAÇÃO DE LOG
# =================================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="log_criador_armazens_erpnext.txt",  # Log específico
    filemode="w",
)

console = logging.getLogger("console_wh_erpnext")  # Nome diferente
console.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))

# Garantir que o handler de log esteja configurado
if not console.handlers:
    console.addHandler(console_handler)

# =================================================================================
# CONFIGURAÇÃO DO ERPNext
# =================================================================================
load_dotenv()

# Carregar variáveis do arquivo .env
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

# Verificar se as variáveis de ambiente essenciais estão configuradas
if not all([ERPNext_URL, API_KEY, API_SECRET]):
    console.error("!!! ERRO CRÍTICO: Variáveis ERPNext (URL, API_KEY, API_SECRET) ausentes!")
    exit(1)

console.info(f"Conectando ao ERPNext: {ERPNext_URL}")

# Cabeçalhos para requisições HTTP no ERPNext
HEADERS_ERPNext = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Connection": "keep-alive",
    "User-Agent": "CDC-WH-Prep/1.0",
}

COMPANY_NAME = "CDC"
ARQUIVO_MAPEAMENTO = r"C:\Users\Santos\Desktop\DataPath\EXTRATOR - ERP NEXT\centro_de_custo_armazen.csv"

# Controlo de espera
SHORT_WAIT_CREATE = 15  # Tempo curto para verificação otimista (segundos)
VERIFY_INTERVAL = 3  # Intervalo entre verificações (segundos)

# =================================================================================
# FUNÇÕES UTILITÁRIAS E REQUISIÇÕES HTTP
# =================================================================================
def validar_variaveis_ambiente():
    """Valida as variáveis de ambiente essenciais para a conexão com o ERPNext."""
    console.info("--- Validando variáveis de ambiente (ERPNext) ---")
    if not all([ERPNext_URL, API_KEY, API_SECRET]):
        msg = "ERRO CRÍTICO: Variáveis ERPNext (URL, API_KEY, API_SECRET) ausentes!"
        console.error(msg)
        logging.critical(msg)
        raise SystemExit(1)
    console.info("-> Variáveis OK.")

def carregar_mapeamento() -> pd.DataFrame:
    """Carrega o mapeamento de armazéns a partir de um arquivo CSV."""
    console.info(f"--- Carregando mapeamento de '{os.path.basename(ARQUIVO_MAPEAMENTO)}' ---")
    try:
        if not os.path.exists(ARQUIVO_MAPEAMENTO):
            raise FileNotFoundError(f"Arquivo não encontrado em: {ARQUIVO_MAPEAMENTO}")

        console.info(f"Lendo arquivo: {ARQUIVO_MAPEAMENTO}")
        df = pd.read_csv(ARQUIVO_MAPEAMENTO, sep=";", dtype=str, encoding="latin-1")
        df.columns = ["centro_custo", "armazem"]
        df = df.dropna(subset=["armazem"])
        df["armazem"] = df["armazem"].str.strip()
        df = df[df["armazem"] != ""]
        df = df.drop_duplicates(subset=["armazem"])
        console.info(f"-> {len(df)} Armazéns únicos carregados.")
        return df
    except FileNotFoundError as e:
        msg = f"ERRO CRÍTICO: {e}"
        console.error(msg)
        logging.critical(msg)
        raise SystemExit(1)
    except Exception as e:
        msg = f"ERRO CRÍTICO ao ler mapeamento: {e}"
        console.error(msg)
        logging.critical(msg)
        raise SystemExit(1)

def erp_request(method: str, path: str, params: Dict = None, payload: Dict = None, timeout=60) -> requests.Response:
    """Função genérica para enviar requisições HTTP para o ERPNext."""
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
    """Verifica se um documento existe pelo nome/ID exato."""
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
    """Tenta criar e faz uma verificação otimista (assume sucesso se POST foi enviado)."""
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
def criar_armazens(df_mapeamento: pd.DataFrame) -> bool:
    """Cria ou verifica a existência de armazéns no ERPNext com base no mapeamento fornecido."""
    console.info("\n--- Iniciando criação/verificação de Armazéns no ERPNext ---")
    sucesso_geral = True
    total_linhas = len(df_mapeamento)
    criados_count = 0
    falha_count = 0
    ja_existia_count = 0

    # Itera pelos nomes únicos de armazém
    for index, nome_armazem in enumerate(df_mapeamento["armazem"].unique()):
        if not nome_armazem:
            continue  # Pula armazéns com nome vazio

        console.info(f"\n({index + 1}/{total_linhas}) Processando Armazém: '{nome_armazem}'")

        if erp_doc_exists("Warehouse", nome_armazem):
            console.info(f"    -> Armazém '{nome_armazem}' já existe.")
            ja_existia_count += 1
            continue

        # Payload para criação do Armazém
        payload_wh = {
            "warehouse_name": nome_armazem,
            "company": COMPANY_NAME,
            "is_group": 0,  # Assume que são armazéns finais, não grupos
        }

        # Tenta criar e verificar com sucesso otimista
        if ensure_erp_doc_com_verificacao_otimista("Warehouse", nome_armazem, payload_wh):
            criados_count += 1
        else:
            falha_count += 1
            sucesso_geral = False  # Marca como falha se algum POST falhar

    console.info("\n--- Resumo da Criação ---")
    console.info(f"Armazéns criados (ou POST OK): {criados_count}")
    console.info(f"Armazéns já existentes: {ja_existia_count}")
    console.info(f"Falhas no comando POST: {falha_count}")

    if sucesso_geral:
        console.info("\n--- Criação/Verificação de Armazéns concluída com sucesso ---")
    else:
        console.warning("\n!!! Criação/Verificação de Armazéns concluída com falhas. Verifique o log.")

    return sucesso_geral

# =================================================================================
# EXECUÇÃO PRINCIPAL
# =================================================================================
if __name__ == "__main__":
    console.info("\n====== INICIANDO CRIADOR DE ARMAZÉNS ERPNEXT (v1.0) ======")
    validar_variaveis_ambiente()
    mapeamento_df = carregar_mapeamento()
    iniciar = time.time()
    sucesso = False
    try:
        sucesso = criar_armazens(mapeamento_df)
    except SystemExit:
        console.error("\n!!! Execução abortada devido a erros críticos iniciais.")
    except Exception as e:
        console.error(f"\n!!! Erro inesperado: {e}")
        logging.exception("Erro inesperado na execução principal")
    finally:
        if sucesso:
            console.info(f"\n====== CRIAÇÃO DE ARMAZÉNS CONCLUÍDA com sucesso em {time.time() - iniciar:.1f}s ======")
        else:
            console.error(f"\n====== CRIAÇÃO DE ARMAZÉNS FINALIZADA com erros em {time.time() - iniciar:.1f}s ======")
