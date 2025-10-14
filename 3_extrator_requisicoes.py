import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
from typing import List, Dict, Set
import json
import time # Importa a biblioteca de tempo

# --- CONFIGURAÇÕES ---
load_dotenv()
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")

ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

# --- CONFIGURAÇÕES DO ERPNEXT ---
COMPANY_NAME = "CDC"
PARENT_COST_CENTER = "CDC - CDC" 
WAREHOUSE_ALVO = os.getenv("ERPNext_WAREHOUSE") 

if not all([ERPNext_URL, API_KEY, API_SECRET, ONGSYS_URL_BASE, ONGSYS_USER, ONGSYS_PASS, WAREHOUSE_ALVO]):
    print("!!! ERRO CRÍTICO: Verifique TODAS as variáveis de ambiente no arquivo .env (incluindo ERPNext_WAREHOUSE) !!!")
    exit(1)

HEADERS_ERPNext = {"Authorization": f"token {API_KEY}:{API_SECRET}", "Content-Type": "application/json"}
HEADERS_ONGSYS = {'User-Agent': 'Mozilla/5.0'}

# --- FUNÇÕES HELPERS ---

def erp_request(method: str, path: str, payload: Dict = None) -> requests.Response:
    """Função unificada para fazer requisições à API do ERPNext, retornando o objeto de resposta completo."""
    url = f"{ERPNext_URL.rstrip('/')}/api/resource/{path}"
    try:
        response = requests.request(method, url, headers=HEADERS_ERPNext, json=payload, timeout=60)
        return response
    except Exception as e:
        print(f"!!! ERRO INESPERADO DE REDE: {e}")
        response = requests.Response()
        response.status_code = 500
        return response

# "Memória" para os Centros de Custo já verificados nesta execução
centros_de_custo_verificados = set()

def ensure_cost_center(cost_center_name: str):
    """Verifica e cria o Centro de Custo, se necessário, de forma robusta."""
    if not cost_center_name or cost_center_name in centros_de_custo_verificados:
        return
    
    get_response = erp_request("GET", f"Cost Center/{cost_center_name}")

    if get_response.status_code == 200:
        print(f" -> Centro de Custo '{cost_center_name}' já existe.")
        centros_de_custo_verificados.add(cost_center_name)
        return

    if get_response.status_code == 404:
        print(f" -> Centro de Custo '{cost_center_name}' não encontrado. Tentando criar...")
        payload = {
            "cost_center_name": cost_center_name,
            "parent_cost_center": PARENT_COST_CENTER,
            "is_group": 0,
            "company": COMPANY_NAME
        }
        
        post_response = erp_request("POST", "Cost Center", payload)

        if post_response.status_code == 200:
            print(f" -> Centro de Custo '{cost_center_name}' criado com sucesso.")
            print("    ...aguardando 2 segundos para consistência do banco de dados.")
            time.sleep(2) # Pausa estratégica para evitar a condição de corrida
        elif post_response.status_code == 409:
            print(f" -> Centro de Custo '{cost_center_name}' já existe (detectado durante a criação).")
        else:
            print(f"\n\n!!! ERRO CRÍTICO AO TENTAR CRIAR O CENTRO DE CUSTO '{cost_center_name}' !!!")
            print(f"STATUS: {post_response.status_code}, MENSAGEM: {post_response.text}")
            exit(1)
            
        centros_de_custo_verificados.add(cost_center_name)
    else:
        print(f"!!! ERRO INESPERADO ao verificar Centro de Custo '{cost_center_name}': {get_response.text}")
        exit(1)


# --- FUNÇÕES PRINCIPAIS ---

def extrair_requisicoes_ongsys() -> List[Dict]:
    print("--- ETAPA 1: Extraindo PEDIDOS da API do ONGSYS ---")
    endpoint = f"{ONGSYS_URL_BASE.rstrip('/')}/pedidos"
    pagina_atual, todos_pedidos = 1, []
    while True:
        print(f"Buscando dados da página: {pagina_atual}...")
        try:
            response = requests.get(endpoint, auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params={'pageNumber': pagina_atual}, headers=HEADERS_ONGSYS, timeout=30)
            if response.status_code == 422: print("-> Fim dos dados (422)."); break
            response.raise_for_status()
            pedidos_da_pagina = response.json().get('data', [])
            if not pedidos_da_pagina: print("-> Página vazia, fim."); break
            
            pedidos_produto = [p for p in pedidos_da_pagina if p.get("tipoPedido") == "Produto"]
            pedidos_finalizados = [p for p in pedidos_produto if p.get("statusPedido") == "Ordem finalizada"]
            
            todos_pedidos.extend(pedidos_finalizados)
            print(f" -> {len(pedidos_da_pagina)} registros | {len(pedidos_produto)} de produto | {len(pedidos_finalizados)} finalizados.")
            pagina_atual += 1
        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}"); break
    print(f"\n-> Extração concluída: {len(todos_pedidos)} pedidos FINALIZADOS encontrados.")
    return todos_pedidos

def get_lancamentos_existentes_erpnext() -> Set[str]:
    print("--- ETAPA 2: Verificando Lançamentos existentes ---")
    response = erp_request("GET", "Stock Entry?fields=[\"custom_id_ongsys\"]&limit=9999")
    if response.status_code == 200:
        data = response.json()
        ids_existentes = {str(req['custom_id_ongsys']) for req in data.get('data', []) if req.get('custom_id_ongsys')}
        print(f"-> {len(ids_existentes)} lançamentos encontrados no ERPNext.")
        return ids_existentes
    else:
        print(f"!!! AVISO: Não foi possível buscar lançamentos existentes: {response.text}")
        return set()

def transformar_requisicoes_para_erpnext(pedidos: List[Dict], existentes: Set[str]) -> List[Dict]:
    print("--- ETAPA 3: Transformando PEDIDOS para LANÇAMENTO DE ESTOQUE ---")
    lancamentos_finais = []
    for pedido in pedidos:
        id_pedido_str = str(pedido.get("idPedido"))
        if id_pedido_str in existentes: continue

        novo_lancamento = {
            "doctype": "Stock Entry",
            "stock_entry_type": "Material Receipt",
            "posting_date": pedido.get("dataPedido"),
            "docstatus": 1,
            "custom_id_ongsys": id_pedido_str,
            "items": []
        }

        for item_pedido in pedido.get("itensPedido", []):
            try:
                quantidade = float(item_pedido.get("quantidade", 0))
                if quantidade <= 0: continue

                centro_custo = item_pedido.get("centroCusto")
                ensure_cost_center(centro_custo)

                item_formatado = {
                    "item_code": str(int(item_pedido.get("idProduto"))),
                    "qty": quantidade,
                    "rate": 1,
                    "t_warehouse": WAREHOUSE_ALVO,
                    "cost_center": centro_custo
                }
                novo_lancamento["items"].append(item_formatado)
            except (TypeError, ValueError, AttributeError) as e:
                print(f" -> AVISO: Item inválido no pedido '{id_pedido_str}' ignorado. Erro: {e}")
                continue
        
        if novo_lancamento["items"]:
            lancamentos_finais.append(novo_lancamento)
    
    print(f"-> {len(lancamentos_finais)} novos Lançamentos de Estoque prontos.")
    return lancamentos_finais

def carregar_lancamentos_erpnext(lancamentos: List[Dict]):
    if not lancamentos:
        print("--- ETAPA 4: Nenhum Lançamento para carregar. ---")
        return
        
    print(f"--- ETAPA 4: Carregando {len(lancamentos)} LANÇAMENTOS no ERPNext ---")
    for lancamento in lancamentos:
        id_original = lancamento.get("custom_id_ongsys")
        print(f"Enviando Lançamento (ID ONGsys: {id_original})...")
        response = erp_request("POST", "Stock Entry", lancamento)
        if response.status_code == 200:
            print(" -> SUCESSO!")
        else:
            print(f" -> FALHA DE API: {response.text}")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"\n====== INICIANDO EXTRATOR DE REQUISIÇÕES ======")
    pedidos_ongsys = extrair_requisicoes_ongsys()
    if pedidos_ongsys:
        ids_ja_importados = get_lancamentos_existentes_erpnext()
        lancamentos_para_erpnext = transformar_requisicoes_para_erpnext(pedidos_ongsys, ids_ja_importados)
        carregar_lancamentos_erpnext(lancamentos_para_erpnext)
    print("\n====== EXTRATOR DE REQUISIÇÕES FINALIZADO ======")