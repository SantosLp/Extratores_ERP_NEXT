import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
from typing import List, Dict, Set

# --- CONFIGURAÇÕES ---
load_dotenv()
ERPNext_URL = os.getenv("ERPNext_URL")
API_KEY = os.getenv("ERPNext_API_KEY")
API_SECRET = os.getenv("ERPNext_API_SECRET")

ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE")
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

if not all([ERPNext_URL, API_KEY, API_SECRET, ONGSYS_URL_BASE, ONGSYS_USER, ONGSYS_PASS]):
    print("!!! ERRO CRÍTICO: Verifique TODAS as variáveis de ambiente no arquivo .env !!!")
    exit()

HEADERS_ERPNext = {"Authorization": f"token {API_KEY}:{API_SECRET}"}
HEADERS_ONGSYS = {'User-Agent': 'Mozilla/5.0'}

# --- FUNÇÕES DO EXTRATOR ---

def extrair_requisicoes_ongsys() -> List[Dict]:
    print("--- ETAPA 1: Extraindo PEDIDOS da API do ONGSYS ---")
    endpoint = f"{ONGSYS_URL_BASE.rstrip('/')}/pedidos"
    pagina_atual = 1
    todos_pedidos = []
    
    while True:
        print(f"Buscando dados da página: {pagina_atual}...")
        try:
            params = {'pageNumber': pagina_atual}
            response = requests.get(endpoint, auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), params=params, headers=HEADERS_ONGSYS, timeout=30)
            if response.status_code == 422:
                print("-> Fim dos dados (422 recebido)."); break
            response.raise_for_status()
            dados = response.json()
            pedidos_da_pagina = dados.get('data', [])
            if not pedidos_da_pagina:
                print("-> Fim dos dados. Página vazia recebida."); break
            todos_pedidos.extend(pedidos_da_pagina)
            print(f" -> {len(pedidos_da_pagina)} registros encontrados nesta página.")
            pagina_atual += 1
        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}"); break
            
    print(f"\n-> Extração do ONGSYS concluída. Total de {len(todos_pedidos)} pedidos encontrados.")
    return todos_pedidos

def get_requisicoes_existentes_erpnext() -> Set[str]:
    print("--- ETAPA 2: Verificando requisições já existentes no ERPNext ---")
    ids_existentes = set()
    try:
        url = f"{ERPNext_URL}/api/resource/Purchase Invoice?fields=[\"custom_id_ongsys\"]&limit=9999"
        response = requests.get(url, headers=HEADERS_ERPNext, timeout=30)
        response.raise_for_status()
        dados = response.json()
        ids_existentes = {str(req['custom_id_ongsys']) for req in dados.get('data', []) if req.get('custom_id_ongsys')}
        print(f"-> {len(ids_existentes)} requisições do ONGSYS encontradas no ERPNext.")
    except Exception as e:
        print(f"!!! AVISO: Não foi possível buscar requisições existentes. Pode haver duplicatas. Erro: {e}")
    return ids_existentes

def transformar_requisicoes_para_erpnext(pedidos_origem: List[Dict], ids_existentes: Set[str]) -> List[Dict]:
    print("--- ETAPA 3: Transformando PEDIDOS para o formato de FATURA DE COMPRA ---")
    faturas_finais = []
    for pedido in pedidos_origem:
        id_pedido_str = str(pedido.get("idPedido"))

        if id_pedido_str in ids_existentes:
            continue

        fornecedor_info = pedido.get('fornecedor', {})
        
        # <<<--- CORREÇÃO APLICADA AQUI: Pegar a conta de despesa ---<<<
        conta_financeira_completa = pedido.get("contaPlanoFinanceiro", "")
        # Pega a parte depois do ' - ', se existir. Se não, usa um valor padrão.
        conta_de_despesa = conta_financeira_completa.split(' - ')[-1] if ' - ' in conta_financeira_completa else "Despesas Diversas"

        nova_fatura = {
            "doctype": "Purchase Invoice",
            "supplier": fornecedor_info.get("nome"),
            "posting_date": pedido.get("dataPedido"),
            "due_date": pedido.get("dataPedido"),
            "docstatus": 1,
            "custom_id_ongsys": id_pedido_str,
            "items": []
        }

        for item_pedido in pedido.get("itensPedido", []):
            nome_servico = item_pedido.get("nomeServico")
            if not nome_servico:
                continue

            item_formatado = {
                "item_name": nome_servico,
                "description": item_pedido.get("descricao") or nome_servico,
                "qty": item_pedido.get("quantidade", 1),
                "rate": item_pedido.get("valorUnitario", 1), # Usando o valor unitário se existir, senão 1
                "expense_account": conta_de_despesa # Adiciona a conta de despesa em cada item
            }
            nova_fatura["items"].append(item_formatado)

        if nova_fatura["items"]:
            faturas_finais.append(nova_fatura)
    
    print(f"-> {len(faturas_finais)} novas faturas prontas para serem enviadas.")
    return faturas_finais

def carregar_requisicoes_erpnext(lista_de_faturas: List[Dict]):
    if not lista_de_faturas:
        print("--- ETAPA 4: Nenhuma fatura nova para carregar. ---")
        return
        
    print(f"--- ETAPA 4: Carregando {len(lista_de_faturas)} FATURAS DE COMPRA no ERPNext ---")
    url = f"{ERPNext_URL}/api/resource/Purchase Invoice"

    for fatura in lista_de_faturas:
        id_original = fatura.get("custom_id_ongsys")
        print(f"Enviando Fatura (ID ONGsys: {id_original})...")
        try:
            response = requests.post(url, headers=HEADERS_ERPNext, json=fatura, timeout=60)
            response.raise_for_status()
            print(f" -> SUCESSO!")
        except Exception as e:
            print(f" -> FALHA: {e}")
            if hasattr(e, 'response'): print(f" -> Resposta do Servidor: {e.response.text}")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"\n====== INICIANDO EXTRATOR DE REQUISIÇÕES ======")
    pedidos_ongsys = extrair_requisicoes_ongsys()
    if pedidos_ongsys:
        ids_ja_importados = get_requisicoes_existentes_erpnext()
        faturas_para_erpnext = transformar_requisicoes_para_erpnext(pedidos_ongsys, ids_ja_importados)
        carregar_requisicoes_erpnext(faturas_para_erpnext)
    print("\n====== EXTRATOR DE REQUISIÇÕES FINALIZADO ======")