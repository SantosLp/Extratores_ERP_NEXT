import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv

# --- CONFIGURAÇÕES ---
load_dotenv()

# Credenciais do ERPNext (Destino)
ERPNext_URL = os.getenv("ERPNext_URL")
ERPNext_API_KEY = os.getenv("ERPNext_API_KEY")
ERPNext_API_SECRET = os.getenv("ERPNext_API_SECRET")

# Credenciais do ONGSYS (Origem)
ONGSYS_URL_BASE = os.getenv("ONGSYS_URL_BASE") + "/fornecedores"
ONGSYS_USER = os.getenv("ONGSYS_USERNAME")
ONGSYS_PASS = os.getenv("ONGSYS_PASSWORD")

# print(ONGSYS_URL_BASE)
# print(ONGSYS_USER)
# print(ONGSYS_PASS)

# Validação das credenciais do ERPNext
if not all([ERPNext_URL, ERPNext_API_KEY, ERPNext_API_SECRET]):
    print("!!! ERRO CRÍTICO: Verifique as variáveis do ERPNext no arquivo .env !!!")
    exit()

HEADERS_ERPNext = {"Authorization": f"token {ERPNext_API_KEY}:{ERPNext_API_SECRET}"}

# --- FUNÇÕES DO EXTRATOR DE FORNECEDORES ---
def extrair_fornecedores_da_api_real():
    """Extrai os dados de fornecedores da API REAL do ONGsys, lidando com paginação."""
    print("--- ETAPA 1: Extraindo dados de Fornecedores do ONGSYS (VIDA REAL) ---")
    
    pagina_atual = 1
    todos_fornecedores = []

    while True:
        print(f"Buscando dados da página: {pagina_atual}...")
        try:
            # Monta os parâmetros para a requisição da página atual
            params = {'pageNumber': pagina_atual}
            
            # Faz a requisição para a API
            response = requests.get(
                ONGSYS_URL_BASE, 
                auth=HTTPBasicAuth(ONGSYS_USER, ONGSYS_PASS), 
                params=params, 
                timeout=30
            )
            response.raise_for_status()
            
            dados = response.json()

            # --- CONDIÇÃO DE PARADA ---
            # Verifica se a mensagem de erro específica foi recebida
            if 'errors' in dados and isinstance(dados.get('errors'), list) and dados['errors']:
                if dados['errors'][0].get('message') == "Não existe registros de fornecedores com estes parâmetros informados":
                    print("\n-> Fim dos dados. Mensagem de parada recebida da API.")
                    break # Encerra o loop

            # Extrai os fornecedores da página atual
            fornecedores_da_pagina = dados.get('data', [])
            
            # Segunda condição de parada: se a lista de dados vier vazia
            if not fornecedores_da_pagina:
                print("-> Página sem registros. Fim da extração.")
                break
            
            todos_fornecedores.extend(fornecedores_da_pagina)
            print(f" -> {len(fornecedores_da_pagina)} registros encontrados nesta página.")
            
            # Prepara para a próxima página
            pagina_atual += 1

        except Exception as e:
            print(f"!!! FALHA na extração na página {pagina_atual}: {e}")
            break 

    print(f"\n-> Extração do ONGSYS concluída. Total de {len(todos_fornecedores)} fornecedores encontrados.")
    return todos_fornecedores


def get_fornecedores_existentes_do_erpnext():
    """Busca no ERPNext a lista de fornecedores que já foram cadastrados."""
    print("--- ETAPA 2: Verificando fornecedores que já existem no ERPNext ---")
    try:
        url = f"{ERPNext_URL}/api/resource/Supplier?fields=[\"supplier_name\"]&limit=9999"
        response = requests.get(url, headers=HEADERS_ERPNext, timeout=30)
        response.raise_for_status()
        dados = response.json()
        nomes_existentes = {f['supplier_name'] for f in dados.get('data', [])}
        print(f"-> {len(nomes_existentes)} fornecedores encontrados no ERPNext.")
        return nomes_existentes
    except Exception as e:
        print(f"!!! FALHA ao buscar fornecedores do ERPNext: {e}")
        return set()

def transformar_e_filtrar_fornecedores(fornecedores_origem, nomes_existentes):
    """Compara as duas listas e formata apenas os fornecedores novos."""
    print("--- ETAPA 3: Comparando e transformando apenas os dados novos ---")
    novos_fornecedores = []
    for fornecedor in fornecedores_origem:
        nome = fornecedor.get('nomeEmpresa')
        if nome and nome not in nomes_existentes:
            fornecedor_formatado = {
                "supplier_name": nome,
                "supplier_group": "Local",
                "tax_id": fornecedor.get('documento')
            }
            novos_fornecedores.append(fornecedor_formatado)
    print(f"-> {len(novos_fornecedores)} novos fornecedores para criar.")
    return novos_fornecedores

def carregar_novos_fornecedores(lista_para_criar):
    """Envia a lista de fornecedores novos para a API do ERPNext."""
    if not lista_para_criar:
        print("--- ETAPA 4: Nenhum fornecedor novo para carregar. ---")
        return

    print(f"--- ETAPA 4: Carregando {len(lista_para_criar)} novos FORNECEDORES no ERPNext ---")
    url = f"{ERPNext_URL}/api/resource/Supplier"
    
    for fornecedor in lista_para_criar:
        print(f"Enviando fornecedor '{fornecedor.get('supplier_name')}'...")
        try:
            response = requests.post(url, headers=HEADERS_ERPNext, json=fornecedor, timeout=30)
            response.raise_for_status()
            print(f" -> SUCESSO!")
        except Exception as e:
            print(f" -> FALHA: {e}")
            if hasattr(e, 'response'): print(f" -> Resposta do Servidor: {e.response.text}")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"\n====== INICIANDO EXTRATOR DE FORNECEDORES (VIDA REAL) ======")
    
    fornecedores_ongsys = extrair_fornecedores_da_api_real()
    
    if fornecedores_ongsys:
        fornecedores_erpnext = get_fornecedores_existentes_do_erpnext()
        fornecedores_novos = transformar_e_filtrar_fornecedores(fornecedores_ongsys, fornecedores_erpnext)
        carregar_novos_fornecedores(fornecedores_novos)
        
    print("\n====== EXTRATOR DE FORNECEDORES FINALIZADO ======")