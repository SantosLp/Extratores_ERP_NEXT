from common import Common
from typing import Dict
import pandas as pd
api = Common()



COMPANY_NAME = "CDC"
PARENT_COST_CENTER = "CDC - CDC"
ARQUIVO_MAPEAMENTO = "mapeamento_centrocusto.csv"
centro_de_custo_armazen = pd.read_csv(ARQUIVO_MAPEAMENTO)


payload_cc = {
          "cost_center_name": cc_nome_erpnext,
          "parent_cost_center": PARENT_COST_CENTER,
          "company": COMPANY_NAME,
          "is_group": 0,
        }

#leia o ARQUIVO_MAPEAMENTO em 
##  Carregar o mapeamento de centro de custo