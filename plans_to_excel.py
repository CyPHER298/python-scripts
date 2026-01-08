import requests 
import pandas as pd 

# ============================= #
#  CONFIGURAÇÃO 
# ============================= 

url = "https://kitcorretoramil.com.br/wp-admin/admin-ajax.php?action=ktc_get_price_table_values" 

payload = { 
    "pf": "false",
    "Estado": "INTERIOR SP - 1",
    "Numero_de_vidas_plano": "5 a 29",
    "Compulsorio": "MEI",
    "Linha": "Linha Amil",
    "Coparticipação": "Com coparticipação30" 
} 

headers = { 
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://kitcorretoramil.com.br",
    "Referer": "https://kitcorretoramil.com.br/linha-selecionada-pme/tabela-de-precos-pme/" 
} 

# ============================= 
# REQUEST 
# ============================= 

regiões = [
    "INTERIOR SP - 1",
    "INTERIOR SP - 2",
    "BAHIA",
    "CEARÁ",
    "DISTRITO FEDERAL",
    "GOIÁS",
    "MARANHÃO",
    "MINAS GERAIS",
    "PARAÍBA",
    "PARANÁ",
    "PERNAMBUCO",
    "RIO DE JANEIRO",
    "RIO GRANDE DO SUL",
    "RIO GRANDE DO NORTE",
    "SANTA CATARINA",
    "SÃO PAULO",
]

tipo_empresa = [
    "MEI",
    "Demais Empresas",
    "Livre Adesão",
    "Compulsório"
]

linhas = [
    "Linha Selecionada",
    "Linha Amil"
]

resposta = []

for linha in linhas:
    payload["Linha"] = linha
    for regiao in regiões:
        for empresa in tipo_empresa:
            payload["Compulsorio"] = empresa
            payload["Estado"] = regiao
            response = requests.post( url, json=payload, # 🔥 ISSO É O PONTO-CHAVE
                                headers=headers 
                            ) 

            data = response.json()
            print(response.status_code, regiao, empresa, linha, len(data))
            resposta.append({f"{regiao}_{empresa}": data})

with open("amil_pme_interior_sp_response.json", "w", encoding="utf-8") as f:
    import json
    json.dump(resposta, f, ensure_ascii=False, indent=4)

print(resposta)  # Exemplo de saída para a primeira região

# ============================= 
# VALIDAÇÃO 
# ============================= 

if not isinstance(data, dict): 
    raise Exception(f"❌ Resposta inesperada: {data}") 

# ============================= 
# NORMALIZAÇÃO 
# ============================= 

faixas = [
    "0-18",
    "19-23",
    "24-28",
    "29-33",
    "34-38",
    "39-43",
    "44-48",
    "49-53",
    "54-58",
    "59+"
]

linhas = []

for bloco in resposta:
    for chave_bloco, planos in bloco.items():

        regiao, tipo_empresa = chave_bloco.rsplit("_", 1)

        for _, valores in planos.items():

            if not isinstance(valores, list) or len(valores) < 12:
                continue

            plano = valores[0]
            acomodacao = valores[1]
            precos = valores[2:12]
            vidas = valores[-1]  # ex: "5 a 29" ou "30 a 99"

            # =============================
            # REGRA DE NEGÓCIO 🔥
            # =============================
            if vidas == "30 a 99":
                if tipo_empresa not in ["Compulsório", "Livre Adesão"]:
                    continue
            else:
                if tipo_empresa not in ["MEI", "Demais Empresas"]:
                    continue
            # =============================

            for faixa, preco in zip(faixas, precos):
                linhas.append({
                    "Plano": plano,
                    "Acomodação": acomodacao,
                    "Região": regiao,
                    "Faixa Etaria": faixa,
                    "Tipo_Empresa": tipo_empresa,
                    "Preço": float(preco.replace(".", "").replace(",", ".")),
                    "Vidas": vidas,
                    "Coparticipação": payload["Coparticipação"]
                })


        
df = pd.DataFrame(linhas) 
print(df.head()) 
       
# ============================= 
# EXPORTAÇÃO 
# ============================= 

df.to_excel("amil_pme_interior_sp.xlsx", index=False) 
print("\n✅ Excel gerado com sucesso: amil_pme_interior_sp.xlsx")