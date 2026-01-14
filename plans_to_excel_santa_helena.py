import requests
import pandas as pd

payload = {
    "linha_de_plano": "Linha Santa Helena PME",
    "numero_de_vidas_plano": "2 a 29",
    "regiao_plano": "Maua",
    "contratacao": "MEI",
    "verticalizadas": "1"
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://kitcorretoramil.com.br",
    "Referer": "https://kitcorretoramil.com.br/santahelena/tabela-de-precos/"
}

regioes = [
    "DIADEMA",
    "MAUA",
    "RIBEIRAO PIRES",
    "SANTO ANDRE",
    "SAO BERNARDO DO CAMPO",
    "SAO CAETANO DO SUL"
]

tipos_empresa = [
    "MEI",
    "Demais empresas",
    "Compulsório",
    "Livre Adesão"
]

faixas = [
    "00_18",
    "19_23",
    "24_28",
    "29_33",
    "34_38",
    "39_43",
    "44_48",
    "49_53",
    "54_58",
    "59_mais",
]

vidas = [
    "2 a 29",
    "30 a 99"
]
resposta = []
print("Iniciando requisições...")
for regiao in regioes:
    for vida in vidas:
        payload['numero_de_vidas_plano'] = vida    
        for tipo in tipos_empresa:
            payload['regiao_plano'] = regiao
            payload['contratacao'] = tipo

            
            url = f'https://app.kitcorretoramil.com.br/api/planos/getWithFilters?linha_de_plano=Linha%20Santa%20Helena%20PME&numero_de_vidas_plano={vida.replace(" ", "%20")}&regiao_plano={regiao.replace(" ", "%20")}&contratacao={tipo.replace(" ", "%20")}&verticalizadas=1'

            response = requests.get(url, headers=headers)
            response.raise_for_status()
            dados = response.json()
            
            if not isinstance(dados, dict) or 'plans' not in dados:
                raise Exception(f"❌ Resposta inesperada para Região: {regiao}, Vidas: {vida}, Tipo: {tipo}: {dados}")
            
            for plano in dados['plans']:
                for faixa in faixas:
                    key = f'precos_{faixa}'
                    resposta.append({
                        'Regiao': regiao,
                        'Plano': plano.get('plano'),
                        'Vidas': vida,
                        'Tipo Empresa': tipo,
                        'Acomodacao': plano.get('acomodacao'),
                        'Faixa Etaria': faixa.replace("_", "-") if faixa != "59_mais" else "59+",
                        'Preço': float(plano.get(key)),
                        'Coparticipação': plano.get('coparticipacao'),
                        'Contratação': plano.get('contratacao')
                    })
print(resposta)

if not isinstance(dados, dict):
    raise Exception(f"❌ Resposta inesperada: {dados}")

df = pd.DataFrame(resposta)
print("Exportando para Excel...")
input("Pressione Enter para continuar...")

df.to_excel("amil_santa_helena_pme.xlsx", index=False)
print("Exportação concluída: amil_santa_helena_pme.xlsx")
                
            