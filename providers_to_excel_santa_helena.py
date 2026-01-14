import time
import requests
import pandas as pd

# =============================
# CONFIGURAÇÃO
# =============================
BASE_URL = "https://app.kitcorretoramil.com.br/api/redes-credenciadas/getByPlan/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://kitcorretoramil.com.br",
    "Referer": "https://kitcorretoramil.com.br/santahelena/resumo-de-rede",
}

# Mantive no padrão do seu base (lista)
LINHAS = ["Linha Santa Helena PME"]

TIPOS_REDE = {
    "Hospitais": "hospitais SH PME",
    "Laboratórios": "laboratorios SH PME",
    "Centros Médicos": "centro medico SH PME",  # se não retornar, tente "centros medicos SH PME" ou "centros_medicos SH PME"
}

REGIOES_ID = {
    "SANTO ANDRE": 10214,
    "DIADEMA": 10211,
    "MAUA": 10212,
    "RIBEIRAO PIRES": 10213,
    "SAO CAETANO DO SUL": 10216,
    "SAO BERNARDO DO CAMPO": 10215,
}

# ⚠️ "produto" no endpoint é o SLUG (ex.: diamante), não o nome do plano (DIAMANTE III)
PRODUTOS = ["diamante"]  # se tiver outros, adicione aqui: ["diamante", "ouro", "prata", ...]

TIMEOUT = 60
DELAY = 0.25


# =============================
# FUNÇÕES AUXILIARES
# =============================
def is_credenciado(rel):
    """
    No response:
      credenciado: {"id": 927170, "atributos": "LAB", "planos_rede_credenciada": {...}}
      não credenciado: {"id": null, "atributos": false}
    """
    if not isinstance(rel, dict):
        return False

    if rel.get("id") is None:
        return False

    atr = rel.get("atributos")
    if atr is False or atr in [None, "", "0"]:
        return False

    plano_obj = rel.get("planos_rede_credenciada")
    return isinstance(plano_obj, dict) and bool(plano_obj.get("plano"))


def fetch_rede(sess, regiao_id, linha, produto, tipo):
    params = {
        "regiao": str(regiao_id),
        "linha": linha,
        "produto": produto,
        "tipo": tipo,
    }
    r = sess.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


# =============================
# EXTRAÇÃO PRINCIPAL
# =============================
sess = requests.Session()
dados_consolidados = []

print("🚀 Iniciando extração Santa Helena (getByPlan)...")

for linha in LINHAS:
    for tipo_rede_label, tipo_param in TIPOS_REDE.items():
        for regiao_label, regiao_id in REGIOES_ID.items():
            for produto in PRODUTOS:
                print(f"🔄 Buscando: {linha} | {tipo_rede_label} | {regiao_label} | produto={produto}")

                try:
                    payload = fetch_rede(sess, regiao_id, linha, produto, tipo_param)
                except Exception as e:
                    print(f"❌ Erro na requisição ({regiao_label}/{tipo_rede_label}/{produto}): {e}")
                    continue

                data = payload.get("data", [])
                if not isinstance(data, list) or not data:
                    time.sleep(DELAY)
                    continue

                for item in data:
                    attrs = item.get("attributes", {}) if isinstance(item, dict) else {}
                    if not isinstance(attrs, dict):
                        continue

                    prestador = (attrs.get("nome") or "").strip()
                    cidade = (attrs.get("cidade") or "").strip()

                    # No response: "regiao": "METROPOLITANA DE SÃO PAULO"
                    estado_macro = (attrs.get("regiao") or "").strip()

                    relacoes = attrs.get("relacoes", [])
                    if not isinstance(relacoes, list) or not relacoes:
                        continue

                    for rel in relacoes:
                        if not is_credenciado(rel):
                            continue

                        modalidade = rel.get("atributos")
                        plano_obj = rel.get("planos_rede_credenciada", {}) or {}
                        plano = (plano_obj.get("plano") or "").strip()

                        # Monta EXATAMENTE as colunas que você pediu
                        dados_consolidados.append({
                            "Linha": linha,
                            "Tipo Rede": tipo_rede_label,
                            "Região": regiao_label,
                            "Estado": estado_macro,
                            "Cidade": cidade,
                            "Produto": produto,
                            "Plano": plano,
                            "Prestador": prestador,
                            "Modalidade": modalidade,
                        })

                time.sleep(DELAY)

# =============================
# EXPORTAÇÃO
# =============================
if dados_consolidados:
    df = pd.DataFrame(dados_consolidados)

    df = df[[
        "Linha",
        "Tipo Rede",
        "Região",
        "Estado",
        "Cidade",
        "Produto",
        "Plano",
        "Prestador",
        "Modalidade",
    ]]

    df = df.astype(str).drop_duplicates()

    nome_arquivo = "Rede_Credenciada_SantaHelena.xlsx"
    df.to_excel(nome_arquivo, index=False)

    print(f"\n✅ Concluído! Arquivo gerado: {nome_arquivo} com {len(df)} linhas.")
else:
    print("\n❌ Nenhum dado encontrado. Verifique TIPOS_REDE (texto do tipo) e PRODUTOS (slug).")
