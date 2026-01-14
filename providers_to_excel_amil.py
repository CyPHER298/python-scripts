import time
import requests
import pandas as pd

# =============================
# CONFIGURAÇÃO
# =============================
URL_PROVIDERS = "https://kitcorretoramil.com.br/wp-admin/admin-ajax.php?action=ktc_get_providers"
URL_PLANOS = "https://kitcorretoramil.com.br/wp-admin/admin-ajax.php?action=kc_get_planos_rede"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://kitcorretoramil.com.br",
    "Referer": "https://kitcorretoramil.com.br/santahelena/resumo-de-rede",
}

REGIOES = {
    "Norte": ["Acre", "Amapá", "Amazonas", "Pará", "Rondônia", "Roraima", "Tocantins"],
    "Nordeste": ["Alagoas", "Bahia", "Ceará", "Maranhão", "Paraíba", "Pernambuco", "Piauí", "Rio Grande do Norte", "Sergipe"],
    "Sul": ["Paraná", "Rio Grande do Sul", "Santa Catarina"],
    "Sudeste": ["Espírito Santo", "Minas Gerais", "Rio de Janeiro", "SP e Interior"],
    "Centro-Oeste": ["Distrito Federal", "Goiás", "Mato Grosso", "Mato Grosso do Sul"],
}

LINHAS = ["Linha Selecionada", "Linha Amil"]
TIPOS_REDE = ["Hospitais", "Laboratórios"]

# =============================
# FUNÇÕES AUXILIARES
# =============================

def normaliza_celula(valor):

    if isinstance(valor, list):
        txt = ", ".join(map(str, valor))
    else:
        txt = str(valor)

    txt = txt.strip()

    # Laboratórios: costuma vir SVG com class="true"
    if "<svg" in txt and 'class="true"' in txt:
        return "Credenciado"

    # Casos específicos (se quiser manter)
    if txt == '<i class="fa fa-times"></i>' or txt in ['0', 'None', '', None, False]:
        return "Não Credenciado"

    # Hospitais: vem texto tipo "H - PS - INT..."
    return txt


def get_planos(sess, produto_slug, estado, linha):
    """
    Busca a lista de planos (por produto/estado/linha).
    Resposta típica:
      {"data":[{"id":..,"attributes":{"plano":"Prata", "order_resumo_rede":7}}, ...]}
    """
    payload_planos = {
        "produto": produto_slug,
        "regiao": estado,
        "pf": "false",
        "linhas_de_planos": linha,
    }

    r = sess.post(URL_PLANOS, json=payload_planos, headers=HEADERS, timeout=60)
    r.raise_for_status()
    j = r.json()

    planos = j.get("data", [])
    # Ordena para casar com a ordem das colunas retornadas no provider
    planos = sorted(planos, key=lambda x: x.get("attributes", {}).get("order_resumo_rede", 9999))
    return planos


# =============================
# EXTRAÇÃO PRINCIPAL
# =============================

sess = requests.Session()

dados_consolidados = []

# Cache pra não buscar planos repetidos para o mesmo (produto, estado, linha)
planos_cache = {}  # (produto_slug, estado, linha) -> lista planos

print("🚀 Iniciando extração dos dados...")

for linha in LINHAS:
    for rede_tipo in TIPOS_REDE:
        for regiao, estados in REGIOES.items():
            for estado in estados:
                print(f"🔄 Buscando: {linha} | {rede_tipo} | {regiao} | {estado}")

                # --- PASSO A: Buscar Prestadores ---
                payload_provider = {
                    "pf": "false",
                    "estado": estado,
                    "Tipo de Rede": rede_tipo,
                    "linha": linha,
                    "regiao": regiao,
                }

                try:
                    r_prov = sess.post(URL_PROVIDERS, json=payload_provider, headers=HEADERS, timeout=60)
                    r_prov.raise_for_status()
                    data_prov = r_prov.json()
                except Exception as e:
                    print(f"❌ Erro na requisição de prestadores ({estado}): {e}")
                    continue

                if not isinstance(data_prov, dict) or not data_prov:
                    continue

                # O JSON vem agrupado por produto (slug)
                for produto_slug, lista_prestadores in data_prov.items():
                    if not lista_prestadores:
                        continue

                    # --- PASSO B: Buscar Planos para este Produto/Estado/Linha (com cache) ---
                    cache_key = (produto_slug, estado, linha)
                    if cache_key not in planos_cache:
                        try:
                            planos_cache[cache_key] = get_planos(sess, produto_slug, estado, linha)
                        except Exception as e:
                            print(f"⚠️ Erro ao buscar planos para {produto_slug} ({estado}/{linha}): {e}")
                            planos_cache[cache_key] = []

                    planos = planos_cache[cache_key]
                    if not planos:
                        continue

                    # --- PASSO C: Cruzar Dados (Matriz) ---
                    for prestador in lista_prestadores:
                        try:
                            nome_prestador = prestador[0]
                            cidade_prestador = prestador[3] if len(prestador) > 3 else "N/A"

                            # Colunas fixas: 0..3
                            # Colunas por plano começam em 4
                            for i, plano_obj in enumerate(planos):
                                plano_nome = plano_obj.get("attributes", {}).get("plano", "")
                                idx_coluna = 4 + i

                                if idx_coluna >= len(prestador):
                                    continue

                                valor_celula = prestador[idx_coluna]
                                status = normaliza_celula(valor_celula)
                                if not status:
                                    continue

                                dados_consolidados.append({
                                    "Linha": linha,
                                    "Tipo Rede": rede_tipo,
                                    "Região": regiao,
                                    "Estado": estado,
                                    "Cidade": cidade_prestador,
                                    "Produto": produto_slug,
                                    "Plano": plano_nome,
                                    "Prestador": nome_prestador,
                                    "Modalidade": status
                                })
                        except Exception:
                            # ignora erro pontual por prestador
                            pass

                # pequeno delay pra evitar bloqueio
                time.sleep(0.4)

# =============================
# EXPORTAÇÃO
# =============================

if dados_consolidados:
    df = pd.DataFrame(dados_consolidados)

    # Força tudo como string para evitar problemas com listas/objetos
    df = df.astype(str).drop_duplicates()

    nome_arquivo = "Rede_Credenciada_Amil_Final.xlsx"
    df.to_excel(nome_arquivo, index=False)

    print(f"\n✅ Concluído! Arquivo gerado: {nome_arquivo} com {len(df)} linhas.")
else:
    print("\n❌ Nenhum dado encontrado.")
