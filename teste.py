import requests
import pandas as pd
import time
import json

# =============================
# CONFIGURAÇÃO
# =============================
url_providers = "https://kitcorretoramil.com.br/wp-admin/admin-ajax.php?action=ktc_get_providers"
url_planos = "https://kitcorretoramil.com.br/wp-admin/admin-ajax.php?action=kc_get_planos_rede"

headers = { 
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://kitcorretoramil.com.br",
    "Referer": "https://kitcorretoramil.com.br/linha-amil/resumo-da-rede/" 
}

regioes = {
    # Para testes rápidos, descomente apenas uma linha abaixo:
    # "Sudeste": ["SP e Interior"], 
    "Norte": ["Acre", "Amapá", "Amazonas", "Pará", "Rondônia", "Roraima", "Tocantins"],
    "Nordeste": ["Alagoas", "Bahia", "Ceará", "Maranhão", "Paraíba", "Pernambuco", "Piauí", "Rio Grande do Norte", "Sergipe"],
    "Sul": ["Paraná", "Rio Grande do Sul", "Santa Catarina"],
    "Sudeste": ["Espírito Santo", "Minas Gerais", "Rio de Janeiro", "SP e Interior"],
    "Centro-Oeste": ["Distrito Federal", "Goiás", "Mato Grosso", "Mato Grosso do Sul"]
}

linhas = ["Linha Selecionada", "Linha Amil"]
tipos_rede = ["Hospitais", "Laboratórios"]

sess = requests.Session()
dados_consolidados = []

print("🚀 Iniciando extração dos dados...")

# =============================
# LOOP PRINCIPAL
# =============================

for linha in linhas:
    for rede_tipo in tipos_rede:
        for regiao, estados in regioes.items():
            for estado in estados:
                
                print(f"🔄 Buscando: {linha} | {rede_tipo} | {regiao} | {estado}")

                # --- PASSO A: Buscar Prestadores ---
                payload_provider = {
                    "pf": "false",
                    "estado": estado,
                    "Tipo de Rede": rede_tipo,
                    "linha": linha,
                    "regiao": regiao
                }
                
                try:
                    r_prov = sess.post(url_providers, json=payload_provider, headers=headers)
                    data_prov = r_prov.json()
                except Exception as e:
                    print(f"❌ Erro na requisição de prestadores ({estado}): {e}")
                    continue

                if not data_prov or not isinstance(data_prov, dict):
                    continue
                
                # O JSON vem agrupado por "slug" do produto (ex: amil_facil_sp)
                for produto_slug, lista_prestadores in data_prov.items():
                    
                    if not lista_prestadores:
                        continue
                    
                    # --- PASSO B: Buscar Nomes dos Planos para este Produto ---
                    payload_planos = {
                        "produto": produto_slug,
                        "regiao": estado,
                        "pf": "false",
                        "linhas_de_planos": linha
                    }
                    
                    lista_nomes_planos = []
                    try:
                        r_plan = sess.post(url_planos, json=payload_planos, headers=headers)
                        resp_planos = r_plan.json()
                        
                        # Normaliza a resposta dos planos (pode vir dict ou list)
                        if isinstance(resp_planos, dict):
                            lista_nomes_planos = list(resp_planos.values())
                        elif isinstance(resp_planos, list):
                            lista_nomes_planos = resp_planos
                    except Exception as e:
                        print(f"⚠️ Erro ao buscar planos para {produto_slug}: {e}")
                        continue
                    index = 0
                    # --- PASSO C: Cruzar Dados (Matriz) ---
                    for prestador in lista_prestadores:
                        # Estrutura observada: 
                        # Indice 0: Nome, 1: ?, 2: ?, 3: Cidade, 4+: Planos
                        
                        try:
                            nome_prestador = prestador[0]
                            # As vezes o indice 3 não é cidade, mas vamos assumir que sim pelo padrão
                            cidade_prestador = prestador[3] if len(prestador) > 3 else "N/A"

                            # Itera sobre as colunas de planos
                            for i, nome_plano in enumerate(lista_nomes_planos):
                                idx_modalidade = 4 + i # Offset de colunas fixas
                                index = index + 1
                                
                                if idx_modalidade < len(prestador):
                                    valor_celula = prestador[idx_modalidade]
                                    
                                    
                                    # 2. Ignora ícone de "X" (não atende)
                                    if isinstance(valor_celula, str) and 'fa-times' in valor_celula:
                                        continue
                                                            
                                    # --- Lógica de Validação ---
                                    # 0 ou Vazio = Não atende
                                    if isinstance(valor_celula, list):
                                        modalidade_final = ", ".join([str(v) for v in valor_celula])
                                    else:
                                        # Converte qualquer outra coisa para string (para evitar erro no SVG ou Números)
                                        modalidade_final = str(valor_celula)

                                    # Lógica do SVG (Laboratórios)
                                    if '<svg' in modalidade_final and 'true' in modalidade_final:
                                        modalidade_final = "Credenciado"
                                        
                                    if valor_celula == '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 13 16" width="13" height="16" class="true"><path d="m2 10.1l3.2 3.1 6.2-12.2" /></svg>':
                                        modalidade_final = "ACCEPT"
                                    if valor_celula == '<i class="fa fa-times"></i>' or valor_celula in ['0', 0, '', None, False]:
                                        modalidade_final = "DECLINE"
                                    
                                    # Se for texto (Hospital), ex: "PS, INT"
                                    # Já está na variável modalidade_final
                                    
                                    # Adiciona na lista final
                                    dados_consolidados.append({
                                        "Linha": linha,
                                        "Tipo Rede": rede_tipo,
                                        "Região Geográfica": regiao,
                                        "Estado": estado,
                                        "Cidade": cidade_prestador,
                                        "Produto (Slug)": produto_slug,
                                        "Plano": nome_plano[index]['attributes']['plano'],
                                        "Prestador": nome_prestador,
                                        "Modalidade/Status": modalidade_final
                                    })
                        except Exception as e:
                            # Ignora erro pontual em um prestador para não parar o script
                            pass
                
                # Pequeno delay para evitar bloqueio
                time.sleep(0.5)

# =============================
# EXPORTAÇÃO
# =============================
if dados_consolidados:
    df = pd.DataFrame(dados_consolidados)
    
    # Converte colunas que podem conter listas para string
    # Isso resolve o erro 'unhashable type: list'
    df = df.astype(str) 
    
    df = df.drop_duplicates()
    
    nome_arquivo = "Rede_Credenciada_Amil_Final.xlsx"
    df.to_excel(nome_arquivo, index=False)
    print(f"\n✅ Concluído! Arquivo gerado: {nome_arquivo} com {len(df)} linhas.")
else:
    print("\n❌ Nenhum dado encontrado.")