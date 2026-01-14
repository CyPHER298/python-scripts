from logging import debug
import os
import io
import re
import numpy as np
import pandas as pd
import paramiko
from urllib.parse import urlparse
from dotenv import load_dotenv

COL_PLANO = "Plano"
COL_REGIAO = "Regiao"
COL_VIDAS = "Vidas"
COL_PORTE = "Tipo Empresa"
COL_ACOMODACAO = "Acomodacao"
COL_FAIXA = "Faixa Etaria"
COL_PRECO = "Preço"
COL_COPART = "Coparticipação"

SHEET_NAME = "Sheet1"

def parse_money_brl(s: str):
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    s = s.replace("R$", "").strip()

    # 1.234,56 -> 1234.56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    # 700,00 -> 700.00
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    m = re.search(r"\d+(\.\d+)?", s)
    if not m:
        return None

    try:
        return float(m.group(0))
    except:
        return None

def faixa_preco(valor_estimado: float, margem: float = 100.0):
    if valor_estimado is None:
        return (None, None)
    return (valor_estimado - margem, valor_estimado + margem)


def parse_range_numbers(s: str):
    if not s:
        return (None, None)
    nums = list(map(int, re.findall(r"\d+", str(s))))
    if len(nums) == 0:
        return (None, None)
    if len(nums) == 1:
        return (nums[0], nums[0])
    return (min(nums[0], nums[1]), max(nums[0], nums[1]))

def normalize_faixa(s: str):
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    return str(s).replace(" ", "").strip()


def parse_vidas_df(s: str):
    if not s or (isinstance(s, float) and np.isnan(s)):
        return (None, None)
    nums = list(map(int, re.findall(r"\d+", str(s))))
    if len(nums) == 0:
        return (None, None)
    if len(nums) == 1:
        return (nums[0], nums[0])
    return (min(nums[0], nums[1]), max(nums[0], nums[1]))


def filtrar_regiao_series(series: pd.Series, regiao_payload: str) -> pd.Series:
    s = series.astype(str).str.upper().str.strip()
    rp = (regiao_payload or "").upper().strip()

    if not rp:
        return pd.Series([True] * len(series), index=series.index) and s.str.contains(rp, na=False)


# -----------------------
# SFTP: parse + connect + read excel
# -----------------------
def parse_sftp_url(sftp_url: str):
    """
    sftp://user@host:port/remote/path.xlsx
    -> (host, port, user, remote_path)
    """
    u = urlparse(sftp_url)
    if u.scheme.lower() != "sftp":
        raise ValueError("URL deve começar com sftp://")

    host = u.hostname
    port = u.port or 22
    user = u.username
    remote_path = u.path  # começa com "/"

    if not host or not user or not remote_path:
        raise ValueError("URL SFTP inválida (host/user/path).")

    return host, port, user, remote_path

def conectar_sftp(host: str, port: int, user: str, password: str):
    transport = paramiko.Transport((host, int(port)))
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

def ler_excel_sftp(sftp, remote_path: str, sheet_name: str = SHEET_NAME) -> pd.DataFrame:
    # valida existência
    sftp.stat(remote_path)

    with sftp.open(remote_path, "rb") as f:
        data = f.read()

    bio = io.BytesIO(data)
    df = pd.read_excel(bio, sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def carregar_planos_de_sftp(sftp_url: str, sheet_name: str = SHEET_NAME) -> pd.DataFrame:
    load_dotenv()

    # você pode deixar o password só no .env
    password = os.getenv("PASSWORD_ADMIN_SFTP")
    if not password:
        raise RuntimeError("PASSWORD_ADMIN_SFTP não encontrado no .env")

    host, port, user, remote_path = parse_sftp_url(sftp_url)

    sftp = transport = None
    try:
        sftp, transport = conectar_sftp(host, port, user, password)
        df = ler_excel_sftp(sftp, remote_path, sheet_name=sheet_name)
        return df
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except:
                pass

        if transport is not None:
            try:
                transport.close()
            except:
                pass


# -----------------------
# Montagem DF_PLANOS + buscar_planos
# -----------------------
def preparar_df_planos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # colunas auxiliares
    df["_FAIXA_N"] = df[COL_FAIXA].apply(normalize_faixa)
    df["_VALOR_NUM"] = df[COL_PRECO].apply(parse_money_brl)

    vidas_minmax = df[COL_VIDAS].apply(parse_vidas_df)
    df["_VIDAS_MIN"] = vidas_minmax.apply(lambda x: x[0])
    df["_VIDAS_MAX"] = vidas_minmax.apply(lambda x: x[1])

    return df


def buscar_planos(payload: dict, debug: bool = False):
    df_raw = carregar_planos_de_sftp(SFTP_URL, sheet_name=SHEET_NAME)
    DF_PLANOS = preparar_df_planos(df_raw)
    df = DF_PLANOS

    regiao = str(payload.get("regiao", "")).strip().upper()
    porte  = str(payload.get("porte_empresarial", "")).strip().upper()

    budget = parse_money_brl(payload.get("valor_estimado"))

    if budget is not None and budget > 0:
        if budget * 0.10 < 100.0:
            teto = budget + 100.0
        else:
            teto = budget * 1.10   # 10% de margem
        if debug:
            print(f"[DEBUG] Modo COM orçamento: {budget} | Teto: {teto}")
    else:
        budget = None
        teto = None
        if debug:
            print("[DEBUG] Modo CATÁLOGO (sem filtro de preço)")


    faixas_payload = payload.get("faixa_etaria") or []
    from collections import Counter

    faixas_payload = [normalize_faixa(x) for x in faixas_payload if x]
    faixa_contagem = Counter(faixas_payload)

    if not faixas_payload:
        if debug: print("[DEBUG] faixas_payload vazio")
        return []

    vmin_q, vmax_q = parse_range_numbers(payload.get("vidas"))

    def cnt(m, label):
        if debug:
            print(f"[DEBUG] {label}: {int(np.sum(m))} linhas")

    mask = np.ones(len(df), dtype=bool)
    cnt(mask, "TOTAL")

    if regiao:
        m_reg = filtrar_regiao_series(df[COL_REGIAO], regiao).to_numpy()
        mask &= m_reg
        cnt(mask, f"APÓS REGIÃO ({regiao})")

    if porte:
        m_por = df[COL_PORTE].astype(str).str.upper().str.contains(porte, na=False).to_numpy()
        mask &= m_por
        cnt(mask, f"APÓS PORTE ({porte})")

    if "_FAIXA_N" not in df.columns:
        raise KeyError("Coluna auxiliar _FAIXA_N não existe (prepare o DF antes).")

    m_faixa = df["_FAIXA_N"].isin(faixas_payload).to_numpy()
    mask &= m_faixa
    cnt(mask, f"APÓS FAIXAS ({faixas_payload})")

    if vmin_q is not None:
        vmin_col = pd.to_numeric(df["_VIDAS_MIN"], errors="coerce").to_numpy()
        vmax_col = pd.to_numeric(df["_VIDAS_MAX"], errors="coerce").to_numpy()
        m_vidas = (vmax_col >= vmin_q) & (vmin_col <= vmax_q)
        mask &= m_vidas
        cnt(mask, f"APÓS VIDAS ({vmin_q}-{vmax_q})")

    base = df.loc[mask].copy()
    if base.empty:
        if debug:
            print("[DEBUG] base vazio. Verifique regiao/porte/faixas/vidas.")
        return []

    base["_VALOR_NUM"] = pd.to_numeric(base["_VALOR_NUM"], errors="coerce")
    base = base.dropna(subset=["_VALOR_NUM"])
    if base.empty:
        if debug: print("[DEBUG] base vazio após dropna _VALOR_NUM.")
        return []

    chaves = [COL_PLANO, COL_REGIAO, COL_ACOMODACAO, COL_PORTE, COL_COPART]

    def calcular_soma(grupo):
        total = 0
        for faixa, qtd in faixa_contagem.items():
            linha = grupo[grupo["_FAIXA_N"] == faixa]
            if linha.empty:
                return None  # plano inválido, não cobre essa faixa
            preco_unit = linha["_VALOR_NUM"].iloc[0]
            total += preco_unit * qtd
        return total

    agg = (
        base.groupby(chaves, dropna=False)
            .apply(calcular_soma)
            .reset_index(name="soma_valor")
    )

    agg = agg.dropna(subset=["soma_valor"])

    
    if teto is not None:
        agg_budget = agg[agg["soma_valor"] <= teto]

        if agg_budget.empty:
            if debug:
                print("[DEBUG] Nenhum plano dentro do orçamento.")
                print(agg.sort_values("soma_valor").head(10))
            return []
    else:
        # sem orçamento → traz tudo
        agg_budget = agg.copy()


    if teto is not None:
        # mais perto do teto primeiro (melhor uso do orçamento)
        agg_budget["_rank"] = (teto - agg_budget["soma_valor"]).abs()
        agg_budget = agg_budget.sort_values(["_rank", "soma_valor"])
    else:
        # catálogo → mais barato primeiro
        agg_budget = agg_budget.sort_values("soma_valor")



    resultados = []
    for _, combo in agg_budget.iterrows():
        f = np.ones(len(base), dtype=bool)
        for k in chaves:
            f &= (base[k] == combo[k]).to_numpy()

        itens = []
        for faixa, qtd in faixa_contagem.items():
            linha = base.loc[f & (base["_FAIXA_N"] == faixa)].iloc[0]
            itens.append({
                "faixa_etaria": faixa,
                "vidas": qtd,
                "valor_unitario": linha[COL_PRECO],
                "valor_total": float(linha["_VALOR_NUM"]) * qtd
            })

        itens.sort(key=lambda x: x["valor_total"])

        resultados.append({
            "nome_plano": combo[COL_PLANO],
            "regiao": combo[COL_REGIAO],
            "acomodacao": combo[COL_ACOMODACAO],
            "porte_empresarial": combo[COL_PORTE],
            "copart": combo[COL_COPART],
            "budget_cliente": float(budget) if budget is not None else None,
            "teto_com_margem": float(teto) if teto is not None else None,
            "valor_somatoria": f"{float(combo['soma_valor']):.2f}",
            "detalhes_por_faixa": itens
        })

    return resultados

SFTP_URL = "sftp://AppAdmin@192.168.9.4:2022/Atendimentoaocorretor-GoTolky/configuracao/arquivos_base/Valores-amil.xlsx"

payload = {
    "regiao": "DIADEMA",
    "porte_empresarial": "MEI",
    "faixa_etaria": ["19-23", "0-18"],
    "vidas": "2",
    "valor_estimado": "2000.0"
}

print(buscar_planos(payload=payload, debug=True))