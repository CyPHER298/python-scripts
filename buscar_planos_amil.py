import os
import io
import re
import numpy as np
import pandas as pd
import paramiko
from urllib.parse import urlparse
from dotenv import load_dotenv

COL_PLANO = "Plano"
COL_ACOMODACAO = "Acomodação"
COL_FAIXA_ETARIA = "Faixa Etaria"
COL_REGIAO = "Região"
COL_PORTE = "Tipo_Empresa"
COL_VALOR = "Preço"
COL_VIDAS = "Vidas"
COL_COPART = "Coparticipação"

SHEET_NAME = "Sheet1"

# -----------------------
# Helpers de parsing
# -----------------------
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
        return pd.Series([True] * len(series), index=series.index)

    if rp == "SP":
        return (s == "SP")
    else:
        return s.str.contains(rp, na=False)


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
        # fecha corretamente
        try:
            if sftp:
                sftp.close()
        finally:
            if transport:
                transport.close()


# -----------------------
# Montagem DF_PLANOS + buscar_planos
# -----------------------
def preparar_df_planos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # colunas auxiliares
    df["_FAIXA_N"] = df[COL_FAIXA_ETARIA].apply(normalize_faixa)
    df["_VALOR_NUM"] = df[COL_VALOR].apply(parse_money_brl)

    vidas_minmax = df[COL_VIDAS].apply(parse_vidas_df)
    df["_VIDAS_MIN"] = vidas_minmax.apply(lambda x: x[0])
    df["_VIDAS_MAX"] = vidas_minmax.apply(lambda x: x[1])

    return df


def buscar_planos(payload: dict, debug: bool = False):
    margem_budget = 100.0
    df_raw = carregar_planos_de_sftp(SFTP_URL, sheet_name=SHEET_NAME)
    DF_PLANOS = preparar_df_planos(df_raw)
    df = DF_PLANOS

    regiao = str(payload.get("regiao", "")).strip().upper()
    porte  = str(payload.get("porte_empresarial", "")).strip().upper()

    budget = parse_money_brl(payload.get("valor_estimado"))
    if budget is None:
        if debug: print("[DEBUG] budget inválido:", payload.get("valor_estimado"))
        return []

    teto = budget + float(margem_budget)

    faixas_payload = payload.get("faixa_etaria") or []
    faixas_payload = [normalize_faixa(x) for x in faixas_payload if x]
    faixas_payload = list(dict.fromkeys(faixas_payload))
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
    qtd_faixas = len(set(faixas_payload))

    agg = (
        base.groupby(chaves, dropna=False)
            .agg(
                faixas_distintas=("_FAIXA_N", "nunique"),
                soma_valor=("_VALOR_NUM", "sum")
            )
            .reset_index()
    )

    agg_all = agg[agg["faixas_distintas"] == qtd_faixas]
    agg_budget = agg_all[agg_all["soma_valor"] <= teto]

    if agg_budget.empty:
        if debug:
            top = agg_all.sort_values("soma_valor", ascending=True).head(10)
            print("[DEBUG] Nenhum combo coube no teto. TOP 10:")
            print(top[[COL_PLANO, COL_REGIAO, COL_ACOMODACAO, COL_PORTE, COL_COPART, "soma_valor"]])
        return []

    agg_budget["_rank_budget"] = (teto - agg_budget["soma_valor"]).abs()
    agg_budget = agg_budget.sort_values(["_rank_budget", "soma_valor"], ascending=[True, False])

    resultados = []
    for _, combo in agg_budget.iterrows():
        f = np.ones(len(base), dtype=bool)
        for k in chaves:
            f &= (base[k] == combo[k]).to_numpy()

        itens = base.loc[f, [COL_FAIXA_ETARIA, COL_VALOR, "_VALOR_NUM"]].copy()
        itens = itens.sort_values("_VALOR_NUM", ascending=True)

        resultados.append({
            "nome_plano": combo[COL_PLANO],
            "regiao": combo[COL_REGIAO],
            "acomodacao": combo[COL_ACOMODACAO],
            "porte_empresarial": combo[COL_PORTE],
            "copart": combo[COL_COPART],
            "budget_cliente": float(budget),
            "teto_com_margem": float(teto),
            "valor_somatoria": f"{float(combo["soma_valor"]):.2f}",
            "detalhes_por_faixa": [
                {
                    "faixa_etaria": r[COL_FAIXA_ETARIA],
                    "valor": r[COL_VALOR],
                }
                for _, r in itens.iterrows()
            ]
        })

    return resultados


# -----------------------
# USO
# -----------------------
SFTP_URL = "sftp://AppAdmin@192.168.9.4:2022/Atendimentoaocorretor-GoTolky/configuracao/arquivos_base/Valores-amil.xlsx"

payload = {
    "regiao": "BAHIA",
    "porte_empresarial": "Demais Empresas",
    "faixa_etaria": ["29-33", "59+"],
    "vidas": "7",
    "valor_estimado": "7000,00"
}

print(buscar_planos(payload=payload, debug=True))
