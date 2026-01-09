import os
import io
import re
import numpy as np
import pandas as pd
import paramiko
import unicodedata
from urllib.parse import urlparse
from dotenv import load_dotenv

# ===============================
# CONFIG FIXA (infraestrutura)
# ===============================
SFTP_URL = "sftp://AppAdmin@192.168.9.4:2022/Atendimentoaocorretor-GoTolky/configuracao/arquivos_base/Valores-amil.xlsx"
SHEET_NAME = "Sheet1"   # ajuste se a aba da rede for outra

COD_LINHA = 'Linhas'
COD_TIPO_REDE = 'Tipo Rede'
COD_REGIAO = 'Região'
COD_ESTADO = 'Estado'
COD_CIDADE = 'Cidade'
COD_PRODUTO = 'Produto'
COD_PLANO = 'Plano'
COD_PRESTADOR = 'Prestador'
COD_MODALIDADE = 'Modalidade'


# ===============================
# Helpers
# ===============================
def _norm(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s.upper()


def _contains(series, value):
    v = _norm(value)
    if not v:
        return np.ones(len(series), dtype=bool)
    return series.str.contains(re.escape(v), na=False)


def _equals(series, value):
    v = _norm(value)
    if not v:
        return np.ones(len(series), dtype=bool)
    return series == v


# ===============================
# SFTP
# ===============================
def _parse_sftp_url(url):
    u = urlparse(url)
    return u.hostname, u.port or 22, u.username, u.path


def _ler_excel_sftp():
    load_dotenv()
    password = os.getenv("PASSWORD_ADMIN_SFTP")
    if not password:
        raise RuntimeError("PASSWORD_ADMIN_SFTP não definido no .env")

    host, port, user, path = _parse_sftp_url(SFTP_URL)

    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        with sftp.open(path, "rb") as f:
            df = pd.read_excel(io.BytesIO(f.read()), sheet_name=SHEET_NAME)
            df.columns = [c.strip() for c in df.columns]
            return df
    finally:
        sftp.close()
        transport.close()


# ===============================
# Função pública
# ===============================
def buscar_rede_credenciada(payload: dict, debug: bool = False):
    """
    Payload aceito:
    {
        "linha",
        "tipo_rede",
        "regiao",
        "estado",
        "cidade",
        "produto",
        "plano",
        "modalidade",
        "prestador",
        "q"
    }
    """

    df = _ler_excel_sftp()

    # Normaliza colunas
    df["_LINHA"]     = df[COD_LINHA].apply(_norm)
    df["_TIPO"]      = df[COD_TIPO_REDE].apply(_norm)
    df["_REGIAO"]    = df[COD_REGIAO].apply(_norm)
    df["_ESTADO"]    = df[COD_ESTADO].apply(_norm)
    df["_CIDADE"]    = df[COD_CIDADE].apply(_norm)
    df["_PRODUTO"]   = df[COD_PRODUTO].apply(_norm)
    df["_PLANO"]     = df[COD_PLANO].apply(_norm)
    df["_MODAL"]     = df[COD_MODALIDADE].apply(_norm)
    df["_PREST"]     = df[COD_PRESTADOR].apply(_norm)

    mask = np.ones(len(df), dtype=bool)

    def step(m, label):
        nonlocal mask
        mask &= m
        if debug:
            print(f"[DEBUG] {label}: {mask.sum()}")

    step(_contains(df["_LINHA"], payload.get("linha")), "LINHA")
    step(_contains(df["_TIPO"], payload.get("tipo_rede")), "TIPO_REDE")
    step(_contains(df["_REGIAO"], payload.get("regiao")), "REGIAO")
    step(_equals(df["_ESTADO"], payload.get("estado")), "ESTADO")
    step(_contains(df["_CIDADE"], payload.get("cidade")), "CIDADE")
    step(_contains(df["_PRODUTO"], payload.get("produto")), "PRODUTO")
    step(_contains(df["_PLANO"], payload.get("plano")), "PLANO")
    step(_contains(df["_MODAL"], payload.get("modalidade")), "MODALIDADE")
    step(_contains(df["_PREST"], payload.get("prestador")), "PRESTADOR")

    q = payload.get("q")
    if q:
        qn = _norm(q)
        step(
            df["_PREST"].str.contains(qn) |
            df["_PLANO"].str.contains(qn) |
            df["_PRODUTO"].str.contains(qn),
            "BUSCA_LIVRE"
        )

    base = df.loc[mask]

    if base.empty:
        return []

    # Agrupamento final para API
    resultado = []
    for (plano, produto, modalidade), g in base.groupby([COD_PLANO, COD_PRODUTO, COD_MODALIDADE]):
        resultado.append({
            "plano": plano,
            "produto": produto,
            "modalidade": modalidade,
            "regiao": g[COD_REGIAO].iloc[0],
            "estado": g[COD_ESTADO].iloc[0],
            "cidade": g[COD_CIDADE].iloc[0],
            "tipo_rede": g[COD_TIPO_REDE].iloc[0],
            "linha": g[COD_LINHA].iloc[0],
            "prestadores": sorted(g[COD_PRESTADOR].dropna().unique().tolist())
        })

    return resultado


plano: str
estado: str
regiao: str
cidade: str
tipo_rede: str

payload = {
    "plano" : "Amil S380",
    "estado" : "Acre",
    "regiao" : "Norte",
    "cidade" : "Rio Branco",
    "tipo_rede" : "Nacional"
}

print(buscar_rede_credenciada(payload=payload, debug=True))