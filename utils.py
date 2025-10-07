import requests
import datetime
import dotenv
import time
import os
from support_files import *
import pandas as pd
import streamlit as st
import altair as alt

dotenv.load_dotenv(dotenv_path='secret.env')

MERCADO_BITCOIN_ENDPOINT_VOLUME_URL= os.getenv("MERCADO_BITCOIN_ENDPOINT_VOLUME_URL")
BITSO_ENDPOINT_URL= os.getenv("BITSO_ENDPOINT_URL")
FOXBIT_ENDPOINT_URL= os.getenv("FOXBIT_ENDPOINT_URL")
BINANCE_BRL_ENDPOINT_URL= os.getenv("BINANCE_BRL_ENDPOINT_URL")
COINSPH_ENDPOINT_URL = os.getenv("COINSPH_ENDPOINT_URL")
BITVAVO_ENDPOINT_URL = os.getenv("BITVAVO_ENDPOINT_URL")
MERCADO_BITCOIN_ENDPOINT_TRADES_URL = os.getenv("MERCADO_BITCOIN_ENDPOINT_TRADES_URL")
NOVADAX_KLINE_ENDPOINT_URL = os.getenv("NOVADAX_KLINE_ENDPOINT_URL")
RIPIO_ENDPOINT_URL = os.getenv("RIPIO_ENDPOINT_URL")
PASS = os.getenv("PASSWORD")
USER = os.getenv("USER")



@st.cache_data(ttl=3600)
def get_volume_30m():

    candleslookback=336 #10 days
    interval=interval_list[1]

    newlist=[]

    for s in pairs:
        data_mercadobtc= get_volume_mercado_bitcoin_30m(candleslookback, pairs[s]["mercadobitcoin"],interval)
        data_binance_brl = get_volume_binance_30m(candleslookback, pairs[s]["binance_brl"],interval)
        data_binance_ars = get_volume_binance_30m(candleslookback, pairs[s]["binance_ars"], interval)
        data_binance_mxn = get_volume_binance_30m(candleslookback, pairs[s]["binance_mxn"], interval)
        data_foxbit = get_volume_foxbit_30m(candleslookback, pairs[s]["foxbit"],interval)
        data_bitvavo = get_volume_bitvavo_30m(candleslookback, pairs[s]["bitvavo"],interval)
        data_bitso_brl = get_volume_bitso_30m(candleslookback, pairs[s]["bitso_brl"],interval)
        data_bitso_ars = get_volume_bitso_30m(candleslookback, pairs[s]["bitso_ars"],interval)
        data_bitso_mxn = get_volume_bitso_30m(candleslookback, pairs[s]["bitso_mxn"],interval)
        data_coinsph = get_volume_coinsph_30m(candleslookback, pairs[s]["coinsph"],interval)
        data_novadax = get_volume_novadax_30m(candleslookback, pairs[s]["novadax"], interval)
        data_ripio = get_volume_ripio_30m(candleslookback, pairs[s]["ripio"], interval)


        newlist.extend(data_mercadobtc)
        newlist.extend(data_binance_brl)
        newlist.extend(data_binance_ars)
        newlist.extend(data_binance_mxn)
        newlist.extend(data_foxbit)
        newlist.extend(data_bitvavo)
        newlist.extend(data_bitso_brl)
        newlist.extend(data_bitso_ars)
        newlist.extend(data_bitso_mxn)
        newlist.extend(data_coinsph)
        newlist.extend(data_novadax)
        newlist.extend(data_ripio)

    return newlist


def get_volume_mercado_bitcoin_30m(candlesloockaback:int,basepair,interval):

    # OBS: Has control of from and to
       # how many candles back
    url = MERCADO_BITCOIN_ENDPOINT_VOLUME_URL

    match interval:
        case "30m":
            interval_delta = 30
        case "5m":
            interval_delta = 5

    rightmostdate = datetime.datetime.now(datetime.timezone.utc)
    minus = datetime.timedelta(minutes=candlesloockaback*interval_delta)

    leftmostdate = rightmostdate - minus

    rightmostdate_ts = int(rightmostdate.timestamp())
    leftmostdate_ts = int(leftmostdate.timestamp())

    match interval:
        case "30m":
            interval_delta= 30
        case "5m":
            interval_delta= 5


    params = {
        "symbol": basepair,
        "resolution": interval,
        "to":rightmostdate_ts,
        "from":leftmostdate_ts,
        "countback": candlesloockaback
    }

    try:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code}, {response.text}")

        data = response.json()

        newlist = []

        basepair_formatted= basepair.upper().replace("-","")

        for r in range(candlesloockaback):

            register={
                "exchange": f"({basepair_formatted[-3:]}) Mercado_Bitcoin",
                "pair": basepair_formatted[:-3],
                "timestamp" : datetime.datetime.utcfromtimestamp(data["t"][r]),
                "volume" : data["v"][r]
            }

            newlist.append(register)

        return newlist

    except Exception as error:
        return []

def get_trades_mercadobitcoin(candlesloockaback:int,basepair):

    url= MERCADO_BITCOIN_ENDPOINT_TRADES_URL.format(symbol=basepair)

    rightmostdate = datetime.datetime.now(datetime.timezone.utc)
    minus = datetime.timedelta(hours=24)

    leftmostdate = rightmostdate-minus

    rightmostdate_ts = int(rightmostdate.timestamp())
    leftmostdate_ts = int(leftmostdate.timestamp())


    params={

        "to": rightmostdate_ts,
        "from":leftmostdate_ts,
    }

    response=requests.get(url,params=params)

    data=response.json()

    return data

def get_volume_binance_30m(candlesloockaback:int,basepair,interval):
    limit = candlesloockaback   # how many candles back
    url = BINANCE_BRL_ENDPOINT_URL
    params = {
        "symbol": basepair,  # ex: BTCBRL, BTCUSDT
        "interval": interval,
        "limit": limit,
    }

    try:

        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code}, {response.text}")

        data = response.json()

        newlist=[]

        for v in data:

            register={
                "exchange" : f"({basepair[-3:]}) Binance",
                "pair":basepair[:-3],
                "timestamp" : datetime.datetime.utcfromtimestamp(int(v[0]) / 1000),
                "volume": v[5]
            }

            newlist.append(register)

        return newlist

    except Exception as erro:
        return []

def get_volume_foxbit_30m(candlesloockaback:int,basepair,interval):

    #there is no limit look back, then i needed to do a diferent startTime condition

    candlesloockaback+=1

    match interval:
        case "30m":
            interval_delta= 30
        case "5m":
            interval_delta= 5


    url =FOXBIT_ENDPOINT_URL.format(pair=basepair)
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(minutes=candlesloockaback*interval_delta)


    params = {
        "interval": interval,
        "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    try:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code}, {response.text}")

        data = response.json()

        newlist=[]

        basepair_formatted= basepair.upper()

        print(data)

        for v in data:

            register={

                "exchange" : f"({basepair_formatted[-3:]}) Foxbit",
                "pair":basepair_formatted[:-3],
                "timestamp" : datetime.datetime.utcfromtimestamp(int(v[0])/1000),
                "volume" : float(v[5])

            }
            newlist.append(register)

        newlist.pop(-1)

        return newlist

    except Exception as erro:
        return erro

def get_volume_bitso_30m(candlesloockback:int,basepair,interval):
    url = BITSO_ENDPOINT_URL

    match interval:
        case "30m":
            interval= 1800
        case "5m":
            interval= 300

    end = int(time.time() * 1000)
    start = end - (candlesloockback * interval * 1000)


    params = {
        "book": basepair,
        "time_bucket": interval,
        "start": start,
        "end": end
    }

    try:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code}, {response.text}")

        data = response.json()

        newlist=[]

        basepair_formatted = basepair.upper().replace("_","")

        for v in data["payload"]:
            register={
                "exchange" : f"({basepair_formatted[-3:]}) Bitso",
                "pair": basepair_formatted[:-3],
                "timestamp": datetime.datetime.utcfromtimestamp(v["bucket_start_time"]/1000),
                "volume": v["volume"]
            }
            newlist.append(register)

        newlist.pop(0)
        newlist.sort(key=lambda x: x['timestamp'])

        return newlist

    except Exception as error:
        return []

def get_volume_coinsph_30m(candlesloockback, basepair,interval):
    url = COINSPH_ENDPOINT_URL


    params = {
        "symbol": basepair,
        "interval": interval,
        "limit": candlesloockback
    }

    try:

        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code}, {response.text}")

        data = response.json()

        newlist = []


        for v in data:

            register={
                "exchange" : f"({basepair[-3:]}) CoinsPH",
                "pair":basepair[:-3],
                "timestamp" : datetime.datetime.utcfromtimestamp(v[0] / 1000),
                "volume" : v[5]
            }

            newlist.append(register)

        newlist.sort(key=lambda x: x['timestamp'])

        return newlist

    except Exception as erro:
        return []

def get_volume_bitvavo_30m(candleslookback, basepair,interval):
    url = BITVAVO_ENDPOINT_URL.format(basepair=basepair)

    params = {
        "interval": interval,
        "limit": candleslookback,
        "start": time.time()

    }

    try:

        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code}, {response.text}")

        data = response.json()

        newlist = []

        basepair_formatted= basepair.upper().replace("-","")

        for c in data:
            register={
                "exchange" : f"({basepair_formatted[-3:]}) Bitvavo",
                "pair": basepair_formatted[:-3],
                "timestamp" : datetime.datetime.utcfromtimestamp(c[0] / 1000),
                "volume" : c[5]
            }
            newlist.append(register)

        newlist.sort(key=lambda x: x['timestamp'])

        return newlist

    except Exception as erro:
        return []


def get_volume_novadax_30m(candleslookback, basepair,interval):

    url= NOVADAX_KLINE_ENDPOINT_URL

    match interval:
        case "30m":
            interval = "HALF_HOU"
        case "5m":
            interval = "FIVE_MIN"

    end = int(time.time())
    start = int(end - (candleslookback * 1800 if interval== "HALF_HOU" else 5))


    params = {
        "symbol": basepair,
        "unit": interval,
        "from": start,
        "to": end
    }
    try:
        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            raise Exception(f"Erro na API NovaDAX (kline): {resp.status_code}, {resp.text}")

        j = resp.json()
        data = j.get("data", [])
        print(j)

        newlist = []
        basepair_formatted = basepair.upper().replace("_", "")

        pair = basepair_formatted[:-3]
        quote = basepair_formatted[-3:]

        for candle in data:
            ts = candle.get("score")
            dt = datetime.datetime.utcfromtimestamp(ts)
            volume = float(candle.get("amount", 0))

            newlist.append({
                "exchange": f"({quote}) NovaDAX",
                "pair": pair,
                "timestamp": dt,
                "volume": volume
            })

        # ordenar por timestamp
        newlist.sort(key=lambda x: x["timestamp"])
        return newlist

    except Exception as e:
        print("Erro em get_volume_novadax_30m (kline):", e)
        return []

def get_volume_ripio_30m(candleslookback, basepair, interval):
    url = RIPIO_ENDPOINT_URL

    match interval:
        case "30m":
            interval_minutes = 30
        case "5m":
            interval_minutes = 5
        case _:
            interval_minutes = 30



    end_dt = datetime.datetime.utcnow()
    start_dt = end_dt - datetime.timedelta(minutes=candleslookback * interval_minutes)

    # Formato ISO 8601 com 'Z' (UTC)
    start_iso = start_dt.replace(microsecond=0).isoformat() + "Z"
    end_iso = end_dt.replace(microsecond=0).isoformat() + "Z"

    print("Start:", start_iso)
    print("End:", end_iso)

    trades = []
    # O cursor deve começar como None para a primeira requisição
    cursor = None
    try:
        while True:

            params = {
                "pair": basepair.upper(),
                "start_time": start_iso,
                "end_time": end_iso,
                "page_size": 1000  # Máximo de 1000 por página
            }

            # 1. Se o cursor existe, adicione-o ao parâmetro 'c'
            if cursor:
                params["c"] = cursor

            response = requests.get(url, params=params)
            if response.status_code != 200:
                raise Exception(f"Erro na API Ripio: {response.status_code}, {response.text}")

            j = response.json()

            page_trades = j.get("data", {}).get("trades", [])
            trades.extend(page_trades)

            # 3. Atualiza o cursor para a próxima iteração
            # O próximo cursor está no campo 'nc' no dicionário 'data'
            cursor = j.get("data", {}).get("nc")

            # 4. Condição de parada
            # Parar se:
            # a) A API não retornou um cursor 'nc', OU
            # b) A página de trades está vazia (última página atingida)
            if not cursor or len(page_trades) == 0:
                break

        # 🔽 Continuação do seu código para processar os trades...
        # (Não alterado)
        basepair_formatted = basepair.upper().replace("_", "")
        pair = basepair_formatted[:-3]
        quote = basepair_formatted[-3:]

        newlist = [
            {
                "exchange": f"({quote}) Ripio",
                "pair": pair,
                "date": x.get("date"),
                "volume": float(x.get("amount", 0))
            }
            for x in trades
        ]

        interval_str = "30m" if interval_minutes == 30 else "5m"

        result = ripio_aggregate_trades_to_candles(basepair, newlist, interval_str)

        return result

    except Exception as e:
        return []

def ripio_aggregate_trades_to_candles(basepair, trades, interval="30m"):
    if not trades:
        return []

    df = pd.DataFrame(trades)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df.set_index("date", inplace=True)

    resample_map = {
        "5m": "5T",
        "15m": "15T",
        "30m": "30T",
        "1h": "1H"
    }
    resample_interval = resample_map.get(interval, "30T")

    basepair_formatted = basepair.upper().replace("_", "")
    pair = basepair_formatted[:-3]
    quote = basepair_formatted[-3:]

    # 🔹 Agrupa e soma o volume
    grouped = df.resample(resample_interval).agg({
        "volume": "sum",
        "exchange": "first",
        "pair": "first"
    })

    # 🔹 Cria uma sequência completa de candles até o horário atual (UTC)
    full_range = pd.date_range(
        start=df.index.min().floor(resample_interval),
        end=pd.Timestamp.utcnow().ceil(resample_interval),
        freq=resample_interval
    )

    # 🔹 Reindexa para preencher candles ausentes com volume 0
    grouped = grouped.reindex(full_range, fill_value=0)
    grouped = grouped.rename_axis("date").reset_index()

    # 🔹 NOVO BLOCO: Preenche metadados faltantes de forma robusta
    # Isso garante que as colunas de texto (que ficaram com 0 ou NaN após o reindex)
    # sejam preenchidas com os metadados corretos.

    # 1. Trata o valor 0 (se o fill_value=0 funcionou para strings)
    exchange_name = f"({quote}) Ripio"
    grouped["exchange"] = grouped["exchange"].replace(0, exchange_name)
    grouped["pair"] = grouped["pair"].replace(0, pair)

    # 2. Trata valores nulos (NaN), que são mais comuns no reindex de strings
    grouped["exchange"] = grouped["exchange"].fillna(exchange_name)
    grouped["pair"] = grouped["pair"].fillna(pair)

    # Se o Pandas gerou uma string vazia '', use o seguinte:
    grouped["exchange"] = grouped["exchange"].replace('', exchange_name)
    grouped["pair"] = grouped["pair"].replace('', pair)

    # 🔹 Converte para lista final
    newlist = []
    for _, row in grouped.iterrows():
        newlist.append({
            "exchange": row["exchange"],
            "pair": row["pair"],
            "timestamp": row["date"],
            "volume": float(row["volume"])
        })

    return newlist

def dataframe (marketdata):

    marketdata=pd.DataFrame(marketdata)


    return marketdata

def authenticator (user,password):

    if user.lower()==USER and password==PASS:
        return True
    else:
        return False


