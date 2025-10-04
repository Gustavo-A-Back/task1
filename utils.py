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
PASS = os.getenv("PASSWORD")
USER = os.getenv("USER")



@st.cache_data(ttl=3600)
def get_volume_30m():

    candleslookback=336 #10 days
    interval=interval_list[1]

    newlist=[]

    for s in pairs:
        data_mercadobtc= get_volume_mercado_bitcoin_30m(candleslookback, pairs[s]["mercadobitcoin"],interval)
        data_binance = get_volume_binance_30m(candleslookback, pairs[s]["binance"],interval)
        data_foxbit = get_volume_foxbit_30m(candleslookback, pairs[s]["foxbit"],interval)
        data_bitvavo = get_volume_bitvavo_30m(candleslookback, pairs[s]["bitvavo"],interval)
        data_bitso = get_volume_bitso_30m(candleslookback, pairs[s]["bitso"],interval)
        data_coinsph = get_volume_coinsph_30m(candleslookback, pairs[s]["coinsph"],interval)

        newlist.extend(data_mercadobtc)
        newlist.extend(data_binance)
        newlist.extend(data_foxbit)
        newlist.extend(data_bitvavo)
        newlist.extend(data_bitso)
        newlist.extend(data_coinsph)

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
                "exchange": "(BRL) Mercado_Bitcoin",
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
                "exchange" : "(BRL) Binance",
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

        for v in data:

            register={

                "exchange" : "(BRL) Foxbit",
                "pair":basepair_formatted[:-3],
                "timestamp" : datetime.datetime.utcfromtimestamp(int(v[0])/1000),
                "volume" : v["volume"][v]

            }
            newlist.append(register)

        newlist.pop(-1)

        return newlist

    except Exception as erro:
        return []

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
                "exchange" : "(BRL) Bitso",
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
                "exchange" : "(PHP) CoinsPH",
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
        print(data)

        newlist = []

        basepair_formatted= basepair.upper().replace("-","")

        for c in data:
            register={
                "exchange" : "(EUR) Bitvavo",
                "pair": basepair_formatted[:-3],
                "timestamp" : datetime.datetime.utcfromtimestamp(c[0] / 1000),
                "volume" : c[5]
            }
            newlist.append(register)

        newlist.sort(key=lambda x: x['timestamp'])

        return newlist

    except Exception as erro:
        return []


def dataframe (marketdata):

    marketdata=pd.DataFrame(marketdata)


    return marketdata


def authenticator (user,password):

    if user.lower()==USER and password==PASS:
        return True
    else:
        return False




