'''
Binance Balancer
2020 - Sandy Bay

Re-balances every hour based on manually fixed allocations
Defaults to limit orders which are cancelled if unfilled and recalculated for the new rebalance

'''
import math
import time
import pandas as pd
import numpy as np
from binance.client import Client
from apscheduler.schedulers.blocking import BlockingScheduler
from csv import writer
from datetime import datetime
import pprint


# set keys
api_key = ''
api_secret = ''

# set weights
# look for 6 to 12 month value
# hedge fiat (usd,rub,try,eur)
# focus on trusted cryptos with the following priority
# security
# value
# usage
# fees
# privacy
# speed

lastweights = {
    "AAVE": 0.0625,
    "ADA":  0.0625,
    "BNB":  0.0625,
    "BTC":  0.0625,
    "DOT":  0.0625,
    "EOS":  0.0625,
    "ETH":  0.0625,
    "LINK": 0.0625,
    "LTC":  0.0625,
    "RUNE": 0.0625,
    "SOL":  0.0625,
    "THETA":0.0625,
    "UNI":  0.0625,
    "XLM":  0.0625,
    "XMR":  0.0625,
    "XRP":  0.0625,
}

# Timestamped bitcoin and usd portfolio value
csvBalance = 'binance_balance_log.csv'

# globals
prices = {} # asset prices in bnb
prices['BNB'] = 1.0
BNBUSD = 0.0
balances = {}
balancesbnb = {}
totalbnb = 0
diffs = {}
steps = {}
ticks = {}
minQtys = {}

# connect
client = Client(api_key, api_secret)
# time offset binance bug fix
# servTime = client.get_server_time()
# time_offset  = servTime['serverTime'] - int(time.time() * 1000)

def sanityCheck():
    sane = False
    sumWeights = round(sum(lastweights.values()),4)
    if sumWeights == 1.0000:
        sane = True
    else:
        print("Incorrect weights. Sum ratios must equal 1.0. Currently ",sumWeights)
    return sane

def append_list_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)

def saveBalance():
    # Returns a datetime object containing the local date and time
    dateTimeObj = datetime.now()
    # List of row elements (Timestamp, BNB balance, USD balance, Notes)
    row_contents = [str(dateTimeObj), str(totalbnb) , str(totalbnb * BNBUSD)]
    # Append a list as new line to an old csv file
    append_list_as_row(csvBalance, row_contents)


def getPrices():
    global prices, BNBUSD
    # get prices
    priceinfo = client.get_all_tickers()
    print('priceinfo:')
    pprint.pprint(priceinfo)
    for price in priceinfo:
        sym = price['symbol']
        asset = sym[0:-3]
        quote = sym[-3:]
        p = float(price['price'])
        if sym == 'BNBUSDT':
            BNBUSD = p
            prices['USDT'] = 1 / p
        if sym == 'BNBBTC':
            prices['BTC'] = 1 / p
        if sym == 'BNBETH':
            prices['ETH'] = 1 / p
        elif quote == 'BNB':
            if asset in lastweights:
                print(f'quote: BNB, asset: {asset}')
                prices[asset] = p
    print('Prices (BNB)')
    pprint.pprint(prices)

def getBalance():
    global balances, balancesbnb, totalbnb
    totalbnb = 0
    # get balance
    info = client.get_account()
    # print("client.get_account() balances:")
    # pprint.pprint(client.get_account()['balances'])
    pprint.pprint(f'balancesbnb: {balancesbnb}')
    pprint.pprint('lastweights:')
    pprint.pprint(lastweights)
    for balance in info['balances']:
        pprint.pprint('processing balance:')
        pprint.pprint(balance)
        free = float( balance['free'] )
        locked =  float( balance['locked'] )
        asset = balance['asset']
        print(f'asset: {asset}')
        if asset in lastweights:
            bal = free + locked
            balances[ asset ] = bal
            balancesbnb[ asset ] = bal * prices[asset]
            totalbnb = totalbnb + bal * prices[asset]
    # print(balances)
    print("Balances (BNB)")
    pprint.pprint(balancesbnb)
    print("Total (BNB / USD)")
    print(totalbnb," BNB /  $ ",totalbnb*BNBUSD)

def getDiffs():
    global diffs
    # get difference
    for asset in lastweights:
        adjshare = totalbnb * lastweights[asset]
        currshare = balancesbnb[asset]
        diff = adjshare - currshare
        diffs [ asset ] = diff
    diffs = dict(sorted(diffs.items(), key=lambda x: x[1]))
    print('Adjustments (BNB)')
    pprint.pprint(diffs)

def cancelOrders():
    # cancel current orders
    print('Canceling open orders')
    orders = client.get_open_orders()
    for order in orders:
        sym = order['symbol']
        asset = sym[0:-3]
        if sym == 'BNBUSDT' or asset in lastweights:
            orderid = order['orderId']
            result = client.cancel_order(symbol=sym,orderId=orderid)
            # print(result)

def step_size_to_precision(ss):
    return ss.find('1') - 1

def format_value(val, step_size_str):
    precision = step_size_to_precision(step_size_str)
    if precision > 0:
        return "{:0.0{}f}".format(val, precision)
    return math.floor(int(val))

def getSteps():
    global steps, ticks, minQtys
    # step sizes
    info = client.get_exchange_info()
    for dat in info['symbols']:
        sym = dat['symbol']
        asset = dat['baseAsset']
        quote = dat['quoteAsset']
        filters = dat['filters']
        if quote == 'BNB' and asset in lastweights:
            for filt in filters:
                if filt['filterType'] == 'LOT_SIZE':
                    steps[asset] = filt['stepSize']
                elif filt['filterType'] == 'PRICE_FILTER':
                    ticks[asset] = filt['tickSize']
                elif filt['filterType'] == 'MIN_NOTIONAL':
                    minQtys[asset] = filt['minNotional']
        elif sym == 'BNBUSDT':
            for filt in filters:
                if filt['filterType'] == 'LOT_SIZE':
                    steps[sym] = filt['stepSize']
                elif filt['filterType'] == 'PRICE_FILTER':
                    ticks[sym] = filt['tickSize']
                elif filt['filterType'] == 'MIN_NOTIONAL':
                    minQtys['USDT'] = filt['minNotional']


def placeOrders():
    # all go through bnb
    # this can be smart routed later
    global diffs
    getSteps()
    # set sell orders
    for asset in diffs:
        diff = diffs[asset]
        if asset != 'BNB':
            thresh = float(minQtys[asset])
            if  diff <  -0.0001 : # threshold $ 1
                if asset != 'BNB' and asset != 'USDT':
                    sym = asset + 'BNB'
                    amountf = 0-diff # amount in bnb

                    amount = format_value ( amountf / prices[asset] , steps[asset] )
                    price = format_value ( prices [ asset ] + 0.003 * prices [ asset ], ticks[asset] )# adjust for fee
                    minNotion = float(amount) * float(price)
                    if minNotion > thresh:
                        diffs[asset] = diffs[asset] + amountf
                        diffs['BNB'] = diffs['BNB'] - amountf
                        print('Setting sell order for {}, amount:{}, price:{}, thresh:{}'.format(asset,amount,price,thresh))
                        order = client.order_limit_sell(
                            symbol = sym,
                            quantity = amount,
                            price = price )

                elif asset == 'USDT':
                    sym = 'BNBUSDT'
                    amount = 0-diff
                    if amount > ( thresh / BNBUSD ):
                        diffs[asset] = diffs[asset] + amount
                        diffs['BNB'] = diffs['BNB'] - amount
                        amount = format_value ( amount  , steps[sym] )
                        price = format_value ( BNBUSD - 0.003 * BNBUSD , ticks[sym])# adjust for fee
                        print('Setting buy order for {}, amount:{}, price:{}'.format(asset,amount,price))
                        order = client.order_limit_buy(
                            symbol = sym,
                            quantity = amount,
                            price = price )



    # set buy orders
    diffs = dict(sorted(diffs.items(), key=lambda x: x[1], reverse=True))

    for asset in diffs:
        diff = diffs[ asset ]
        if asset != 'BNB':
            thresh = float( minQtys[ asset ] )
            if  diff >  0.0001 : # threshold $ 1
                if asset != 'BNB' and asset != 'USDT':
                    sym = asset + 'BNB'
                    amountf = diff

                    amount = format_value ( amountf / prices[asset] , steps[asset] )
                    price = format_value ( prices [ asset ] - 0.003 * prices [ asset ] , ticks[asset] )# adjust for fee
                    minNotion = float(amount) * float(price)
                    if minNotion > thresh:
                        diffs[asset] = diffs[asset] - amountf
                        diffs['BNB'] = diffs['BNB'] + amountf
                        print('Setting buy order for {}, amount:{}, price:{}, thresh:{}'.format(asset,amount,price,thresh))
                        order = client.order_limit_buy(
                            symbol = sym,
                            quantity = amount,
                            price = price )

                elif asset == 'USDT':
                    sym = 'BNBUSDT'
                    amount = diff
                    if amount > ( thresh / BNBUSD ):
                        diffs[asset] = diffs[asset] - amount
                        diffs['BNB'] = diffs['BNB'] + amount
                        amount = format_value ( amount  , steps[sym] )
                        price = format_value ( BNBUSD + 0.003 * BNBUSD , ticks[sym])# adjust for fee
                        print('Setting sell order for {}, amount:{}, price:{}'.format(asset,amount,price))
                        order = client.order_limit_sell(
                            symbol = sym,
                            quantity = amount,
                            price = price )


    # print ( 'Final differences' )
    # pprint.pprint ( diffs )

def iteratey():
    sane = sanityCheck()
    if sane == True:
        getPrices()
        getBalance()
        getDiffs()
        cancelOrders()
        placeOrders()
        saveBalance()

iteratey()

scheduler = BlockingScheduler()
scheduler.add_job(iteratey, 'interval', minutes=20)
scheduler.start()
