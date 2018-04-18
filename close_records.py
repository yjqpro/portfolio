# !/usr/bin/env python
# encoding: utf-8
import sys
import json
from datetime import datetime, date, timedelta, time
import time as t
import pandas as pd
import os

contract_multiple = 5.0


def settle(output):
    df = pd.DataFrame(output)
    if not len(df):
        return 0.0
    buys = df[df.direction == 'Buy'].reset_index()
    sells = df[df.direction != 'Buy'].reset_index()
    return ((sells.price - buys.price) * contract_multiple).sum()


def read_file(path):
    datas = {}
    with open(path, 'r') as f:
        datas = json.load(f)
    orders = {}
    settle_timestamps = []
    datetime_from = date(2016, 12, 5)
    datetime_to = date(2017, 11, 23)
    for i in range((datetime_to - datetime_from).days + 1):
        day = datetime_from + timedelta(days=i)
        dt = datetime.combine(day, time(15, 0, 0))
        dt = dt + timedelta(hours=8)

        settle_timestamps.append(int(t.mktime(dt.timetuple()) * 1000))
    output = {
        'order_id': [],
        'direction': [],
        'datetime': [],
        'price': [],
        'qty': []
    }
    result = {
        'date': [],
        'pl': [],
    }
    for order in datas['datas']:
        timestamp = int(order['timestamp'])
        while timestamp > settle_timestamps[0]:
            dt = datetime.fromtimestamp(settle_timestamps[0] / 1000.0)
            #print('{}:{}'.format(dt.strftime('%Y-%m-%d'), settle(output)))
            result['date'].append(dt.strftime('%Y-%m-%d'))
            result['pl'].append(settle(output))
            #output = reset_output()
            del settle_timestamps[0]

        if order['type'] == 'limit_order':
            orders[order['order_id']] = order['direction']
        elif order['type'] == 'trade_order':
            order_id = order['order_id']
            assert order_id in orders, order_id
            for i in range(int(order['qty'])):
                output['order_id'].append(order_id)
                output['direction'].append(orders[order_id])
                output['datetime'].append(
                    datetime.utcfromtimestamp(
                        int(order['timestamp']) / 1000.0))
                output['price'].append(float(order['price']))
                output['qty'].append(1)
        else:
            pass
    dt = datetime.fromtimestamp(settle_timestamps[0] / 1000.0)
    result['date'].append(dt.strftime('%Y-%m-%d'))
    result['pl'].append(settle(output))
    return result, output


def main():
    result, trade_orders = read_file(sys.argv[1])
    close_records = {
        'direction': [],
        'open_order_id': [],
        'open_datetime': [],
        'open_price': [],
        'close_order_id': [],
        'close_datetime': [],
        'close_price': [],
        'trailing_pl': []
    }
    position_details = []

    trailing_pl = 0.0
    for i in range(len(trade_orders['order_id'])):
        trade = {
            'order_id': trade_orders['order_id'][i],
            'direction': trade_orders['direction'][i],
            'datetime': trade_orders['datetime'][i],
            'price': trade_orders['price'][i],
            'qty': trade_orders['qty'][i]
        }
        if not len(position_details
                   ) or position_details[0]['direction'] == trade['direction']:
            position_details.append(trade)
        else:
            position_detail = position_details[0]
            close_records['direction'].append(position_detail['direction'])
            close_records['open_order_id'].append(position_detail['order_id'])
            close_records['open_datetime'].append(position_detail['datetime'])
            close_records['open_price'].append(position_detail['price'])
            close_records['close_order_id'].append(trade['order_id'])
            close_records['close_datetime'].append(trade['datetime'])
            close_records['close_price'].append(trade['price'])
            trailing_pl = trailing_pl + (
                trade['price'] - position_detail['price']
                if position_detail['direction'] == 'Buy' else
                position_detail['price'] - trade['price'])
            close_records['trailing_pl'].append(trailing_pl)
            del position_details[0]

    df = pd.DataFrame(close_records)
    df[[
        'direction', 'open_order_id', 'open_datetime', 'open_price',
        'close_order_id', 'close_datetime', 'close_price', 'trailing_pl'
    ]].to_csv('close_records.csv')


if __name__ == '__main__':
    sys.exit(main())
