# !/usr/bin/env python
# encoding: utf-8
import sys
import os
import json
import datetime
import time
import bisect
import pandas as pd
from functools import total_ordering


class Account(object):
    """Docstring for Account. """

    def __init__(self):
        """TODO: to be defined1. """

    def order(self, order_id, direction, price, qty):
        raise "no implement"

    def trade(self, order_id, price, qty):
        raise "no implement"

    def cancel(self, order_id, leaves_qty):
        raise "no implement"

    def settle(self, timestamp):
        raise "no implement"


@total_ordering
class Event(object):
    """Docstring for Event. """

    def __init__(self, timestamp):
        self._timestamp = int(timestamp)

    def __cmp__(self, other):
        pass
        #  return cmp(self._timestamp, other._timestamp)

    def __eq__(self, other):
        return self._timestamp == other._timestamp

    def __lt__(self, other):
        return self._timestamp < other._timestamp

    def do(self, account):
        raise "error"


class LimitOrder(Event):
    """Docstring for LimitOrder. """

    def __init__(self, timestamp, order_id, direction, price, qty):
        """TODO: to be defined1. """
        Event.__init__(self, timestamp)
        self._order_id = order_id
        self._direction = direction
        self._price = price
        self._qty = qty

    def do(self, account):
        account.order(self._order_id, self._direction, self._price, self._qty)


class TradeOrder(Event):
    """Docstring for TradeOrder. """

    def __init__(self, timestamp, order_id, price, qty):
        """TODO: to be defined1. """
        Event.__init__(self, timestamp)
        self._order_id = order_id
        self._price = price
        self._qty = qty

    def do(self, account):
        account.trade(self._order_id, self._price, self._qty)


class CancelOrder(Event):
    """Docstring for CancelOrder. """

    def __init__(self, timestamp, order_id):
        """TODO: to be defined1. """
        Event.__init__(self, timestamp)
        self._order_id = order_id

    def do(self, account):
        account.cancel(self._order_id)


class DailySettle(Event):
    """Docstring for DailySettle. """

    def __init__(self, timestamp):
        Event.__init__(self, timestamp)

    def do(self, account):
        account.settle(self._timestamp)


class Order(object):
    """Docstring for Order. """

    def __init__(self, direction, leaves_qty):
        """TODO: to be defined1.

        :direction: TODO
        :leaves_qty: TODO

        """
        self._direction = direction
        self._leaves_qty = leaves_qty

    def trade(self, qty):
        self._leaves_qty = self._leaves_qty - qty
        assert (self._leaves_qty >= 0)


class PositionDetail(object):
    """Docstring for PositionDetail. """

    def __init__(self, direction, price, leaves_qty):
        self._direction = direction
        self._price = price
        self._leaves_qty = leaves_qty

    def close(self, price, qty, update_pl):
        close_qty = min(self._leaves_qty, qty)
        self._leaves_qty = self._leaves_qty - close_qty
        if self._direction == "Buy":
            update_pl((price - self._price) * close_qty)
        else:
            update_pl((self._price - price) * close_qty)
        return qty - close_qty


class FutureAccount(object):
    """Docstring for FutureAccount. """

    def __init__(self, contract_multiple):
        self._orders = {}
        self._long_position = []
        self._short_position = []
        self._long_unfill = 0
        self._short_unfill = 0
        self._pl = 0.0
        self._contract_multiple = contract_multiple
        self._result = {
            'date': [],
            'pl': [],
            'long_position': [],
            'short_position': [],
            'long_unfill': [],
            'short_unfill': []
        }

    def order(self, order_id, direction, price, qty):
        self._orders[order_id] = Order(direction, qty)
        if self._orders[order_id]._direction == "Buy":
            self._long_unfill = self._long_unfill + qty
        else:
            self._short_unfill = self._short_unfill + qty

    def trade(self, order_id, price, qty):
        assert (order_id in self._orders)
        order = self._orders[order_id]
        order.trade(qty)
        close_positions = self._long_position if order._direction == "Sell" else self._short_position
        open_positions = self._long_position if order._direction == "Buy" else self._short_position

        leaves_qty = qty
        while leaves_qty > 0 and len(close_positions) > 0:
            leaves_qty = close_positions[0].close(price, leaves_qty, self.update_pl)
            if close_positions[0]._leaves_qty == 0:
                del close_positions[0]
            else:
                assert close_positions[0]._leaves_qty > 0, close_positions[
                    0]._leaves_qty

        if leaves_qty > 0:
            open_positions.append(PositionDetail(order._direction, price, leaves_qty))

        if order._direction == "Buy":
            self._long_unfill = self._long_unfill - qty
            assert (self._long_unfill >= 0)
        else:
            self._short_unfill = self._short_unfill - qty
            assert (self._short_unfill >= 0)

    def cancel(self, order_id):
        assert (order_id in self._orders)
        leaves_qty = self._orders[order_id]._leaves_qty
        if self._orders[order_id]._direction == "Buy":
            self._long_unfill = self._long_unfill - leaves_qty
            assert (self._long_unfill >= 0)
        else:
            self._short_unfill = self._short_unfill - leaves_qty
            assert (self._short_unfill >= 0)

    def settle(self, timestamp):
        self._result['date'].append(
            datetime.datetime.utcfromtimestamp(timestamp / 1000.0))
        self._result['pl'].append(self._pl * self._contract_multiple)
        self._result['long_position'].append(
            sum([pos._leaves_qty for pos in self._long_position]))
        self._result['short_position'].append(
            sum([pos._leaves_qty for pos in self._short_position]))
        self._result['long_unfill'].append(self._long_unfill)
        self._result['short_unfill'].append(self._short_unfill)

    def to_df(self):
        return pd.DataFrame(self._result)

    def update_pl(self, pl):
        self._pl = self._pl + pl


def read_file(path, events):
    with open(path, 'r') as f:
        datas = json.load(f)
        for order in datas['datas']:
            if order['type'] == 'limit_order':
                events.append(
                    LimitOrder(order['timestamp'], order['order_id'],
                               order['direction'], float(order['price']),
                               int(order['qty'])))
            elif order['type'] == 'cancel_order':
                events.append(
                    CancelOrder(order['timestamp'], order['order_id']))
            elif order['type'] == 'trade_order':
                events.append(
                    TradeOrder(order['timestamp'], order['order_id'],
                               float(order['price']), int(order['qty'])))
            else:
                assert (False)

def summary(path):
    dfs = []
    product_infos = {}
    with open('product_info.json', 'r') as f:
        product_infos = json.load(f)
    for f in os.listdir(path):
        events = []
        read_file(os.path.join(path, f), events)
        instrument = os.path.splitext(f)[0]
        product_code = instrument.strip('0123456789')
        assert product_code in product_infos, product_code
        account = FutureAccount(product_infos[product_code]['contract_multiple'])
        datetime_from = datetime.date(2016, 12, 5)
        datetime_to = datetime.date(2017, 11, 23)
        for i in range((datetime_to - datetime_from).days + 1):
            day = datetime_from + datetime.timedelta(days=i)
            dt = datetime.datetime.combine(day, datetime.time(15, 0, 0))
            dt = dt + datetime.timedelta(hours=8) # utc
            ts = time.mktime(dt.timetuple())
            bisect.insort_right(events, DailySettle(int(ts * 1000)))

        assert (all(
            events[i] <= events[i + 1] for i in range(len(events) - 1)))
        for event in events:
            event.do(account)

        df = account.to_df()
        df['instrument'] = instrument
        dfs.append(df)
    return pd.concat(dfs)

def main():
    path = sys.argv[1]
    if not os.path.exists(path):
        return 1
    df = summary(path)
    df.to_csv('daily_pl.csv')
    df.groupby('instrument').last().to_csv('pl.csv')


if __name__ == '__main__':
    sys.exit(main())
