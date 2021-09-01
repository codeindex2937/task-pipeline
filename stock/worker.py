from ..pipeline.parallel import SimpleWorker
from ..pipeline.service import ServiceManager
from datetime import timedelta, datetime, date as date_type
from io import StringIO
import re
import os
import json
import time
import math
import cmath
import numpy
import pandas
import requests
import itertools
import unicodedata
from scipy import stats


class ConsoleWriter(SimpleWorker):
	def process(self, data):
		print(data)

class Store:
	def __init__(self):
		self.history = []

	def buy(self, date, count, value):
		self.history.append((date, count, value))

	def sell(self, date, count, value):
		self.history.append((date, -count, -value))

	def remain_count(self):
		return sum([h[1] for h in self.history])

	def profit(self, price):
		remain_value = sum([h[2] for h in self.history])
		return self.remain_count() / 1000 * price - remain_value

	def accum(self, until):
		date = self.history[0][0]
		accum_result = list()
		accum_value = 0
		current_date = 0
		for date, count, value in self.history:
			if date > until:
				break
			if date != current_date:
				accum_result.append((current_date, accum_value))
				current_date = date
			accum_value += value
		accum_result.append((current_date, accum_value))
		return accum_result[1:]

def parseInt(s):
	try:
		return int(s.replace(',', ''))
	except:
		return None

def parseFloat(s):
	try:
		if type(s) == float:
			if numpy.isnan(s):
				return None
			else:
				return s
		if s.endswith('%'):
			return float(s.replace('%', '').replace(',', ''))
		else:
			return float(s.replace(',', ''))
	except:
		return None

def parseFloat2(s):
	try:
		if type(s) == float:
			if numpy.isnan(s):
				print(1, s)
				return None
			else:
				return s
		if s == '---' or s == '除息' or s == '除權' or s == '除權息':
			return 0.0
		if s[0] == '+':
			return float(s[1:].replace(',', ''))
		else:
			return float(s.replace(',', ''))
	except Exception as e:
		print(e)
		return None

def parseTaiwanDate(s, delimiter='/'):
	try:
		taiwan_year, month, m_day = [int(segment) for segmant in delimiter.split(s)]
		return datetime(taiwan_year + 1911, month, m_day)
	except:
		return datetime(2008, 1, 1)

def convertUD(f):
	if f is None or type(f) == str:
		return ''
	elif f > 0:
		return '+'
	elif f < 0:
		return '-'
	else:
		return ''

def ensureInt(text):
	return int(text.replace(',', ''))

def parseTrades(source, db, total_store = None):
	stock_list = db.list_stock_with_groups()
	stock_name_map = {stock['stock_name']: stock['stock_id'] for stock in stock_list}
	store_map = {}
	total_store = Store()

	with open(source, 'r', encoding='utf8') as f:
		lines = f.read().splitlines()
		for line in lines:
			if not line or line.startswith('//'):
				continue
			trade = line.split(' ')
			stock_name = unicodedata.normalize('NFKC', trade[2])

			rename_list = [
				('創見資', '創見'),
				('東鋼', '東和鋼鐵'),
				('亞德ＫＹ', '亞德客-ＫＹ'),
				('友達光電', '友達'),
				('友達光電', '友達'),
				('群光電子', '群光'),
				('全新光', '全新'),
				('台灣50', '元大台灣50'),
				('京元電', '京元電子'),
				('慧洋KY', '慧洋-KY'),
			]
			for raw, fixed in rename_list:
				if stock_name == unicodedata.normalize('NFKC', raw):
					stock_name = unicodedata.normalize('NFKC', fixed)
					break

			if stock_name not in stock_name_map:
				print('unknown stock', stock_name)
				continue

			date = trade[0].replace('/', '-')
			action = trade[1]
			count = ensureInt(trade[3])
			stock_id = stock_name_map[stock_name]
			store_map.setdefault(stock_id, Store())
			store = store_map[stock_id]

			if action == '普買' or action == '櫃買':
				total_store.buy(date, count, parseInt(trade[7]))
				store.buy(date, count, ensureInt(trade[7]))
			elif action == '普賣' or action == '櫃賣':
				total_store.sell(date, count, parseInt(trade[8]))
				store.sell(date, count, ensureInt(trade[8]))
			else:
				print('unknown action', action)
				continue
	return store_map, total_store

class DateGenerator(SimpleWorker):
	def process(self, item):
		last_record, end_date, date_step = self.config

		while last_record <= end_date:
			if last_record.weekday() < 5:
				self.output(last_record.replace())
			last_record += date_step

class SingleStockFetcher(SimpleWorker):
	def __init__(self, pipe, config):
		super(SingleStockFetcher, self).__init__(pipe, config)
		self.tui = ServiceManager.get('message')
		self.db = ServiceManager.get(self.config["db"])

	def process(self, date):
		datestr = date.strftime('%Y%m%d')
		df = pandas.DataFrame()
		self.tui.progress('fetching trade at %s' % (datestr))
		r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&type=ALL&date=' + datestr)
		self.tui.done()
		if not r.text:
			print('no data')
		else:
			try:
				self.tui.progress('transforming')
				rows = [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if not i.startswith('="') and i.count('",') == 16]
				df = pandas.read_csv(StringIO("\n".join(rows)), header=0)
				self.tui.done()

				self.tui.progress('insert stock')
				self.db.insert_stock([{'id': row[1], 'name': row[2], 'level_id': 1} for row in df.itertuples()])
				self.tui.done()

				self.tui.progress('insert trade')
				self.db.insert_trade([{
						'stock_id': row[1],
						'date': date,
						'share_amount': parseInt(row[3]),
						'transaction_amount': parseInt(row[4]),
						'turnover': parseInt(row[5]),
						'open_price': parseFloat(row[6]),
						'highest_price': parseFloat(row[7]),
						'lowest_price': parseFloat(row[8]),
						'close_price': parseFloat(row[9]),
						'ud': '' if type(row[10]) == float and numpy.isnan(row[10]) else row[10],
						'ud_amount': parseFloat(row[11]),
						'last_purchase_price': parseFloat(row[12]),
						'last_purchase_amount': parseInt(row[13]),
						'last_sell_price': parseFloat(row[14]),
						'last_sell_amount': parseInt(row[15]),
						'pe_ratio': parseFloat(row[16]),
					} for row in df.itertuples()])

			except Exception as e:
				print(e)
				df.to_csv('%s.csv' % (datestr))
				# with open('%s.csv' % (datestr), 'w') as f:
				# 	f.write(r.text)
				return

		for i in range(self.config["fetch_interval"]):
			self.tui.progress('sleeping', i, self.config["fetch_interval"])
			time.sleep(1)

class CounterSingleStockFetcher(SimpleWorker):
	def __init__(self, pipe, config):
		super(CounterSingleStockFetcher, self).__init__(pipe, config)
		self.tui = ServiceManager.get('message')
		self.db = ServiceManager.get(self.config["db"])

	def process(self, date):
		datestr = '/'.join('%02d' % (s,) for s in [date.year - 1911, date.month, date.day])
		df = pandas.DataFrame()
		self.tui.progress('fetching counter trade at %s' % (datestr))
		stock_re = re.compile(r'"\d{4}"')
		r = requests.post('http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&s=0,asc,0&d=' + datestr)
		self.tui.done()
		if not r.text:
			print('no data')
		else:
			try:
				self.tui.progress('transforming')
				if date > datetime.combine(date_type(2020, 4, 28), datetime.min.time()):
					rows = [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if stock_re.match(i) and i.count('",') == 18]
					last_sell_price_index = 14
				else:
					rows = [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if stock_re.match(i) and i.count('",') == 16]
					last_sell_price_index = 13

				if not rows:
					with open('{}.csv'.format(date.strftime('%Y%m%d')), 'w', encoding='utf-8') as f:
						f.write(r.text)
				else:
					df = pandas.read_csv(StringIO("\n".join(rows)), header=0)
					self.tui.done()

					self.tui.progress('insert stock')
					self.db.insert_stock([{'id': row[1], 'name': row[2], 'level_id': 2} for row in df.itertuples()])
					self.tui.done()

					self.tui.progress('insert trade')
					self.db.insert_trade([{
								'stock_id': row[1],
								'date': date,
								'share_amount': parseInt(row[9]),
								'transaction_amount': parseInt(row[11]),
								'turnover': parseInt(row[10]),
								'open_price': parseFloat(row[5]),
								'highest_price': parseFloat(row[6]),
								'lowest_price': parseFloat(row[7]),
								'close_price': parseFloat(row[3]),
								'ud': convertUD(parseFloat(row[4])),
								'ud_amount': parseFloat2(row[4]),
								'last_purchase_price': parseFloat(row[12]),
								'last_purchase_amount': 1,
								'last_sell_price': parseFloat(row[last_sell_price_index]),
								'last_sell_amount': 1,
								'pe_ratio': 0,
							} for row in df.itertuples()])

			except Exception as e:
				print(e)
				df.to_csv('%s.csv' % (datestr))
				# with open('%s.csv' % (datestr), 'w') as f:
				# 	f.write(r.text)
				return

		for i in range(self.config["fetch_interval"]):
			self.tui.progress('sleeping', i, self.config["fetch_interval"])
			time.sleep(1)

class CounterSingleForeignFetcher(SimpleWorker):
	def __init__(self, pipe, config):
		super(CounterSingleForeignFetcher, self).__init__(pipe, config)
		self.tui = ServiceManager.get('message')
		self.db = ServiceManager.get(self.config["db"])

	def process(self, date):
		datestr = '/'.join('%02d' % (s,) for s in [date.year - 1911, date.month, date.day])
		stock_re = re.compile(r'"\d+","\d{4}"')
		df = pandas.DataFrame()
		self.tui.progress('fetching counter foreign at %s' % (datestr))
		r = requests.post('https://www.tpex.org.tw/web/stock/3insti/qfii/qfii_result.php?l=zh-tw&s=0,asc,0&o=csv&d=' + datestr)
		self.tui.done()
		if not r.text:
			print('no data')
		else:
			try:
				self.tui.progress('transforming')
				rows = [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if stock_re.match(i) and (i.count('",') == 9)]

				if rows:
					df = pandas.read_csv(StringIO("\n".join(rows)), header=1, na_filter=False)
					self.tui.done()

					if not df.empty:
						self.tui.progress('insert stock')
						self.db.insert_stock([{'id': row[1], 'name': row[2], 'level_id': 2} for row in df.itertuples()])
						self.tui.done()

						self.tui.progress('insert invester')
						self.db.insert_invester([{
							'stock_id': row[2],
							'date': date,
							'total_stock': parseInt(row[4]),
							'valid_remain_for_foreign': parseInt(row[5]),
							'hold_by_foreign': parseInt(row[6]),
							'valid_remain_for_foreign_percent': parseFloat(row[7]),
							'hold_by_foreign_percent': parseFloat(row[8]),
							'hold_by_foreign_percent_max': parseFloat(row[9]),
							'hold_by_china_percent_max': parseFloat(row[9]),
							'update_reason': '',
							'update_date': datetime(2008, 1, 1),
						} for row in df.itertuples()])

			except Exception as e:
				print(e)
				df.to_csv('%s.csv' % (datestr))
				# with open('%s.csv' % (datestr), 'w') as f:
				# 	f.write(r.text)
				return

		for i in range(self.config["fetch_interval"]):
			if not self.running:
				break
			self.tui.progress('sleeping', i, self.config["fetch_interval"])
			time.sleep(1)

class SingleForeignFetcher(SimpleWorker):
	def __init__(self, pipe, config):
		super(SingleForeignFetcher, self).__init__(pipe, config)
		self.tui = ServiceManager.get('message')
		self.db = ServiceManager.get(self.config["db"])

	def process(self, date):
		datestr = date.strftime('%Y%m%d')
		df = pandas.DataFrame()
		self.tui.progress('fetching foreign at %s' % (datestr))
		r = requests.post('http://www.twse.com.tw/fund/MI_QFIIS?response=csv&selectType=ALLBUT0999&date=' + datestr)
		self.tui.done()
		if not r.text:
			print('no data')
		else:
			try:
				self.tui.progress('transforming')
				rows = [i.translate({ord(c): None for c in ' '}) for i in r.text.split('\n') if not i.startswith('="') and (i.count('",') in (11, 12))]

				df = pandas.read_csv(StringIO("\n".join(rows)), header=1, na_filter=False)
				self.tui.done()

				if not df.empty:
					self.tui.progress('insert stock')
					self.db.insert_stock([{'id': row[1], 'name': row[2], 'level_id': 1} for row in df.itertuples()])
					self.tui.done()

					self.tui.progress('insert invester')
					self.db.insert_invester([{
						'stock_id': row[1],
						'date': date,
						'total_stock': parseInt(row[4]),
						'valid_remain_for_foreign': parseInt(row[5]),
						'hold_by_foreign': parseInt(row[6]),
						'valid_remain_for_foreign_percent': parseFloat(row[7]),
						'hold_by_foreign_percent': parseFloat(row[8]),
						'hold_by_foreign_percent_max': parseFloat(row[9]),
						'hold_by_china_percent_max': parseFloat(row[10]) if len(row) == 14 else parseFloat(row[9]),
						'update_reason': row[11] if len(row) == 14 else row[10],
						'update_date': parseTaiwanDate(row[12]) if len(row) == 14 else parseTaiwanDate(row[11]),
					} for row in df.itertuples()])

			except Exception as e:
				print(e)
				df.to_csv('%s.csv' % (datestr))
				# with open('%s.csv' % (datestr), 'w') as f:
				# 	f.write(r.text)
				return

		for i in range(self.config["fetch_interval"]):
			if not self.running:
				break
			self.tui.progress('sleeping', i, self.config["fetch_interval"])
			time.sleep(1)

class StockEnumerator(SimpleWorker):
	def process(self, dummy):
		db = ServiceManager.get(self.config['db'])
		stock_list = db.list_stock_with_groups()
		last_trade_date = max([trade['date'] for trade in db.list_last_trade()])
		since = last_trade_date + timedelta(days=-120)
		date_filter = "date BETWEEN '{}' AND '{}'".format(datetime.strftime(since, '%Y-%m-%d'), datetime.strftime(last_trade_date, '%Y-%m-%d'))

		for stock in stock_list:
			self.output({
				'id': stock['stock_id'],
				'name': stock['stock_name'],
				'group': stock['group'],
				'date_filter': date_filter,
			})

class TradeLoader(SimpleWorker):
	def process(self, data):
		db = ServiceManager.get(self.config['db'])
		message = ServiceManager.get(self.config['message'])

		stock_id = data['id']
		date_filter = data['date_filter']
		message.progress(stock_id)
		trades = db.list_trade({'stock_id': stock_id}, extra=date_filter)

		if trades:
			investers = db.list_invester({'stock_id': stock_id}, date_filter)
			valid_trades = [{
					'date': trade['date'],
					'open_price': trade['open_price'],
					'close_price': trade['close_price'],
					'lowest_price': trade['lowest_price'],
					'highest_price': trade['highest_price'],
				} for trade in trades if trade['close_price'] is not None]

			if valid_trades:
				self.output({
					'id': stock_id,
					'name': data['name'],
					'group': data['group'],
					'trades': valid_trades,
					'investers': investers,
				})
		message.done()

def multiMovingAverage(items, periods):
	max_period = max(periods)
	period_count = len(periods)
	cumsum = [0]
	moving_aves = [[] for _ in range(period_count)]
	for i, x in enumerate(items, 1):
		cumsum.append(cumsum[i-1] + x)
		if i>=max_period:
			for j, period in enumerate(periods):
				moving_ave = (cumsum[i] - cumsum[i-period])/period
				moving_aves[j].append(moving_ave)
	return moving_aves

class TrendTagUpdater(SimpleWorker):
	def process(self, data):
		db = ServiceManager.get(self.config['db'])
		message = ServiceManager.get(self.config['message'])

		stock_id = data['id']
		date_filter = data['date_filter']
		message.progress(stock_id)
		trades = db.list_trade({'stock_id': stock_id}, extra=date_filter)
		if not trades:
			return

		dates = [datetime.timestamp(datetime.combine(trade['date'], datetime.min.time())) for trade in trades if trade['close_price'] is not None]
		prices = [trade['close_price'] for trade in trades if trade['close_price'] is not None]
		p1, p3 = multiMovingAverage(prices, [7, 21])

		fit_length = 7
		date_offset_diff = len(prices) - fit_length
		p1_offset = len(p1) - fit_length
		prices_offset = len(prices) - fit_length
		p1a, p1b = numpy.ma.polyfit([dates[-1] - d for d in dates[date_offset_diff:]], [p1[i + p1_offset] - prices[i + prices_offset] for i in range(fit_length)], 1)
		
		tags = ['n4', 'n2', 'p0', 'p2', 'p4', 'p6', 'p8', 'p10']
		tag = 'p0'
		if p3[-1] < p1[-1]:
			if p1b <= 0:
				if p1a <= 0:
					tag = 'p4'
				else:
					tag = 'p6'
			else:
				if p1a <= 0:
					tag = 'p8'
				else:
					tag = 'p10'
		else:
			if p1b <= 0:
				if p1a <= 0:
					tag = 'p2'
				else:
					tag = 'p0'
			else:
				if p1a <= 0:
					tag = 'n2'
				else:
					tag = 'n4'

		db.update_exclusive_tag(stock_id, tag, tags)
		message.done()

class RecordTagUpdater(SimpleWorker):
	def on_start(self):
		db = ServiceManager.get(self.config['db'])
		self.trades_map, _ = parseTrades('stock/trades.txt', db)

	def process(self, data):
		stock_id = data['id']
		db = ServiceManager.get(self.config['db'])

		if stock_id in self.trades_map:
			db.set_tag(stock_id, 'buyed')

class AppCacheWriter(SimpleWorker):
	def on_start(self):
		db = ServiceManager.get(self.config['db'])
		self.trades_map, _ = parseTrades('stock/trades.txt', db)

	def process(self, data):
		stock_id = data['id']
		trades = data['trades']
		investers = data['investers']

		transactions = []
		if stock_id in self.trades_map:
			transactions.extend(self.trades_map[stock_id].history)

		cache_path = os.path.join('stock/cache', stock_id)
		last_date = max([trade['date'] for trade in trades])
		with open(cache_path, 'w') as f:
			json.dump({
				'date': datetime.strftime(last_date, '%Y-%m-%d'),
				'name': data['name'],
				'group': data['group'],
				'data': {
					'date': [datetime.strftime(trade['date'], '%Y-%m-%d') for trade in trades],
					'open': [trade['open_price'] for trade in trades],
					'close': [trade['close_price'] for trade in trades],
					'low': [trade['lowest_price'] for trade in trades],
					'high': [trade['highest_price'] for trade in trades],
				},
				'trades': transactions,
				'invester': {
					'date': [datetime.strftime(info['date'], '%Y-%m-%d') for info in investers],
					'percent': [info['hold_by_foreign_percent'] for info in investers],
				},
			}, f)

class AmountFilter(SimpleWorker):
	def process(self, v):
		db = ServiceManager.get(self.config["db"])
		k = v['id']
		now = datetime.today()

		r = db.list_trade({'stock_id': k})
		trade_list = [trade for trade in r if trade['close_price'] is not None]

		if len(trade_list) < 30:
			return

		if (now - datetime.combine(trade_list[-1]['date'], datetime.min.time())).total_seconds() > 30 * 24 * 60 * 60:
			return

		x = [datetime.timestamp(datetime.combine(trade['date'], datetime.min.time())) for trade in trade_list]
		y = [trade['close_price'] for trade in trade_list]
		la, lb = numpy.ma.polyfit(x, y, 1)
		# if la <= 0:
		# 	return

		history = db.list_invester({'stock_id': k})
		if not history:
			foreign_coef = 0.0
		else:
			foreign = [0] * len(x)
			date_map = {trade['date']: idx for idx, trade in enumerate(trade_list)}
			for i in history:
				if i['date'] in date_map:
					foreign[date_map[i['date']]] = i['hold_by_foreign_percent']

			foreign_coef, p_value = stats.pearsonr(y, foreign)
			foreign_coef *= numpy.mean(foreign)

		last_higher = datetime.fromtimestamp(0)

		print(v['id'])
		db.insert_result({
			'id': v['id'],
			'name': v['name'],
			'linear_gradient': la,
			'linear_base': lb,
			'last_higher': last_higher.date(),
			'foreign_coef': foreign_coef,
		})


class PairGenerator(SimpleWorker):
	def process(self, trade_map):
		stock_list = list(trade_map.items())
		for pair in itertools.combinations(stock_list, 2):
			if pair[0][0] != pair[1][0]:
				self.output([pair[0], pair[1]])


class PhaseCorrelation(SimpleWorker):
	@staticmethod
	def error(i):
		r, phi = cmath.polar(i)
		return (1 + abs(cmath.log(r))) * abs(phi / math.pi)

	@staticmethod
	def anti_noise_fft(y):
		scale = max(y, key=abs)
		return [v if abs(v) > 1e-4 else 0 for v in numpy.fft.fft([v / scale for v in y])]

	@staticmethod
	def min_distance(v1, v2, resolution=16):
		common_range = min(len(v1), len(v2))
		f1 = PhaseCorrelation.anti_noise_fft(v1[-common_range:])
		f2 = PhaseCorrelation.anti_noise_fft(v2[-common_range:])

		total_scalar = 0
		vector = []
		total_data = len(f1)
		for i in range(total_data):
			if f1[i] == 0 and f2[i] == 0:
				continue
			elif f1[i] == 0:
				total_scalar += abs(f2[i])
			elif f2[i] == 0:
				total_scalar += abs(f1[i])
			else:
				vector.append(f1[i] / f2[i])

		return (min(
			sum(
				map(
					PhaseCorrelation.error,
					[i * cmath.rect(1., math.pi * (d / resolution - 0.5)) for i in vector]
				)
			) for d in range(resolution)
		) + total_scalar) / total_data

	def process(self, pair):
		db = ServiceManager.get(self.config["db"])
		relation_value = PhaseCorrelation.min_distance(pair[0][1], pair[1][1])

		db.insert_relation({
			'stock_id1': pair[0][0],
			'stock_id2': pair[1][0],
			'coefficient': relation_value
		})
		self.output((pair[0][0], pair[1][0], relation_value))
