from ..pipeline.service import ServiceWorker
from ..utils.db import OrmDatabase
from ..utils.tui import TextUserInterface
import sys
import traceback
from sqlalchemy import func, select, or_

class DB(OrmDatabase):
	def __init__(self):
		super(DB, self).__init__({
		'stock': {
			'columns': [
				{ 'name': 'id',       'type': 'TEXT',    'attr': { 'primary_key': True, 'unique': True} },
				{ 'name': 'name',     'type': 'TEXT',    'attr': {} },
				{ 'name': 'level_id', 'type': 'INTEGER', 'attr': {} },
			]
		},
		'trade': {
			'index': [
				{ 'name': 'last_close_price', 'columns': ('date', 'close_price')},
				{ 'name': 'trade_daterange',  'columns': ('date', 'stock_id')},
			],
			'columns': [
				{ 'name': 'stock_id',             'type': 'TEXT',     'attr': { 'primary_key': True } },
				{ 'name': 'date',                 'type': 'DATE',     'attr': { 'primary_key': True } },
				{ 'name': 'share_amount',         'type': 'INTEGER',  'attr': {} },
				{ 'name': 'transaction_amount',   'type': 'INTEGER',  'attr': {} },
				{ 'name': 'turnover',             'type': 'INTEGER',  'attr': {} },
				{ 'name': 'open_price',           'type': 'REAL',     'attr': { 'nullable': True } },
				{ 'name': 'close_price',          'type': 'REAL',     'attr': { 'nullable': True } },
				{ 'name': 'lowest_price',         'type': 'REAL',     'attr': { 'nullable': True } },
				{ 'name': 'highest_price',        'type': 'REAL',     'attr': { 'nullable': True } },
				{ 'name': 'ud',                   'type': 'TEXT',     'attr': {} },
				{ 'name': 'ud_amount',            'type': 'REAL',     'attr': {} },
				{ 'name': 'last_purchase_price',  'type': 'REAL',     'attr': { 'nullable': True } },
				{ 'name': 'last_purchase_amount', 'type': 'INT',      'attr': { 'nullable': True } },
				{ 'name': 'last_sell_price',      'type': 'REAL',     'attr': { 'nullable': True } },
				{ 'name': 'last_sell_amount',     'type': 'TEXT',     'attr': { 'nullable': True } },
				{ 'name': 'pe_ratio',             'type': 'REAL',     'attr': { 'nullable': True } },
			]
		},
		'invester': {
			'index': [
				{ 'name': 'hold_daterange',     'columns': ('date', 'hold_by_foreign_percent')},
				{ 'name': 'invester_daterange', 'columns': ('date', 'stock_id')},
			],
			'columns': [
				{ 'name': 'stock_id', 'type': 'TEXT', 'attr': { 'primary_key': True } },
				{ 'name': 'date', 'type': 'DATE', 'attr': { 'primary_key': True } },
				{ 'name': 'total_stock', 'type': 'INT', 'attr': {} },
				{ 'name': 'valid_remain_for_foreign', 'type': 'INT', 'attr': {} },
				{ 'name': 'hold_by_foreign', 'type': 'INT', 'attr': {} },
				{ 'name': 'valid_remain_for_foreign_percent', 'type': 'FLOAT', 'attr': {} },
				{ 'name': 'hold_by_foreign_percent', 'type': 'FLOAT', 'attr': {} },
				{ 'name': 'hold_by_foreign_percent_max', 'type': 'FLOAT', 'attr': {} },
				{ 'name': 'hold_by_china_percent_max', 'type': 'FLOAT', 'attr': {} },
				{ 'name': 'update_reason', 'type': 'TEXT', 'attr': {} },
				{ 'name': 'update_date', 'type': 'DATE', 'attr': {} },
			]
		},
	})

	def insert_stock(self, attr):
		return self.insert('stock', attr)

	def insert_trade(self, attr):
		return self.insert('trade', attr)

	def insert_invester(self, attr):
		return self.insert('invester', attr)

	def get_stock(self, attr):
		return self.fetchone('stock', attr)

	def get_trade(self, attr):
		return self.fetchone('trade', attr)

	def list_stock(self, attr=dict()):
		return self.fetchall('stock', attr)

	def list_fetched_stock(self, attr=dict()):
		q = self.session.query(self.record.trade.stock_id, self.record.stock.name).distinct().outerjoin(self.record.stock, self.record.stock.id==self.record.trade.stock_id)
		return [{'id': r[0], 'name': r[1]} for r in q.all()]

	def list_trade(self, attr=dict(), extra=None):
		return self.fetchall('trade', attr, order_by=self.record.trade.date.asc(), extra=extra)

	def list_invester(self, attr=dict(), extra=None):
		return self.fetchall('invester', attr, order_by=self.record.invester.date.asc(), extra=extra)

	def get_trade_max_date(self, attr=dict()):
		if 'stock_id' in attr:
			sql = 'SELECT coalesce(MAX(date), date("2021-01-01")) FROM trade WHERE stock_id="{}"'.format(attr['stock_id'])
		elif 'stock_level' in attr:
			sql = 'SELECT coalesce(MAX(date), date("2021-01-01")) FROM (select id from stock where level_id={}) AS S LEFT JOIN (select stock_id,date from trade group by stock_id having date=MAX(date)) AS T ON S.id=T.stock_id'.format(attr['stock_level'])
		else:
			sql = 'SELECT coalesce(MAX(date), date("2021-01-01")) FROM trade'

		cursor = self.conn.execute(sql)
		return cursor.fetchone()[0]

	def get_foreign_max_date(self, attr=dict()):
		if attr:
			where = 'WHERE ' + ' AND '.join(['{}={}'.format(k, v) for k, v in attr.items()])
		else:
			where = ''
		cursor = self.conn.execute('SELECT coalesce(MAX(date), date("2021-01-01")) FROM invester ' + where)
		return cursor.fetchone()[0]

	def list_last_trade(self, attr=dict()):
		cls = self.__class__
		stmt = self.append_where(cls.table_dict['trade'].columns, cls.table_dict['trade'].select(), attr, fields=['stock_id'], additional_where = 'date = (SELECT MAX(date) FROM trade)')
		result = self.session.execute(stmt)
		return result.fetchall()

	def list_trade_on(self, date):
		cls = self.__class__
		stmt = self.append_where(cls.table_dict['trade'].columns, cls.table_dict['trade'].select(), {'date': date}, fields=['stock_id', 'date'])
		result = self.session.execute(stmt)
		return result.fetchall()

	def list_trade_count(self, attr=dict()):
		cls = self.__class__
		col_stock_id = cls.table_dict['trade'].columns.stock_id
		stmt = select([col_stock_id, func.count()]).select_from(cls.table_dict['trade']).group_by(col_stock_id)
		result = self.session.execute(stmt)
		return result.fetchall()

class AnalyzeDB(OrmDatabase):
	def __init__(self):
		super(AnalyzeDB, self).__init__({
		'tag': {
			'columns': [
				{ 'name': 'tag_id', 'type': 'INTEGER', 'attr': { 'primary_key': True } },
				{ 'name': 'tag', 'type': 'TEXT', 'attr': {} },
				{ 'name': 'parent_tag', 'type': 'INTEGER', 'attr': {} },
			]
		},
		'stock_tag': {
			'columns': [
				{ 'name': 'stock_id', 'type': 'TEXT', 'attr': { 'primary_key': True } },
				{ 'name': 'tag_id', 'type': 'INTEGER', 'attr': { 'primary_key': True } },
			]
		},
	})

	def list_by_tag(self, *ids):
		filters = or_(*[self.record.stock_tag.tag_id==i for i in ids])

		subq = self.session.query(self.record.stock_tag.stock_id) \
			.filter(filters).subquery()
		q = self.session.query(self.record.stock_tag.stock_id, self.record.tag.tag, self.record.tag.parent_tag) \
			.join(self.record.tag, self.record.tag.tag_id==self.record.stock_tag.tag_id) \
			.filter(self.record.stock_tag.stock_id.in_(subq))

		result = {}
		for r in q.all():
			result.setdefault(r[0], {
				'stock_id': r[0],
				'tags': set()
			})
			result[r[0]]['tags'].add((r[1],r[2]))
		return [{
				'stock_id': v['stock_id'],
				'tags': [t[0] for t in sorted(v['tags'], key=lambda t: 0 if t[1] is None else t[1], reverse=True)]
			} for v in result.values()]

	def set_tag(self, stock_id, symbol):
		subq = self.session.query(stock_id, self.record.tag.tag_id).filter(self.record.tag.tag==symbol).subquery()
		ins = self.table_dict['stock_tag'].insert(prefixes=['OR IGNORE']).from_select(
			[self.record.stock_tag.stock_id, self.record.stock_tag.tag_id],
			subq
		)
		self.session.execute(ins)
		self.session.commit()

	def reset_tag(self, stock_id, symbol):
		subq = self.session.query(self.record.tag.tag_id).filter(self.record.tag.tag==symbol).subquery()
		ins = self.table_dict['stock_tag'].delete().where(
			(self.record.stock_tag.stock_id==stock_id) & (self.record.stock_tag.tag_id==subq)
		)
		self.session.execute(ins)
		self.session.commit()

	def list_tag(self):
		parent_filter = or_((self.record.tag.parent_tag==1), (self.record.tag.parent_tag==2), (self.record.tag.parent_tag==None))
		q = self.session.query(self.record.tag.tag_id, self.record.tag.tag, self.record.tag.parent_tag) \
			.filter(parent_filter) \
			.filter((self.record.tag.tag != '概念股') & (self.record.tag.tag != '上市') & (self.record.tag.tag != '上櫃') & (self.record.tag.tag != '電子產業'))

		return [{
			'tag_id': r[0],
			'tag': r[1],
			'parent_tag': r[2],
		} for r in q.all()]

	def list_tag_of_stock(self, stock_id):
		q = self.session.query(self.record.tag.tag).select_from(self.table_dict['tag']) \
			.join(self.record.stock_tag, self.record.stock_tag.tag_id==self.record.tag.tag_id) \
			.filter(self.record.stock_tag.stock_id==stock_id)
		return [r[0] for r in q.all()]

	def update_stock_tag(self, stock_id, tag):
		tags = self.session.query(self.record.tag).filter(or_(*[self.record.tag.tag==symbol for symbol in ('rising', 'jitter', 'falling')])).all()
		new_tag = list(filter(lambda r: r.tag==tag, tags))[0]

		filters = or_(*[self.record.stock_tag.tag_id==tag.tag_id for tag in tags])
		q = self.session.query(self.record.stock_tag)
		q.filter_by(stock_id=stock_id).filter(filters).delete()

		ins = self.table_dict['stock_tag'].insert().values(stock_id=stock_id, tag_id=new_tag.tag_id)
		self.session.execute(ins)
		self.session.commit()

	def update_exclusive_tag(self, stock_id, tag, exclusive_tags, parent=None):
		req = self.session.query(self.record.tag)
		
		if parent is not None:
			subq = self.session.query(self.record.tag.tag_id).filter(self.record.tag.tag==parent).subquery()
			req = req.filter(self.record.tag.parent_tag==subq)

		tags = req.filter(or_(*[self.record.tag.tag==symbol for symbol in exclusive_tags])).all()
		new_tag = list(filter(lambda r: r.tag==tag, tags))[0]

		filters = or_(*[self.record.stock_tag.tag_id==tag.tag_id for tag in tags])
		q = self.session.query(self.record.stock_tag)
		q.filter_by(stock_id=stock_id).filter(filters).delete()

		ins = self.table_dict['stock_tag'].insert().values(stock_id=stock_id, tag_id=new_tag.tag_id)
		self.session.execute(ins)
		self.session.commit()

class DbService(ServiceWorker):
	def __init__(self, config, pipe_out, pipe_in):
		super(DbService, self).__init__(config, pipe_out, pipe_in)

	def on_start(self):
		self.db = DB()
		self.analyze_db = AnalyzeDB()
		self.db.connect(self.config['db_path'])
		self.analyze_db.connect(self.config['analyze_db_path'])

	def on_abort(self):
		self.db.conn.close()

	def process(self, data):
		try:
			if hasattr(self.db, data[0]):
				return getattr(self.db, data[0])(*data[1:])
			elif hasattr(self.analyze_db, data[0]):
				return getattr(self.analyze_db, data[0])(*data[1:])
			else:
				fn, sql, params = data
				if fn == 'insert':
					self.db.insert(sql, params)
				elif fn == 'delete':
					self.db.delete(sql, params)
				elif fn == 'many':
					self.db.executemany(sql, params)
				else:
					self.db.execute(sql, params)
		except Exception as e:
			print('DbService:', e)
			traceback.print_exc(file=sys.stdout)

	class Port(ServiceWorker.Port):
		def get_trade_max_date(self, attr={}):
			return self.request(['get_trade_max_date', attr], no_wait=False)
		def get_foreign_max_date(self, attr={}):
			return self.request(['get_foreign_max_date', attr], no_wait=False)
		def insert_trade(self, attr):
			return self.request(['insert_trade', attr], no_wait=False)
		def insert_invester(self, attr):
			return self.request(['insert_invester', attr], no_wait=False)
		def insert_stock(self, attr):
			return self.request(['insert_stock', attr], no_wait=False)
		def list_stock(self, attr=dict()):
			return self.request(['list_stock', attr], no_wait=False)
		def list_last_trade(self, attr=dict()):
			return self.request(['list_last_trade', attr], no_wait=False)
		def list_trade(self, attr=dict(), extra=None):
			return self.request(['list_trade', attr, extra], no_wait=False)
		def get_stock(self, attr):
			return self.request(['get_stock', attr], no_wait=False)
		def insert_relation(self, attr):
			return self.request(['insert_relation', attr], no_wait=False)
		def list_invester(self, attr, extra=None):
			return self.request(['list_invester', attr, extra], no_wait=False)
		def update_exclusive_tag(self, stock_id, tag, exclusive_tags):
			return self.request(['update_exclusive_tag', stock_id, tag, exclusive_tags], no_wait=False)
		def set_tag(self, stock_id, tag):
			return self.request(['set_tag', stock_id, tag], no_wait=False)

class MessageService(ServiceWorker):
	def __init__(self, config, pipe_out, pipe_in):
		super(MessageService, self).__init__(config, pipe_out, pipe_in)
		self.tui = TextUserInterface()

	def on_start(self):
		pass

	def on_abort(self):
		pass

	def process(self, data):
		if hasattr(self.tui, data[0]):
			getattr(self.tui, data[0])(*data[1:])
		else:
			print(data)

	class Port(ServiceWorker.Port):
		def progress(self, title, current=0, total=1):
			return self.request(['progress', title, current, total])
		def done(self):
			return self.request(['done'])
