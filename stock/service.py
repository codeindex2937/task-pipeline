from ..pipeline.service import ServiceWorker
from ..utils.tui import TextUserInterface
from . import stock_db
from . import analyze_db

import sys
import traceback
import math
from sqlalchemy import func, select, or_

class DB(stock_db.Database):
	def insert_stock(self, attr):
		return self.insert('stock', attr)

	def insert_trade(self, attr):
		return self.insert('trade', attr)

	def get_stock(self, attr):
		return self.fetchone('stock', attr)

	def get_trade(self, attr):
		return self.fetchone('trade', attr)

	def list_stock(self, attr=dict()):
		return self.fetchall('stock', attr)

	def list_fetched_stock(self, attr=dict()):
		q = self.session.query(stock_db.Trade.stock_id, stock_db.Stock.name).distinct().outerjoin(stock_db.Stock, stock_db.Stock.id==stock_db.Trade.stock_id)
		return [{'id': r[0], 'name': r[1]} for r in q.all()]

	def list_trade(self, attr=dict(), extra=None):
		trades = self.fetchall('trade', attr, order_by=stock_db.Trade.date.asc(), extra=extra)
		return [{c.name: t[i] for i, c in enumerate(self.Trade.columns)} for t in trades]

	def get_trade_max_date(self, attr=dict()):
		if 'stock_id' in attr:
			sql = 'SELECT coalesce(MAX(date), date("2021-01-01")) FROM trade WHERE stock_id="{}"'.format(attr['stock_id'])
		elif 'stock_level' in attr:
			sql = 'SELECT coalesce(MAX(date), date("2021-01-01")) FROM (select id from stock where level_id={}) AS S LEFT JOIN (select stock_id,date from trade group by stock_id having date=MAX(date)) AS T ON S.id=T.stock_id'.format(attr['stock_level'])
		else:
			sql = 'SELECT coalesce(MAX(date), date("2021-01-01")) FROM trade'

		cursor = self.conn.execute(sql)
		return cursor.fetchone()[0]

	def list_last_trade(self, attr=dict()):
		stmt = self.append_where(self.Trade.columns, self.Trade.select(), attr, fields=['stock_id'], additional_where = 'date = (SELECT MAX(date) FROM trade)')
		result = self.session.execute(stmt)
		return result.fetchall()

	def list_trade_on(self, date):
		stmt = self.append_where(self.Trade.columns, self.Trade.select(), {'date': date}, fields=['stock_id', 'date'])
		result = self.session.execute(stmt)
		return result.fetchall()

	def list_trade_count(self, attr=dict()):
		col_stock_id = self.Trade.columns.stock_id
		stmt = select([col_stock_id, func.count()]).select_from(self.Trade).group_by(col_stock_id)
		result = self.session.execute(stmt)
		return result.fetchall()

class AnalyzeDB(analyze_db.Database):
	def list_by_tag(self, *ids):
		filters = or_(*[analyze_db.StockTag.tag_id==i for i in ids])

		subq = self.session.query(analyze_db.StockTag.stock_id) \
			.filter(filters).subquery()
		q = self.session.query(analyze_db.StockTag.stock_id, analyze_db.Tag.tag, analyze_db.Tag.parent_tag) \
			.join(analyze_db.Tag, analyze_db.Tag.tag_id==analyze_db.StockTag.tag_id) \
			.filter(analyze_db.StockTag.stock_id.in_(subq))

		result = {}
		for r in q.all():
			result.setdefault(r[0], {
				'stock_id': r[0],
				'tags': set()
			})
			result[r[0]]['tags'].add((r[1],r[2]))
		return [{
				'stock_id': v['stock_id'],
				'tags': [t[0] for t in sorted(v['tags'], key=lambda t: math.inf if t[1] is None else t[1])]
			} for v in result.values()]

	def set_tag(self, stock_id, symbol):
		subq = self.session.query(stock_id, analyze_db.Tag.tag_id).filter(analyze_db.Tag.tag==symbol).subquery()
		ins = self.StockTag.insert(prefixes=['OR IGNORE']).from_select(
			[analyze_db.StockTag.stock_id, analyze_db.StockTag.tag_id],
			subq
		)
		self.session.execute(ins)
		self.session.commit()

	def reset_tag(self, stock_id, symbol):
		subq = self.session.query(analyze_db.Tag.tag_id).filter(analyze_db.Tag.tag==symbol).subquery()
		ins = self.StockTag.delete().where(
			(analyze_db.StockTag.stock_id==stock_id) & (analyze_db.StockTag.tag_id==subq)
		)
		self.session.execute(ins)
		self.session.commit()

	def list_tag(self):
		parent_filter = or_((analyze_db.Tag.parent_tag==1), (analyze_db.Tag.parent_tag==2), (analyze_db.Tag.parent_tag==None))
		q = self.session.query(analyze_db.Tag.tag_id, analyze_db.Tag.tag, analyze_db.Tag.parent_tag) \
			.filter(parent_filter) \
			.filter((analyze_db.Tag.tag != '概念股') & (analyze_db.Tag.tag != '上市') & (analyze_db.Tag.tag != '上櫃') & (analyze_db.Tag.tag != '電子產業'))

		return [{
			'tag_id': r[0],
			'tag': r[1],
			'parent_tag': r[2],
		} for r in q.all()]

	def list_tag_of_stock(self, stock_id):
		q = self.session.query(analyze_db.Tag.tag).select_from(self.table_dict['tag']) \
			.join(analyze_db.StockTag, analyze_db.StockTag.tag_id==analyze_db.Tag.tag_id) \
			.filter(analyze_db.StockTag.stock_id==stock_id)
		return [r[0] for r in q.all()]

	def update_stock_tag(self, stock_id, tag):
		tags = self.session.query(analyze_db.Tag).filter(or_(*[analyze_db.Tag.tag==symbol for symbol in ('rising', 'jitter', 'falling')])).all()
		new_tag = list(filter(lambda r: r.tag==tag, tags))[0]

		filters = or_(*[analyze_db.StockTag.tag_id==tag.tag_id for tag in tags])
		q = self.session.query(analyze_db.StockTag)
		q.filter_by(stock_id=stock_id).filter(filters).delete()

		ins = self.StockTag.insert().values(stock_id=stock_id, tag_id=new_tag.tag_id)
		self.session.execute(ins)
		self.session.commit()

	def update_exclusive_tag(self, stock_id, tag, exclusive_tags, parent=None):
		req = self.session.query(analyze_db.Tag)
		
		if parent is not None:
			subq = self.session.query(analyze_db.Tag.tag_id).filter(analyze_db.Tag.tag==parent).subquery()
			req = req.filter(analyze_db.Tag.parent_tag==subq)

		tags = req.filter(or_(*[analyze_db.Tag.tag==symbol for symbol in exclusive_tags])).all()
		new_tag = list(filter(lambda r: r.tag==tag, tags))[0]

		filters = or_(*[analyze_db.StockTag.tag_id==tag.tag_id for tag in tags])
		q = self.session.query(analyze_db.StockTag)
		q.filter_by(stock_id=stock_id).filter(filters).delete()

		ins = self.StockTag.insert().values(stock_id=stock_id, tag_id=new_tag.tag_id)
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
