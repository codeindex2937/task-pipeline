from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Index
from sqlalchemy import Integer, Text, Float, DateTime, Date
from ..utils.db import OrmBaseDatabase

Base = declarative_base()


class Stock(Base):
	__tablename__ = 'stock'
	id = Column(Text, primary_key=True, unique=True)
	name = Column(Text)
	level_id = Column(Integer)

	def __iter__(self):
		for c in self.__table__.c:
			yield getattr(self, c.name)

	def __repr__(self):
		return '%s[%s]' % ('stock', ','.join('%s: %s' % (c.name, getattr(self, c.name)) for c in self.__table__.columns))


class Trade(Base):
	__tablename__ = 'trade'
	stock_id = Column(Text, primary_key=True)
	date = Column(Date, primary_key=True)
	share_amount = Column(Integer)
	transaction_amount = Column(Integer)
	turnover = Column(Integer)
	open_price = Column(Float)
	close_price = Column(Float)
	lowest_price = Column(Float)
	highest_price = Column(Float)
	ud = Column(Text)
	ud_amount = Column(Float)
	last_purchase_price = Column(Float)
	last_purchase_amount = Column(Integer)
	last_sell_price = Column(Float)
	last_sell_amount = Column(Text)
	pe_ratio = Column(Float)

	def __iter__(self):
		for c in self.__table__.c:
			yield getattr(self, c.name)

	def __repr__(self):
		return '%s[%s]' % ('trade', ','.join('%s: %s' % (c.name, getattr(self, c.name)) for c in self.__table__.columns))


class Invester(Base):
	__tablename__ = 'invester'
	stock_id = Column(Text, primary_key=True)
	date = Column(Date, primary_key=True)
	total_stock = Column(Integer)
	valid_remain_for_foreign = Column(Integer)
	hold_by_foreign = Column(Integer)
	valid_remain_for_foreign_percent = Column(Float)
	hold_by_foreign_percent = Column(Float)
	hold_by_foreign_percent_max = Column(Float)
	hold_by_china_percent_max = Column(Float)
	update_reason = Column(Text)
	update_date = Column(Date)

	def __iter__(self):
		for c in self.__table__.c:
			yield getattr(self, c.name)

	def __repr__(self):
		return '%s[%s]' % ('invester', ','.join('%s: %s' % (c.name, getattr(self, c.name)) for c in self.__table__.columns))


class Database(OrmBaseDatabase):
	Stock = Table(
		'stock',
		Base.metadata,
		Stock.id,
		Stock.name,
		Stock.level_id,
		extend_existing=True
	)
	Trade = Table(
		'trade',
		Base.metadata,
		Trade.stock_id,
		Trade.date,
		Trade.share_amount,
		Trade.transaction_amount,
		Trade.turnover,
		Trade.open_price,
		Trade.close_price,
		Trade.lowest_price,
		Trade.highest_price,
		Trade.ud,
		Trade.ud_amount,
		Trade.last_purchase_price,
		Trade.last_purchase_amount,
		Trade.last_sell_price,
		Trade.last_sell_amount,
		Trade.pe_ratio,
		Index('last_close_price', "date", "close_price"),
		Index('trade_daterange', "date", "stock_id"),
		extend_existing=True
	)
	Invester = Table(
		'invester',
		Base.metadata,
		Invester.stock_id,
		Invester.date,
		Invester.total_stock,
		Invester.valid_remain_for_foreign,
		Invester.hold_by_foreign,
		Invester.valid_remain_for_foreign_percent,
		Invester.hold_by_foreign_percent,
		Invester.hold_by_foreign_percent_max,
		Invester.hold_by_china_percent_max,
		Invester.update_reason,
		Invester.update_date,
		Index('hold_daterange', "date", "hold_by_foreign_percent"),
		Index('invester_daterange', "date", "stock_id"),
		extend_existing=True
	)

	def __init__(self):
		super().__init__({
			'stock': Stock.__table__,
			'trade': Trade.__table__,
			'invester': Invester.__table__,
		}, Base)
