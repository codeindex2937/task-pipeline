from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Index
from sqlalchemy import Integer, Text, Float, DateTime, Date
from ..utils.db import OrmBaseDatabase

Base = declarative_base()


class Tag(Base):
	__tablename__ = 'tag'
	tag_id = Column(Integer, primary_key=True)
	tag = Column(Text)
	parent_tag = Column(Integer)

	def __iter__(self):
		for c in self.__table__.c:
			yield getattr(self, c.name)

	def __repr__(self):
		return '%s[%s]' % ('tag', ','.join('%s: %s' % (c.name, getattr(self, c.name)) for c in self.__table__.columns))


class StockTag(Base):
	__tablename__ = 'stock_tag'
	stock_id = Column(Text, primary_key=True)
	tag_id = Column(Integer, primary_key=True)

	def __iter__(self):
		for c in self.__table__.c:
			yield getattr(self, c.name)

	def __repr__(self):
		return '%s[%s]' % ('stock_tag', ','.join('%s: %s' % (c.name, getattr(self, c.name)) for c in self.__table__.columns))


class Database(OrmBaseDatabase):
	Tag = Table(
		'tag',
		Base.metadata,
		Tag.tag_id,
		Tag.tag,
		Tag.parent_tag,
		extend_existing=True
	)
	StockTag = Table(
		'stock_tag',
		Base.metadata,
		StockTag.stock_id,
		StockTag.tag_id,
		extend_existing=True
	)

	def __init__(self):
		super().__init__({
			'tag': Tag.__table__,
			'stock_tag': StockTag.__table__,
		}, Base)
