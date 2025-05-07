import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, DECIMAL
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv
import datetime

# Load environment variables
load_dotenv()

# 1) Connect to the database with SQLAlchemy
def connect():
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        with engine.connect():
            print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# 2) Create the tables
Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publishers"
    publisher_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)

class Author(Base):
    __tablename__ = "authors"
    author_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(100), nullable=False)
    middle_name = Column(String(50), nullable=True)
    last_name = Column(String(100), nullable=True)

class Book(Base):
    __tablename__ = "books"
    book_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    total_pages = Column(Integer, nullable=True)
    rating = Column(DECIMAL(4, 2), nullable=True)
    isbn = Column(String(13), nullable=True)
    published_date = Column(Date, nullable=True)
    publisher_id = Column(Integer, ForeignKey('publishers.publisher_id'), nullable=True)

class BookAuthor(Base):
    __tablename__ = "book_authors"
    book_id = Column(Integer, ForeignKey('books.book_id', ondelete='CASCADE'), primary_key=True)
    author_id = Column(Integer, ForeignKey('authors.author_id', ondelete='CASCADE'), primary_key=True)

# Create tables if they don't exist already
engine = connect()
Base.metadata.create_all(engine)

# 3) Insert data
Session = sessionmaker(bind=engine)
session = Session()

publishers = [
    Publisher(name="O Reilly Media"),
    Publisher(name="A Book Apart"),
    Publisher(name="A K PETERS"),
    Publisher(name="Academic Press"),
    Publisher(name="Addison Wesley"),
    Publisher(name="Albert&Sweigart"),
    Publisher(name="Albert A. Knopf")
]

# Use add() for pure inserts
for publisher in publishers:
    session.add(publisher)
session.commit()

authors = [
    Author(first_name='Merritt', middle_name=None, last_name='Eric'),
    Author(first_name='Linda', middle_name=None, last_name='Mui'),
    Author(first_name='Alecos', middle_name=None, last_name='Papadatos'),
    Author(first_name='Anthony', middle_name=None, last_name='Molinaro'),
    Author(first_name='David', middle_name=None, last_name='Cronin'),
    Author(first_name='Richard', middle_name=None, last_name='Blum'),
    Author(first_name='Yuval', middle_name='Noah', last_name='Harari'),
    Author(first_name='Paul', middle_name=None, last_name='Albitz')
]

for author in authors:
    session.add(author)
session.commit()

# Use datetime.date() for published_date
books = [
    Book(title='Lean Software Development: An Agile Toolkit', total_pages=240, rating=4.17, isbn='9780320000000', published_date=datetime.date(2003, 5, 18), publisher_id=5),
    Book(title='Facing the Intelligence Explosion', total_pages=91, rating=3.87, isbn=None, published_date=datetime.date(2013, 2, 1), publisher_id=7),
    Book(title='Scala in Action', total_pages=419, rating=3.74, isbn='9781940000000', published_date=datetime.date(2013, 4, 10), publisher_id=1),
    Book(title='Patterns of Software: Tales from the Software Community', total_pages=256, rating=3.84, isbn='9780200000000', published_date=datetime.date(1996, 8, 15), publisher_id=1),
    Book(title='Anatomy Of LISP', total_pages=446, rating=4.43, isbn='9780070000000', published_date=datetime.date(1978, 1, 1), publisher_id=3),
    Book(title='Computing machinery and intelligence', total_pages=24, rating=4.17, isbn=None, published_date=datetime.date(2009, 3, 22), publisher_id=4),
    Book(title='XML: Visual QuickStart Guide', total_pages=269, rating=3.66, isbn='9780320000000', published_date=datetime.date(2009, 1, 1), publisher_id=5),
    Book(title='SQL Cookbook', total_pages=595, rating=3.95, isbn='9780600000000', published_date=datetime.date(2005, 12, 1), publisher_id=7),
    Book(title='The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', total_pages=439, rating=4.29, isbn='9781440000000', published_date=datetime.date(2010, 7, 1), publisher_id=6),
    Book(title='Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', total_pages=222, rating=3.54, isbn='9780750000000', published_date=datetime.date(2007, 2, 13), publisher_id=7)
]

for book in books:
    session.add(book)
session.commit()

book_authors = [
    BookAuthor(book_id=1, author_id=1),
    BookAuthor(book_id=2, author_id=8),
    BookAuthor(book_id=3, author_id=7),
    BookAuthor(book_id=4, author_id=6),
    BookAuthor(book_id=5, author_id=5),
    BookAuthor(book_id=6, author_id=4),
    BookAuthor(book_id=7, author_id=3),
    BookAuthor(book_id=8, author_id=2),
    BookAuthor(book_id=9, author_id=4),
    BookAuthor(book_id=10, author_id=1)
]

for book_author in book_authors:
    session.add(book_author)
session.commit()

# 4) Use Pandas to read and display a table
df_publishers = pd.read_sql("SELECT * FROM publishers", engine)

print(df_publishers)
