import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, Text, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from time import time
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

engine = create_engine('mysql://%s:%s@%s:%d/%s' % (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME))
Base = declarative_base()


class Quote(Base):
    __tablename__ = 'quote'

    id = Column(Integer, primary_key=True)
    score = Column(Integer)
    content = Column(Text)
    posted_at = Column(DateTime)


def resolve_month(name):
    months = (
        'stycznia', 'lutego', 'marca', 'kwietnia', 'maja', 'czerwca',
        'lipca', 'sierpnia', 'wrzesnia', 'pazdziernika', 'listopada', 'grudnia'
    )

    return months.index(name) + 1


def prepare_datetime(bash_date):
    date = bash_date.split()
    date[1] = resolve_month(date[1])
    time = [int(val) for val in date[3].split(':')]
    date_str = f'{int(date[2])}-{date[1]:02}-{int(date[0]):02} {time[0]:02}:{time[1]:02}'

    return datetime.strptime(date_str, '%Y-%m-%d %H:%M')


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    start_time = time()
    page = 1

    while True:
        res = requests.get(f'http://bash.org.pl/latest/?page={page}')
        if res.status_code != 200:
            break

        res_content = BeautifulSoup(res.content, 'html.parser')

        for post in res_content.findAll('div', {'class': 'post'}):
            instance = Quote(
                id=post.find('a', {'class': 'qid'}).text[1::],
                score=post.find('span', {'class': 'points'}).text,
                content=post.find('div', {'class': 'post-content'}).get_text('\n', strip=True),
                posted_at=prepare_datetime(post.find('div', {'class': 'right'}).get_text(strip=True))
            )
            session.merge(instance)

        session.commit()
        page += 1

    quotes_count = session.query(Quote).count()
    total_time = time() - start_time
    print(f'Successfully scrapped {quotes_count} quotes in {total_time:.0f}s')
    session.close()
