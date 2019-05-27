import psycopg2
from typing import Optional


class Metrics:
    def __init__(self, time, idx: int, subs: int, views: int, videos: int):
        self.time = time
        self.idx = idx
        self.subs = subs
        self.views = views
        self.videos = videos


class Subs:
    def __init__(self, time, idx: int, subs: int):
        self.time = time
        self.id = idx
        self.subs = subs

    def vec(self):
        return [self.time, self.id, self.subs]


class Views:
    def __init__(self, time, idx: int, views: int):
        self.time = time
        self.id = idx
        self.views = views

    def vec(self):
        return [self.time, self.id, self.views]


class Videos:
    def __init__(self, time, idx: int, videos: int):
        self.time = time
        self.id = idx
        self.videos = videos

    def vec(self):
        return [self.time, self.id, self.videos]


def connect():
    return psycopg2.connect(user='admin', password='', host='localhost', port='5432', database='youtube')


conn = connect()


def insert_subs(obj: Subs):
    sql: str = 'INSERT INTO youtube.stats.metric_subs (time, id, subs) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.vec())
    conn.commit()
    cursor.close()


def insert_views(obj: Views):
    sql: str = 'INSERT INTO youtube.stats.metric_views (time, id, views) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.vec())
    conn.commit()
    cursor.close()


def insert_videos(obj: Videos):
    sql: str = 'INSERT INTO youtube.stats.metric_videos (time, id, videos) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.vec())
    conn.commit()
    cursor.close()


def get_row() -> Optional[Metrics]:
    query: str = 'SELECT * FROM youtube.stats.metrics ORDER BY time ASC LIMIT 1'
    cursor = conn.cursor()
    cursor.execute(query)
    record = cursor.fetchone()

    cursor.close()

    if record is None:
        return None
    else:
        return Metrics(record[0], record[1], record[2], record[3], record[4])


def main() -> None:
    print("start")
    while True:
        row: Optional[Metrics] = get_row()

        if row is None:
            print('Got None')
            break

        print(get_row())

    conn.close()
    print('done')


if __name__ == '__main__':
    main()
