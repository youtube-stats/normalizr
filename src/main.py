import psycopg2


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


def insert_subs(conn, obj: Subs):
    sql: str = 'INSERT INTO youtube.stats.metric_subs (time, id, subs) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.vec())
    conn.commit()
    cursor.close()


def insert_views(conn, obj: Views):
    sql: str = 'INSERT INTO youtube.stats.metric_views (time, id, views) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.vec())
    conn.commit()
    cursor.close()


def insert_videos(conn, obj: Videos):
    sql: str = 'INSERT INTO youtube.stats.metric_videos (time, id, videos) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.vec())
    conn.commit()
    cursor.close()


def main() -> None:
    print("Hello", "world")


if __name__ == '__main__':
    main()
