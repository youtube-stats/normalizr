import psycopg2
from typing import Optional


class Metrics:
    def __init__(self, time, idx, subs, views, videos):
        self.time = time
        self.idx = idx
        self.subs = subs
        self.views = views
        self.videos = videos

    def all(self):
        return [self.time, self.idx, self.subs, self.views, self.videos]

    def subs(self):
        return [self.time, self.idx, self.subs]

    def views(self):
        return [self.time, self.idx, self.views]

    def videos(self):
        return [self.time, self.idx, self.videos]


def connect():
    return psycopg2.connect(user='admin', password='', host='localhost', port='5432', database='youtube')


conn = connect()


def insert_subs(obj: Metrics):
    print('Inserting subs:', obj.time, obj.idx, obj.subs)

    sql: str = 'INSERT INTO youtube.stats.metric_subs (time, channel_id, subs) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.subs())
    conn.commit()
    cursor.close()


def check_subs(channel_id, subs) -> bool:
    print('Checking subs:', channel_id, subs)

    query: str = 'SELECT subs FROM youtube.stats.metric_subs WHERE channel_id = %s ORDER BY time ASC LIMIT 1'
    cursor = conn.cursor()
    cursor.execute(query, [channel_id])

    record = cursor.fetchone()
    cursor.close()

    if record is None:
        print('Checking subs - no results')
        return True
    else:
        pred: bool = subs == record[0]
        print('Checking subs - inserting?', pred)

        return pred


def insert_views(obj: Metrics):
    print('Inserting views:', obj.time, obj.idx, obj.views)

    sql: str = 'INSERT INTO youtube.stats.metric_views (time, channel_id, views) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.views())
    conn.commit()
    cursor.close()


def check_views(channel_id, views) -> bool:
    print('Checking views:', channel_id, views)

    query: str = 'SELECT views FROM youtube.stats.metric_views WHERE channel_id = %s ORDER BY time ASC LIMIT 1'
    cursor = conn.cursor()
    cursor.execute(query, [channel_id])

    record = cursor.fetchone()
    cursor.close()

    if record is None:
        print('Checking views - no results')
        return True
    else:
        pred: bool = views == record[0]
        print('Checking views - inserting?', pred)

        return pred


def insert_videos(obj: Metrics):
    print('Inserting videos:', obj.time, obj.idx, obj.videos)

    sql: str = 'INSERT INTO youtube.stats.metric_videos (time, channel_id, videos) VALUES (%s, %s, %s);'
    cursor = conn.cursor()

    cursor.execute(sql, obj.videos())
    conn.commit()
    cursor.close()


def check_videos(channel_id, videos) -> bool:
    print('Checking videos:', channel_id, videos)

    query: str = 'SELECT videos FROM youtube.stats.metric_videos WHERE channel_id = %s ORDER BY time ASC LIMIT 1'
    cursor = conn.cursor()
    cursor.execute(query, [channel_id])

    record = cursor.fetchone()
    cursor.close()

    if record is None:
        print('Checking videos - no results')
        return True
    else:
        pred: bool = videos == record[0]
        print('Checking videos - inserting?', pred)

        return pred


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


def delete_row(row: Metrics):
    print('Erasing', row.time, row.idx, row.subs, row.views, row.videos)
    delete: str = 'DELETE FROM youtube.stats.metrics WHERE time = %s AND channel_id = %s AND subs = %s AND views = %s AND videos = %s'

    cursor = conn.cursor()
    cursor.execute(delete)
    conn.commit()
    cursor.close()


def main() -> None:
    print("start")
    while True:
        row: Optional[Metrics] = get_row()

        if row is None:
            print('Got None')
            break

        print('Got row', row.time, row.idx, row.subs, row.views, row.videos)

        if check_subs(row.idx, row.subs):
            insert_subs(row)

        if check_views(row.idx, row.views):
            insert_views(row)

        if check_videos(row.idx, row.videos):
            insert_videos(row)

        delete_row(row)

    conn.close()
    print('done')


if __name__ == '__main__':
    main()
