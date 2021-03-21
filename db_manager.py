import pymysql


class DBManager():
    def __init__(self, host="localhost", user="user", password="password",
                 database="stock_price"):
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    def insert_stock_log(self, stock_id, name, price_at, open_val, high, low,
                         close_val, vol):
        if not self.check_stock(stock_id):
            self.insert_stock(stock_id, name)
        self.insert_stock_data(stock_id, price_at, open_val, high, low,
                               close_val, vol)

    def insert_stock_data(self, stock_id, price_at, open_val, high, low,
                          close_val, vol):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO `stock_data`
                (`stock_id`, `price_at`, `open`, `high`, `low`, `close`, `vol`)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                """, (stock_id, price_at, open_val, high, low, close_val, vol))

    def check_stock(self, stock_id):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM `stock` where id=%s",
                           (stock_id,))
            return True if cursor.fetchone()[0] > 0 else False

    def insert_stock(self, stock_id, name):
        with self.connection.cursor() as cursor:
            cursor.execute("INSERT INTO `stock`(id, name) VALUES(%s, %s)",
                           (stock_id, name))

    def update_stock(self, stock_id, date, open_val, high, low,
                     close_val, vol):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """UPDATE `stock_data` SET open=%s, high=%s, low=%s, close=%s, vol=%s
                WHERE stock_id=%s AND price_at=%s
                """, (open_val, high, low, close_val, vol, stock_id, date)
            )

    def get_latest_date(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT MAX(price_at) FROM `stock_data`")
            return cursor.fetchone()[0]
    
    def get_first_date(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT MIN(price_at) FROM `stock_data`")
            return cursor.fetchone()[0]

    def commit(self):
        self.connection.commit()

    def read_stock(self, stock_id=8591, start=None, end=None):
        start = self.get_first_date() if not start else start
        end = self.get_latest_date() if not end else end
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT * FROM `stock_data` WHERE stock_id=%s
                and price_at>=%s and price_at<=%s
                order by price_at""", (stock_id, start, end)
            )
            return cursor.fetchall()


if __name__ == '__main__':
    print("Hello World")
    with DBManager() as db:
        # db.insert_stock("BTC", "bitcoin")
        rst = db.read_stock()