__author__ = 'barryqiu'

import MySQLdb

OperationalError = MySQLdb.OperationalError


class MySQLDB:
    def __init__(self, DB_HOST, DB_PORT, DB_USER, DB_PWD, DB_NAME, CHARSET="utf8"):
        self.DB_HOST = DB_HOST
        self.DB_PORT = DB_PORT
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_NAME = DB_NAME
        self.CHARSET = CHARSET

        try:
            self.conn = self.getConnection()
            self.conn.autocommit(False)
            self.cur = self.conn.cursor()
        except MySQLdb.Error as e:
            print("Mysql Error %d : %s" % (e.args[0], e.args[1]))


    def getConnection(self):
        return MySQLdb.Connect(
            host=self.DB_HOST,
            port=self.DB_PORT,
            user=self.DB_USER,
            passwd=self.DB_PWD,
            db=self.DB_NAME,
            charset=self.CHARSET
        )


    def __del__(self):
        self.close()


    def query(self, sql):
        try:
            n = self.cur.execute(sql)
            return n
        except MySQLdb.Error as e:
            print("Mysql Error:%s\nSQL:%s" % (e, sql))


    def fetchRow(self):
        result = self.cur.fetchone()
        return result


    def fetchAll(self):
        result = self.cur.fetchall()
        desc = self.cur.description
        d = []
        for inv in result:
            _d = {}
            for i in range(0, len(inv)):
                _d[desc[i][0]] = str(inv[i])
                d.append(_d)
        return d


    def insert(self, table_name, data):
        columns = data.keys()
        _prefix = "".join(['INSERT INTO `', table_name, '`'])
        _fields = ",".join(["".join(['`', column, '`']) for column in columns])
        _values = ",".join(["%s" for i in range(len(columns))])
        _sql = "".join([_prefix, "(", _fields, ") VALUES (", _values, ")"])
        _params = [data[key] for key in columns]
        return self.cur.execute(_sql, tuple(_params))


    def update(self, tbname, data, condition):
        _fields = []
        _prefix = "".join(['UPDATE `', tbname, '`', 'SET'])
        for key in data.keys():
            _fields.append("%s = %s" % (key, data[key]))
        _sql = "".join([_prefix, _fields, "WHERE", condition])

        return self.cur.execute(_sql)


    def delete(self, tbname, condition):
        _prefix = "".join(['DELETE FROM  `', tbname, '`', 'WHERE'])
        _sql = "".join([_prefix, condition])
        return self.cur.execute(_sql)


    def getLastInsertId(self):
        return self.cur.lastrowid


    def rowcount(self):
        return self.cur.rowcount


    def commit(self):
        self.conn.commit()


    def rollback(self):
        self.conn.rollback()


    def close(self):
        self.cur.close()
        self.conn.close()


def md5(str):
    import hashlib
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()


if __name__ == '__main__':
    n = MySQLDB('127.0.0.1', 3306, 'root', '', 'cloudphone')
    tbname = 'user'
    a = ({'name': '3', 'password': md5('3')}, {'name': '4', 'password': md5('4')}, {'name': '5', 'password': md5('5')})
    for d in a:
        n.insert(tbname, d)
    n.commit()
