# -*- coding: utf-8 -*-

"""
编写创建数据库的类，并构建connectMysql方法
author:王诚坤
date：2018/10/16
update: 2018/10/29
"""

import pymysql
import csv


class MySQLCommand(object):
    # 初始化类
    def __init__(self):
        # 数据库地址
        self.host = '192.168.1.110'
        # 端口号
        self.port = 3306
        # 用户名
        self.user = 'root'
        # 密码
        self.password = 'sim509'
        # 数据库名
        self.db = 'wechat_message'

    def connectMysql(self, table='wechat_message'):
        """
        建立数据库连接
        :return:
        """
        try:
            self.table = table
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user,
                                        passwd=self.password, db=self.db, charset='utf8')
            self.cursor = self.conn.cursor()
            print("数据库已连接！")
        except pymysql.Error as e:
            print('连接数据库失败！')
            print(e)

    def insertData(self, data_dict, primary_key='msgId'):
        """
        将数据插入数据库，首先检查数据是否已经存在，如果存在则不插入
        :param data_dict: 要插入的数据字典
        :param primary_key: 主键
        :return:
        """

        # 检测数据是否存在
        sqlExit = "SELECT %s FROM %s WHERE %s = '%s' " % (primary_key, self.table, primary_key, data_dict[primary_key])
        # 执行查找语句
        # print(sqlExit)
        res = self.cursor.execute(sqlExit)
        if res:
            print('数据已经存入数据库', res)
            return 0
        # 数据不存在，则执行插入操作
        try:
            # 拼接属性名
            cols = ','.join(data_dict.keys())
            # 拼接属性名对应的值
            values = '","'.join(data_dict.values())
            # 插入语句
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.table, cols, '"' + values + '"')
            # print(sql)
            try:
                # 执行插入操作
                result = self.cursor.execute(sql)
                insert_id = self.conn.insert_id()
                self.conn.commit()

                if result:
                    print('插入成功', insert_id)
                    return insert_id + 1
            except pymysql.Error as e:
                # 如果出现异常，执行回滚操作
                self.conn.rollback()
                if "key 'PRIMARY'" in e.args[1]:
                    print('数据已存在，未再次插入！')
                else:
                    print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except pymysql.Error as e:
            print("数据库错误，原因 %d: %s" % (e.args[0], e.args[1]))

    def select_word(self, word):
        sql = "SELECT * FROM %s WHERE word = '%s'" % (self.table, word)
        res = self.cursor.execute(sql)
        if res:
            result = self.cursor.fetchone()
            return result
        else:
            raise Exception("数据库中没有找到该'%s'！" % word)

    def select_order(self, title_list, situation='', order='ASC'):
        """
        查找所有数据中的某几列
        :param situation: 条件语句，即WHERE语句
        :param title_list: 要查找的列的名称
        :param order: 排序方式，ASC为升序，DESC为降序
        :return:
        """
        title = ','.join(title_list)
        sql = "SELECT %s FROM %s %s ORDER BY createTime %s" % (title, self.table, situation, order)
        res = self.cursor.execute(sql)
        if res:
            result = self.cursor.fetchall()
            return result
        else:
            raise  Exception("数据库未找到对应数据！")

    def closeMysql(self):
        """
        关闭数据库连接
        :return:
        """
        self.cursor.close()
        self.conn.close()
        print('数据库连接已关闭！')


def main():
    # 初始化并建立数据库连接
    conn = MySQLCommand()
    conn.connectMysql(table="wechat_contact")
    # 读取csv文件
    with open('data/wechat_contact_0314.csv', 'r', encoding="UTF-8") as f:
        csv_file = csv.DictReader(f)
        i = 0
        while True:
            try:
                i = i + 1
                data_dict = next(csv_file)
                # data_dict['content'] = str(data_dict['content']).replace("\"", '\\"')
            except UnicodeDecodeError as e:
                print(e)
                print(data_dict)
                break
            except StopIteration:
                print('遍历结束！')
                break
            try:
                conn.insertData(data_dict, primary_key="username")
            except pymysql.err.ProgrammingError as e:
                print(e)
                print(str(i) + '行出现错误，手动处理')
                print(data_dict)
            # print(i, "插入成功！")
    # 关闭数据库连接
    conn.closeMysql()


if __name__ == '__main__':
    main()
