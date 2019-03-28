# -*- coding: utf-8 -*-
"""
1. 将聊天数据按照群号，划分不同的文件 √
2. 统计每个群中发言次数最多的ID √
3. 统计每个群的活跃度，和所有群加起来的活跃度，在每天时间上的分布（得出的按照每分钟的效果并不好看，尝试每小时或每半小时！）√
4. 统计每个人的发言长短，和平均长度 √
5. 统计每个群中，被@的次数 √
6. 统计四个群中被@的次数，同5
7. 统计用户发链接或图片的次数(也可以统计发图片链接的次数/总发言数)
8. 四个群分别提到最多的词(去掉stopwords) √
"""

from commonTools.ConnectDatabase import MySQLCommand
import time
import os
import re
import jieba


def getChatRoom(connector):
    """
    1. 将聊天数据按照群号，划分不同的文件 √
    数据库返回的数据：第7列为时间（时间为timestamp毫秒类型，需要除以1000取整），第8列为群号，第9列为具体ID和聊天内容
    :param connector: 数据库缓冲池
    :return:
    """
    sql = "select * from wechat_message "
    result = connector.execute(sql)
    for t in range(result):
        print('第', t + 1, '条内容正在划分~')
        one_message = connector.fetchone()
        room_num = one_message[7]
        times = deal_time(one_message[6])

        with open('data/' + room_num + '.txt', 'a', encoding='utf-8') as f:
            f.write(time.strftime('%Y-%m-%d %H:%M:%S', times) + '\t' + one_message[8] + '\n')
    print('******文件划分完成(没有遇到BUG)!!!******')


def countSender(connector):
    """
    2. 统计每个群中发言次数最多的ID √
    :param connector:数据库缓冲池
    :return: 将统计结果保存到result的文件夹中
    """
    # 获取文件夹所有内容
    file_list = os.listdir('data/')

    # 将内容分解的正则表达式
    re_con = re.compile(r'^(.*)(:\n)(.*)')

    sql = "select content from wechat_message where talker = "
    for file_name in file_list:
        # 将文件的.txt后缀去掉
        file_name = file_name.replace('.txt', '')
        # 拼接sql语句
        final_sql = sql + "'" + file_name + "'"
        res = connector.execute(final_sql)
        user_dict = {}
        for t in range(res):

            print('这是群', file_name, '的第', t + 1, '条信息')
            context = connector.fetchone()[0]
            # print(context)

            '''
            # 将撤回的消息去除
            if '撤回了一条消息' in context:
                continue
            '''
            split_res = re_con.match(context)

            # 获取用户ID(当无法获取ID时，可以直接跳过，并打印该条内容)
            try:
                user_id = split_res[1]
            except:
                print(context)
                continue

            # 统计信息
            if user_id not in user_dict:
                user_dict[user_id] = 1
            else:
                user_dict[user_id] += 1

        # 将获得的统计结果输出到指定文件
        with open('contents/count_user_id_' + file_name + '.txt', 'w', encoding='utf-8') as f:
            for user_num in user_dict:
                f.write(user_num + '\t' + str(user_dict[user_num]) + '\n')
        print('******%s文件统计完成(没有遇到BUG)!!!******' % file_name)


def count_by_time(connector):
    """
    3. 统计每个群的活跃度，和所有群加起来的活跃度，在每天时间上的分布
    :param connector: 数据库缓冲池
    :return:count_by_hours_number.txt
    """
    file_list = os.listdir('data/')

    # 基础sql语句
    sql = "select createTime from wechat_message where talker = "

    for file_name in file_list:
        # 获取群号
        file_name = file_name.replace('.txt', '')
        # 拼接sql语句
        final_sql = sql + "'" + file_name + "'"
        res = connector.execute(final_sql)
        # 初始化字典
        time_dict = {}
        for t in range(res):
            print('这是群', file_name, '的第', t + 1, '条信息')
            times = deal_time(connector.fetchone()[0])
            c_time = str(times.tm_hour)  # + ':' + str(times.tm_min)

            # 将统计数据保存到字典中
            if c_time not in time_dict:
                time_dict[c_time] = 1
            else:
                time_dict[c_time] += 1

        # 将获得的统计结果输出到指定文件
        with open('contents/count_by_hour_' + file_name + '.txt', 'w', encoding='utf-8') as f:
            for c_time in time_dict:
                f.write(c_time + '\t' + str(time_dict[c_time]) + '\n')
        print('******%s文件统计完成(没有遇到BUG)!!!******' % file_name)


def count_by_time_all(connector):
    sql = "select createTime from wechat_message"
    time_dict = {}
    res = connector.execute(sql)
    for t in range(res):
        print('第', t + 1, '条信息')
        times = deal_time(connector.fetchone()[0])
        c_time = str(times.tm_hour)  # + ':' + str(times.tm_min)

        # 将统计数据保存到字典中
        if c_time not in time_dict:
            time_dict[c_time] = 1
        else:
            time_dict[c_time] += 1
    with open('contents/count_by_hour_all.txt', 'w', encoding='utf-8') as f:
        for c_time in time_dict:
            f.write(c_time + '\t' + str(time_dict[c_time]) + '\n')
    print('******%s文件统计完成(没有遇到BUG)!!!******')


def count_len(connector):
    file_list = os.listdir('data/')

    # 将内容分解的正则表达式
    re_con = re.compile(r'^(.*)(:\n)(.*)')
    # 基础sql语句
    sql = "select content from wechat_message where talker = "

    for file_name in file_list:
        # 获取群号
        file_name = file_name.replace('.txt', '')
        # 拼接sql语句
        final_sql = sql + "'" + file_name + "'"
        res = connector.execute(final_sql)
        # 初始化字典
        len_dict = {}
        for t in range(res):
            print('这是群', file_name, '的第', t + 1, '条信息')
            context = connector.fetchone()[0]
            # print(context)

            # 获取用户ID(当无法获取ID时，可以直接跳过，并打印该条内容)
            try:
                split_res = re_con.match(context)
                user_id = split_res[1]
                only_con = context.replace(split_res[1] + split_res[2], '')
                if '<' in only_con:
                    lens = 1
                else:
                    lens = len(only_con)
                # print(only_con)
            except:
                print(context)
                continue
            if user_id not in len_dict:
                len_dict[user_id] = {'lens': lens, 'times': 1}
            else:
                len_dict[user_id]['lens'] += lens
                len_dict[user_id]['times'] += 1

        # 将统计结果保存到文件
        with open('contents/count_len_' + file_name + '.txt', 'w', encoding='utf-8') as f:
            for user_id in len_dict:
                f.write(user_id + '\t' + str(len_dict[user_id]['lens']) + '\t' + str(len_dict[user_id]['times']) + '\n')
        print('******%s文件统计完成(没有遇到BUG)!!!******')


def count_at(connector):
    file_list = os.listdir('data/')

    # 将内容分解的正则表达式
    re_at = re.compile(r'^@.*\??')
    # 基础sql语句
    sql = "select content from wechat_message where talker = "
    for file_name in file_list:
        # 获取群号
        file_name = file_name.replace('.txt', '')
        # 拼接sql语句
        final_sql = sql + "'" + file_name + "'"
        res = connector.execute(final_sql)
        # 初始化字典
        name_dict = {}

        for t in range(res):
            user_name = ''
            print('这是群', file_name, '的第', t + 1, '条信息')
            context = connector.fetchone()[0]

            if '<' in context:
                continue

            if '@' in context:
                split_res = re.search(r'@.*?[，\? ]', context, re.M)

                if split_res is None:
                    split_res = re.search(r'@.*', context, re.M)
                    user_name = split_res.group()
                else:
                    user_name = split_res.group()[:-1]
                user_name = user_name.replace('@', '')
            # 保存到字典
            if user_name not in name_dict:
                name_dict[user_name] = 1
            else:
                name_dict[user_name] += 1
        # 保存到文件
        with open('contents/count_@_username_' + file_name + '.txt', 'w', encoding='utf-8') as f:
            for user_name in name_dict:
                f.write(user_name + '\t' + str(name_dict[user_name]) + '\n')
        print('******%s文件统计完成(没有遇到BUG)!!!******' % file_name)


def count_word(connector):
    # 获取文件夹所有内容
    file_list = os.listdir('data/')

    # 加载停用词
    stop_words = get_stopwords('contents/stopwords.txt')
    # 将内容分解的正则表达式
    re_con = re.compile(r'^(.*)(:\n)(.*)')

    sql = "select content from wechat_message where talker = "
    for file_name in file_list:
        # 将文件的.txt后缀去掉
        file_name = file_name.replace('.txt', '')
        # 拼接sql语句
        final_sql = sql + "'" + file_name + "'"
        res = connector.execute(final_sql)
        word_dict = {}
        for t in range(res):
            print('这是群', file_name, '的第', t + 1, '条信息')
            context = connector.fetchone()[0]
            if '<' in context or "撤回" in context:
                continue
            try:
                split_res = re_con.match(context)
                # user_id = split_res[1]
                only_con = context.replace(split_res[1] + split_res[2], '')
                seg = jieba.cut(only_con, cut_all=True)
                for word in seg:
                    if word not in stop_words:
                        if word not in word_dict:
                            word_dict[word] = 1
                        else:
                            word_dict[word] += 1
            except:
                print(context)
                continue
                # 保存到文件
        with open('contents/count_word_' + file_name + '.txt', 'w', encoding='utf-8') as f:
            for word in word_dict:
                f.write(word + '\t' + str(word_dict[word]) + '\n')
        print('******%s文件统计完成(没有遇到BUG)!!!******' % file_name)


def count_special(connector):
    sql = "select type from wechat_message"

    res = connector.execute(sql)

    type_dict = {}
    print('开始分类')
    for t in range(res):

        type_num = connector.fetchone()[0]
        if type_num not in type_dict:
            type_dict[type_num] = 1
        else:
            type_dict[type_num] += 1

    print('结果为：' + str(type_dict))


def get_stopwords(filepath):
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
    return stopwords


def deal_time(times):
    """
    将时间戳timestamp转换为北京时间
    :param times:
    :return:
    """
    return time.localtime(int(times / 1000))


def count_time_of_times():
    """
    统计每个群说话次数的分布
    :return:
    """
    f_list = ['6342900410', '6378298822', '6506298440', '6599081320']
    file_list = os.listdir('contents/')
    i = 0
    for file_name in file_list:
        if 'count_len_' not in file_name:
            continue
        result = {}
        f_name = f_list[i] + '_by_times.txt'
        with open('contents/' + file_name, 'r') as f:
            for line in f.readlines():
                res = line.split('\t')
                times = res[2].replace('\n', '')
                if times not in result:
                    result[times] = 1
                else:
                    result[times] += 1

        with open('contents/' + f_name, 'w') as f:
            for r in result:
                f.write(r + '\t' + str(result[r]) + '\n')
        i += 1


def main():
    # 建立数据库连接
    conn = MySQLCommand()
    conn.connectMysql()
    """
    # 1. 将聊天数据按照群号，划分不同的文件 
    getChatRoom(conn.cursor)
    """
    """
    countSender(conn.cursor)
    """

    # count_by_time(conn.cursor)
    # count_by_time_all(conn.cursor)
    # count_len(conn.cursor)
    # count_at(conn.cursor)
    # count_word(conn.cursor)
    # count_special(conn.cursor)
    count_time_of_times()

    # 关闭数据库连接
    conn.closeMysql()


if __name__ == '__main__':
    main()
