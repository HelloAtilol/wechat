# -*- coding: utf-8 -*-
"""
修改备注的工具类
"""
from commonTools import ConnectDatabase as cd
from commonTools import wechatContent as wc

# 声明全局变量
message_conn = cd.MySQLCommand()
message_conn.connectMysql(table="wechat_message")
contact_conn = cd.MySQLCommand()
contact_conn.connectMysql(table="wechat_contact")
update_conn = cd.MySQLCommand()
update_conn.connectMysql(table="wechat_word")


def getInformationBymsgId(word_conn, msgId, name_list, nicknames):
    """
    根据msgId获取消息的相关信息
    :param name_list: @的备注列表
    :param word_conn: 数据库连接
    :param msgId: 信息ID
    :param nicknames: 被@的用户名昵称列表
    :return:
    """
    title_list = ["content"]
    situation = "WHERE msgId = '%s'" % msgId
    message_conn.select_order(title_list=title_list, situation=situation)
    content = message_conn.cursor.fetchone()[0]
    wechat_content = wc.WechatContent(content)
    result = wechat_content.splitContent()
    userIds = []
    for i in range(len(nicknames)):
        nickname = nicknames[i]
        if nickname is "2":
            userIds.append("")
            continue
        name_cursor = contact_conn.select_order(title_list=["username"], situation="WHERE nickname = '%s'" % nickname)
        userId = name_cursor.fetchone()
        if userId is None:
            name_cursor = contact_conn.select_order(title_list=["username"],
                                                    situation="WHERE username = '%s'" % nickname)
            userId = name_cursor.fetchone()
            if userId is None:
                userIds.append("")
                # 将用户信息保存到联系人数据库
                new_user = {"username": nickname, "conRemark": name_list[i]}
                contact_conn.insertData(new_user, primary_key="username")
            else:
                userIds.append(nickname)
                contact_conn.update_database({"conRemark": name_list[i]}, "WHERE username = '%s'" % nickname)
        else:
            # 搜寻到ID之后，更新备注
            ud = {"conRemark": name_list[i]}
            s = "WHERE username = '%s'" % userId[0]
            contact_conn.update_database(datadict=ud, situation=s)
            # 将ID添加到列表中
            userIds.append(userId[0])
    # 更改发言者、@用户列表、标签Tag
    res = {"userId": result["user_id"], "atUserId": "/".join(userIds), "tag": "1"}
    situation = "WHERE msgId = '%s'" % msgId
    word_conn.update_database(res, situation=situation)


def dealByNickName(chatroomId, chatroomName, res, name_list):
    nicknames = []
    msgId = res[0]
    context = res[1]
    atList = res[2]
    for i in range(len(name_list)):
        atName = name_list[i]
        # 检查备注是否已经存在
        name_cursor = contact_conn.select_order(title_list=["username"], situation="WHERE conRemark = '%s'" % atName)
        userId = name_cursor.fetchone()
        if userId is None:
            # 设置死循环，进行输入处理
            while True:
                print("%s\t被@的用户名：%s" % (chatroomName, atName))
                nickname = input("微信昵称或ID是(输入0代表没有找到，但是@的内容可能存在缺失；输入1代表采用上下文方式补全；输入2代表群内找不到用户)：")
                print("-----------------" * 5)
                # 如果找不到用户，可能是解析出来的用户备注名有误，可手动调整
                if nickname is "0":
                    print("信息原文为：%s" % context)
                    part_name = input("正确的@内容需要添加(不需要更改输入0)：")
                    # 更改了@信息之后，更新数据库中context内容和atList
                    if part_name is not "0":
                        update_data = {"context": context.replace(part_name, part_name + "?"), "atList": str(atList).replace(atName, atName+part_name)}
                        situation = "WHERE msgId = '%s'" % msgId
                        update_conn.update_database(update_data, situation=situation)
                        atName = atName + part_name
                        name_list[i] = atName
                # 获取上下文信息
                elif nickname is "1":
                    next_sql = "SELECT content From wechat_message  where talker = '%s' and msgId >'%s'" % (
                        chatroomId, str(int(msgId) - 3))
                    message_conn.cursor.execute(next_sql)
                    print("之前第二条信息：\n %s" % message_conn.cursor.fetchone()[0])
                    print("之前第一条信息：\n %s" % message_conn.cursor.fetchone()[0])
                    print("原信息：\n %s" % message_conn.cursor.fetchone()[0])
                    print("之后第一条信息：\n %s" % message_conn.cursor.fetchone()[0])
                    print("之后第二条信息：\n %s" % message_conn.cursor.fetchone()[0])
                    print("之后第三条信息：\n %s" % message_conn.cursor.fetchone()[0])
                else:
                    nicknames.append(nickname)
                    break
        # 如果已经存在, 直接更新数据库
        else:
            nicknames.append(userId[0])
    # 将数据存储到数据库
    getInformationBymsgId(update_conn, msgId, name_list, nicknames)


def saveResult(chatroomId, chatroomName):
    # 建立数据库连接
    word_conn = cd.MySQLCommand()
    word_conn.connectMysql(table="wechat_word")

    # 构建跨表查询语句，为了分群标注，减少来回查找用户的数量
    sql = "SELECT msgId, context, atList From wechat_word  where tag = 0 and msgId in (SELECT msgId from  wechat_message where talker = '%s')" % chatroomId
    word_cursor = word_conn.cursor
    res = word_cursor.execute(sql)
    print("结果：", res)

    while True:
        res = word_cursor.fetchone()
        # 遍历结束之后，退出循环

        if res is None:
            break
        # 如果没有@的人，进入下一条
        msgId = res[0]
        atList = res[2]
        nicknames = []
        print("第%s条信息正在处理" % msgId)
        # 如果@的人为空，开启新线程修改数据
        if atList is not "" and atList is not None:
            name_list = atList.split("/")
            dealByNickName(chatroomId, chatroomName, res, name_list)
            continue
            # 将数据存储到数据库
        getInformationBymsgId(update_conn, msgId, [], nicknames)
    # 关闭数据库连接
    word_conn.closeMysql()


def main():
    # 获取所有群编号和群名称
    chatrooms = message_conn.select_distinct()

    for chatroom in chatrooms:
        chatroom = chatroom[0]
        #  查询群名和用户id
        name_sql = "SELECT nickname From wechat_contact WHERE username = '%s'" % chatroom
        message_conn.cursor.execute(name_sql)
        chatroomName = message_conn.cursor.fetchone()[0]
        print("%s 群的@备注修改" % chatroomName)
        print("-----------------" * 5)
        # 处理并保存数据
        saveResult(chatroom, chatroomName)
        # break
    message_conn.closeMysql()
    contact_conn.closeMysql()


if __name__ == '__main__':
    main()
