from sqlalchemy import create_engine
import pymysql
import pandas as pd

'''
author:LancerWu
email:wuxs231@163.com
'''


def df_to_sql(engine_conn, data, table_name):
    '''
    datafrme格式的数据直接写入数据库
    :param engine_conn:数据库连接引擎
    :param data:要存入的字典格式格式的数据
    :param table_name:要存的表名
    :return:
    '''
    try:
        # list形式的字典转为dataframe格式
        df = pd.DataFrame(data)
        # 创建连接数据库的引擎
        engine = create_engine('%s' % engine_conn)
        # 写入数据
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, index_label=False)
        # 关闭engine的连接
        engine.dispose()
        print('写入数据库成功')

    except Exception as e:
        print('dataframe存入数据库失败：%s' % e)
        return None


def list_to_df_to_sql(tb_name, cols, rows, engine_conn):
    '''
    list转dict转df
    :param tb_name:表名
    :param cols:列字段
    :param rows:行数据，仅限一行
    :param engine_conn:数据库连接引擎
    :return:
    '''
    try:
        # 先把列和行转成字典
        dict_temp = dict(zip(cols, rows))
        # 再把字典转换成list
        list_temp = [dict_temp]
        # 最后转为dataframe
        df = pd.DataFrame(list_temp)
        # 构造连接数据库的引擎
        engine = create_engine('%s' % engine_conn)
        # df写入数据库
        df.to_sql(name=tb_name, con=engine, if_exists='append', index=False, index_label=False)
        # 关闭连接引擎
        engine.dispose()
        print('写入数据库成功')

    except Exception as e:
        print('list存入数据库失败：%s' % e)
        return None


def sql_to_df(engine_info, sql):
    '''
    数据库读取为dataframe格式
    :param engine_info:数据库连接引擎
    :param sql:操作的sql语句
    :return:
    '''
    try:
        # 创建数据库连接引擎
        engine = create_engine(engine_info)
        # 从数据库读取结果为dataframe
        df_query = pd.read_sql(sql, engine)
        # 关闭数据库连接
        engine.dispose()
        print('数据库读取完毕', df_query.head())
        # 转为字典list
        data = df_query.to_dict(orient='dict')
        return data

    except Exception as e:
        print('从数据库读取为df失败:%s' % e)
        return None


def sql_caozuo(sql, conn):
    '''
    使用sql语句从数据库中一次性读取
    :param sql:操作的说sql语句
    :param conn:数据库连接的字典
    :return:
    '''
    try:
        # 连接数据库
        conn = pymysql.connect(**conn)
        cur = conn.cursor()
        # 提交sql语句
        cur.execute(sql)
        # 获取查询结果
        results = cur.fetchall()
        # 提交事务执行
        conn.commit()
        # 关闭连接
        cur.close()
        conn.close()

        return results

    except Exception as e:
        print('数据库操作失败：%s' % e)
        return None


if __name__=="__main__":
    # 数据库操作配置参数
    engine_conn = 'mysql+pymysql://(mysql帐号):(帐号密码)@(ip):(端口号)/(table)?charset=utf8'
    pymysql_conn = {
        'host': "",  # ip地址
        'port': 3306,  # 端口号，默认3306
        'user': "",  # mysql帐号
        'password': "",  # 帐号密码
        'db': "",  # 数据库名称
        'charset': 'utf8'  # 编码类型
    }
    sql = "select * from table;"

    # 1、执行数据库查询语句操作
    query_result = sql_caozuo(sql, pymysql_conn)
    print('查询结果', query_result)

    # 2、从数据库查询数据并直接变成dataframe
    data = sql_to_df(engine_conn, sql)  # data为df转化的list，其中包含字典数据
    print('data', data)

    # 3、list插入到数据库
    tb_name = 'test'  # 数据库中要插入的表名
    cols = ['col1', 'col2', 'col3']  # 字段名，要跟数据库现有的字段一一对应
    rows = ['data1', 'data2', 'data3']  # 数据行，要跟cols一一对应，注意跟数据库的字段的数据类型是否匹配
    list_to_df_to_sql(tb_name, cols, rows, engine_conn)

    # 4、dataframe插入到数据库
    data = [{
        'col1': 'data1',
        'col2': 'data2',
        'col3': 'data3'

    }]  # list中装载字典数据
    df_to_sql(engine_conn, data, tb_name)


