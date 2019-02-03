# coding:utf-8


import requests
import json
from pyquery import PyQuery as pq
import redis
import time
from my_logger import Logger
import traceback

local_time = time.localtime()
log_time = time.strftime("%H-%M-%S", local_time)
log_dir = time.strftime("%Y-%m-%d", time.localtime())
logger = Logger(logname='log/' + log_dir + ' ' + log_time + '.log', loglevel=1, logger="hhymyi").getlog()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
    'Accept-Language': 'zh-CN,zh;q=0.9'
}
url = 'https://www.smzdm.com/homepage/json_more'
proxies = {'https': '10.37.235.10:8080'}
check_url = 'http://localhost:8085/zdm-notice/getMailNoticeByArticleId?articleId='

sleep_seconds = 300  # 休眠的间隔 秒
search_page = 5  # 查询的页数

redis_conn = redis.Redis(host='localhost', port=6379)
channel = 'chat'


def loop():
    logger.info('开始')
    try:
        count = 0
        while True:
            count += 1
            logger.info('第' + str(count) + '次爬取 开始')
            for i in range(1, search_page + 1):
                creep(i)
            logger.info('第' + str(count) + '次爬取 结束')
            time.sleep(sleep_seconds)
    except BaseException as e:
        logger.info('出错')
        logger.error(traceback.format_exc())
        raise e


def creep(p, retry=True):
    logger.info('   第' + str(p) + '页')
    response = None
    try:
        response = requests.get(url, headers=headers, params={'p': p}, timeout=5)
    except:
        if retry:
            logger.info('   重试中')
            return creep(p, False)
        else:
            return None
    logger.info('   response' + str(response.status_code))
    if response.status_code == 200:
        resp = json.loads(response.text)
        if resp['error_code'] == 0 and len(resp['data']) > 0:
            for row in resp['data']:
                # print(row)
                if row.get('article_id') is not None:
                    message = creep_row(row)
                    if message is not None:
                        logger.info('消息：' + str(message))
                        redis_conn.publish(channel, str(message))
                # print('--------------------------------------------------')


def creep_row(row, retry=True):
    article_id = row.get('article_id')
    title = ''
    article_url = row['article_url']
    article_type = row.get('article_type')  # 文章类型：好价、好文
    logger.info(article_url)
    sub_resp = None
    try:
        sub_resp = requests.get(article_url, headers=headers, timeout=5)
    except:
        if retry:
            logger.info('   重试中')
            return creep_row(row, False)
        else:
            return None
    time.sleep(0.5)
    logger.info('sub_resp ' + str(sub_resp.status_code))
    if sub_resp.status_code == 200:
        # logger.info(sub_resp.text)
        doc = pq(sub_resp.text)
        if article_type == '好价':
            worth = doc('#rating_worthy_num').text()  # 值
            un_worth = doc('#rating_unworthy_num').text()  # 不值
            comment_num = doc(
                'body > section > div.leftWrap > div.operate_box > div.operate_icon > a.comment > em').text()  # 评论数量
            if int(worth) + int(un_worth) > 20 and int(worth) / (int(worth) + int(un_worth)) > 0.8:
                title = article_type + '-值-' + row.get('article_title')
            elif int(comment_num) > 50:
                title = article_type + '-热议-' + row.get('article_title')
            if len(title) > 0:
                check_res = requests.get(check_url + str(article_id))
                if check_res.status_code == 200 and len(check_res.text) == 0:
                    return_obj = {'articleId': article_id, 'title': title, 'url': article_url}
                    return json.dumps(return_obj)


if __name__ == '__main__':
    loop()

