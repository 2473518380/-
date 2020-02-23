import os
from hashlib import md5
import pymongo
import requests
import re
from multiprocessing import Pool
import time

MONGO_URL = 'localhost:27017'
MONGO_DB = 'wanjiangxinwen'
MONGO_TABLE = 'xinwwentupian'
client = pymongo.MongoClient(MONGO_URL) # 连接MongoDB客户端
db = client[MONGO_DB] # 创建db数据库


def get_page(url):
    # 异常处理模块
    try:
        response = requests.get(url)  # 发起网页请求
        if response.status_code == 200:  # 判断请求是否成功
            result = response.content.decode('utf-8')  # 获取html代码内容
            return result
        return None
    except Exception:
        print('请求出错！！')


def parse_href_page(html):
    pattern = re.compile(r'<span class="fl t"><a href="(.*?)".*?title=".*?</span>', re.S)  # 提取详情页URL的正则表达式对象
    hrefs = re.findall(pattern, html)  # 查找到所有结果
    for i in hrefs:
        href = re.sub(r'(\.\./)+', 'http://wjcollege.ahnu.edu.cn/', i)  # 替换拼接url地址
        yield href  # 生成器


def parse_detail_page(html, url):
    img_pattern = re.compile(r'<p.*?<img.*?src="(/__local.*?)".*?</p>', re.S | re.I)  # 提取图片的正则表达式对象
    title_pattern = re.compile(r'<div class="title-bg">.*?<p>(.*?)</p>.*?</div>', re.S | re.I)  # 提取标题的正则表达式对象
    time_pattern = re.compile(r'&nbsp;&nbsp;(.*?)&nbsp;&nbsp;', re.S | re.I)  # 提取时间的正则表达式对象
    imgs_findall = img_pattern.findall(html)
    titles_search = title_pattern.search(html)
    times_findall = time_pattern.findall(html)

    if len(imgs_findall) > 0 and titles_search and times_findall:  # 筛选： 当网页同时存在图片、标题、时间
        title = titles_search.group(1)  # 获取标题内容
        tim = times_findall[0].strip()  # 获取时间信息
        print('标题:{},时间:{}'.format(title, tim))  # 打印信息
        imgs_url = []  # 用于接收完整的图片URL
        for img in imgs_findall:
            r_img = 'http://wjcollege.ahnu.edu.cn' + img  # 拼接URL
            imgs_url.append(r_img)
            print(r_img)
            download_image(r_img, title)  # 下载图片
        # 存储到 MongoDB中的数据
        return {
            'url地址': url,
            '标题': title,
            '时间': tim,
            '图片链接地址': imgs_url
        }


def save_img(content, title):
    path = 'D:/毕业论文模板/新闻图片test'
    os.chdir(path)  # 更改工作目录
    if not os.path.exists(title):  # 防止重复下载图片
        os.mkdir(title)  # 创建文件夹
        name = '{}.{}'.format(md5(content).hexdigest(), 'jpg')  # 利用MD5编码给图片命名
        path = title + '/' + name  # 图片写入位置
        with open(path, 'wb') as f:
            f.write(content)  # 写入二进制图片数据


def download_image(url, title):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            result = response.content  # 获取到的是bytes二进制数据，response.text 返回的是一个 unicode 型的文本数据
            save_img(result, title)
        return None
    except Exception:
        print('请求网页失败')


def save_to_mongo(result):
    try:
        if db[MONGO_TABLE].update({'标题': result['标题']}, {'$set': result}, True):  # 防止数据重复添加进MongoDB
            print('存储到mongodb成功')
            return
        print('未存储到mongodb')
    except:
        print('存储有误')


def get_page_download():
    lis = list(reversed(list(x for x in range(223))))  # 得到一个0 - 222的到序列表
    try:
        pages = int(input('请输入要抓取的页数(大于等于1小于等于223的整数):'))  # 用户输入
        # 构造URL分页的页码值
        if pages > 0:
            main('')
            j = pages - 1
            if j > 0:
                groups = []
                lis_split = lis[0:j]
                for i in lis_split:
                    res = '/' + str(i)
                    groups.append(res)
                return groups
        else:
            print('请输入一个大于等于1小于等于223的正整数')

    except:
        print('请输入一个大于等于1小于等于223的正整数')


def main(page):
    url = 'http://wjcollege.ahnu.edu.cn/index/wjxw{}.htm'.format(page)  # URL地址
    page = get_page(url)  # 获取HTML代码
    if page:
        detail_urls = list(parse_href_page(page))  # 获得的详情页URL链接
        print(detail_urls)
        if detail_urls:
            for detail_url in detail_urls:
                html = get_page(detail_url)  # 请求详情页获取HTML代码
                if html:
                    result = parse_detail_page(html, detail_url)  # 解析详情页代码
                    if result:
                        save_to_mongo(result)  # 保存到MOngoDB数据库中


if __name__ == '__main__':
    # 开启多进程
    pool = Pool()  # 生成一个进程池对象
    groups = get_page_download()
    if groups:
        s = time.time()
        for i in groups:
            main(i)
        t1 = time.time()
        print('顺序执行时间:',int(t1 - s))
        pool.map(main, groups)  # 多进程抓取
        pool.close()  # 关闭进程池
        pool.join()
        t2 = time.time()
        print('并行执行时间:', int(t2 - t1))
