import os
import time,datetime
import pyodbc
import requests
import threading
from lxml import etree
from pybloom_live import BloomFilter
from fake_useragent import UserAgent
from google_translate import googleapis_translate
import DB
DATABASE = 'Singapore'
COUNTRY = 'Singapore'
MAX_COUNT=3

header = {'User-Agent': UserAgent().random, 'Accept-Language': 'zh-CN,zh;q=0.9'}


def Bulon():
    if os.path.exists('布隆文件/{}.blm'.format(DATABASE)):
        bf =BloomFilter.fromfile(open('布隆文件/{}.blm'.format(DATABASE),'rb'))
    else:
        bf = BloomFilter(1000000,0.001) 
    return bf
   

def get_html(url, count=1):
    print('Crawling', url)
    print('Trying Count', count)
    if count >= MAX_COUNT:
        print('Tried Too Many Counts')
        return None
    try:
        
        response = requests.get(url, headers=header,timeout=15)
        
        return response
        
    except :
        count += 1
        return get_html(url, count)


def save_to_sql(title,originaltitle,createtime,author,content,originalcontent,articlesource,source,label,keyword,country,url,Englishtitle,Englishcontent,details,originaldetails,Englishdetails):       
 
    sql_insert="insert into Singapore (title,originaltitle,createtime,author,content,originalcontent,articlesource,source,label,keyword,country,url,Englishtitle,Englishcontent,details,originaldetails,Englishdetails,currenttime) values(N'" + title + "',N'" +originaltitle + "',N'" +createtime + "',N'" +author + "',N'" +content + "',N'" +originalcontent + "',N'" +articlesource + "',N'" +source + "',N'" +label + "',N'" +keyword + "',N'" +country + "',N'" +url + "',N'" +Englishtitle + "',N'" +Englishcontent + "',N'" +details + "',N'" +originaldetails + "',N'" +Englishdetails + "',GETDATE())"
    DB.PI_YuQing(sql_insert)

def fanyi(line, from_lang, to_lang):
    line = [line[i:i+2500] for i in range(0,len(line), 2500)]
    result = ''
    for i in line:
    	result += str(googleapis_translate(i, from_lang, to_lang))
    return result
    
def time_stamps(line):   
    try:
        timearray = time.strptime(line,"%b %d, %Y | %I:%M %p")
        line = time.strftime("%Y/%m/%d %H:%M",timearray) 
        return line
    except:
        return line

def Translate(originaltitle,createtime,author,originalcontent,articlesource,source,label,keyword,country,url,originaldetails):
    Englishdetails = fanyi(originaldetails, from_lang='auto', to_lang='en').replace("'","''")
    Englishcontent = Englishdetails[:230].replace("'","''")
    Englishtitle = fanyi(originaltitle, from_lang='auto', to_lang='en').replace("'","''")
    details = fanyi(originaldetails, from_lang='auto', to_lang="zh-CN").replace("'","''")
    content = details[:230].replace("'","''").replace("'","''")
    title = fanyi(originaltitle, from_lang='auto', to_lang="zh-CN").replace("'","''")
    print('{0} 新闻时间: {1} 当前时间: {2}'.format(COUNTRY, createtime, time.ctime()))
    print('{0} 新闻标题: {1}'.format(COUNTRY, title))
    save_to_sql(title,originaltitle,createtime,author,content,originalcontent,articlesource,source,label,keyword,country,url,Englishtitle,Englishcontent,details,originaldetails,Englishdetails)

def parse(url): 
	response = get_html(url,count=1)
	if response:
		selector = etree.HTML(response.content)
		articlesource = ''
		country = COUNTRY
		source = 'http://www.beritaharian.sg'
		author = ''
		keyword = ''
		originaltitle = selector.xpath('//h1[@class="headline node-title"]/text()')[0].strip().replace("'","''")
		createtime = selector.xpath('//div[@class="field-item even"]/text()')[0]
		createtime = time_stamps(createtime)
		try:
			originaldetails = ''.join(selector.xpath('//div[@class="odd field-item"]/p/text()')).strip().replace("'","''")
			originalcontent = originaldetails[:230]
		except:
			pass
		try:
			lab = url.split('/')[3]
		except:
			lab = 'news'
		label=googleapis_translate(lab, from_lang='auto', to_lang='en')
		print(label+'--'+createtime+'--'+url)
		Translate(originaltitle,createtime,author,originalcontent,articlesource,source,label,keyword,country,url,originaldetails)

def spider(h):
    global error_url
    error_url=''
    bf = Bulon()
    url_list=[
            'http://www.beritaharian.sg/setempat'
            'http://www.beritaharian.sg/dunia',
            'http://www.beritaharian.sg/ekoniaga',
            'http://www.beritaharian.sg/sukan',

            'http://www.beritaharian.sg/gah',
            'http://www.beritaharian.sg/hidayah',
            'http://www.beritaharian.sg/soshiok-halal',

            'http://www.beritaharian.sg/kembara',
            'http://www.beritaharian.sg/bahasa-budaya',
            'http://www.beritaharian.sg/pru14',
            'http://www.beritaharian.sg/kesihatan',
            'http://www.beritaharian.sg/wacana'
			]
    for url1 in url_list:
        for i in range(0,10):
            if i==0:
               child_url=url1+'/terkini'
            else:
               child_url=url1+'/terkini?page='+str(i)
               
            if 'pru14' in child_url:
                child_url=child_url.replace("/terkini","")
            child_response = get_html(child_url,count=1)
            if child_response:
                child_selector = etree.HTML(child_response.content)
                urls = child_selector.xpath('//h3/a/@href')                
                for url2 in urls:   
                    if 'http' in url2:
                        url=url2
                    else:
                        url='https://www.beritaharian.sg'+str(url2)
                    print('urls:'+url)
                    error_url=url
                    if url in bf:
                        print('break!!!!!!!!!!!')
                        break
                    else:
                        bf.add(url)
                        parse(url)
                        url=''
                        bf.tofile(open('布隆文件/{}.blm'.format(DATABASE),'wb'))
                if url in bf:
                    break

def main():  
    # while True:
    t=1                
    spider(t)
    print('{0} 将在 {1} 小时后继续开始进行爬取'.format(COUNTRY, t))
        # time.sleep(3600 * t)
        
def run():
    try:
        main()
    except:
        name='Singapore10'
        source='http://www.beritaharian.sg'
        country='Singapore'
        stoptime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        import traceback
        error=str(traceback.format_exc()).replace("'","''")

        sql_insert="insert into ErrorMessage(name,source,country,stoptime,error,error_url,infostatus) values('"+name+"','"+source+"','"+country+"','"+stoptime+"','"+error+"','"+error_url+"','否')"
        DB.PI_Error(sql_insert)
        sql_update="update logmessage set is_error=1 where process_name='"+name+"' and id=(select MAX(id) from logmessage where process_name='"+name+"' ) "
        DB.PI_LocalError(sql_update)   

if __name__ == '__main__':
    main()