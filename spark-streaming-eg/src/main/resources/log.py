#coding=UTF-8

import random
import time

#定义url数组
url_path=[
"going/112.html",
"going/129.html",
"going/113.html",
"going/156.html",
"going/189.html",
"going/187.html",
"back/165.html",
"back/167.html",
"city/list"
]

#定义IP
ip_slices=[192,165,231,8,88,17,91,98,68]

#定义HTTP请求引用
http_refs=[
"https://cn.bing.com/search?q={query}",
"https://www.baidu.com/s?wd={query}",
"https://www.so.com/s?q={query}",
"https://www.sogou.com/sie?query={query}",
"https://search.yahoo.com/search?p={query}"
]

#定义搜索关键字
search_kword=[
"北京",
"上海",
"天津",
"成都"
]

#定义状态码
status_arr=[200,403,404,500,505]

#主要调用的方法（默认为100条，也可以自己传入参数）
def generate_log(count = 100):
	time_str=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	#打开一个文件，写入日志，附加写权限
	f = open("/usr/local/big-data/tmp/access.log","w+")

	while count>=1:
		quert_log="{happen_time} {ip} GET {url} HTTP1.1 {refs} {status}".format(url=sample_url()
			,ip=sample_ip(),refs=sample_ref(),status=sample_status(),
			happen_time=time_str)
		count = count-1
		# print quert_log
		f.write(quert_log+"\n")

#抽样取url
def sample_url():
	return "".join(random.sample(url_path,1));
#抽样取IP
def sample_ip():
	 ips = random.sample(ip_slices,4);
	 return ".".join(str(item) for item in ips)
#抽样取引用
def sample_ref():
	if random.uniform(0,1)>0.4:
	 	return "-"
#注意if段落和下面的段落要分开
	ref_str="".join(random.sample(http_refs,1))
	return ref_str.format(query="".join(random.sample(search_kword,1)))

def sample_status():
	if random.uniform(0,1)>0.5:
	 	return status_arr[0]

	return random.sample(status_arr,1)[0]


#主函数 main 方法
if __name__ == '__main__':
	generate_log()




