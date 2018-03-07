import os
import re
import ast
import json
import pandas as pd
from datetime import datetime

from scrapy import signals, Spider, Item, Field
from scrapy.contrib.exporter import CsvItemExporter
from scrapy.http import Request
from scrapy.conf import settings

field_list = []

class PagueMenosSpider(Spider):
    name = 'pague_menos'
    allowed_domains = ['paguemenos.com.br']
    start_urls = ['https://www.paguemenos.com.br/']
 
    custom_settings = {"OWNER_EMAIL_LIST": ['brenda.ramires@bigdata.com.br'],"MANAGER_EMAIL_LIST": [],"OWNER_NOTIFICATION_ON_SUCCESS": True}

    def __init__(self):
        super(PagueMenosSpider, self).__init__()

    @classmethod
    def from_crawler(cls, crawler):
        out = 'output/pague_menos'
        if not os.path.exists(out):
            os.makedirs(out)

        spider = cls()
        spider.file = open('%s/pague_menos-%s.csv' % (out, datetime.now().strftime('%Y-%m-%d')), 'w+b')
        spider._set_crawler(crawler)
        kwargs = {}
        kwargs['fields_to_export'] = field_list
        kwargs['encoding'] = crawler.settings.get('EXPORT_ENCODING', 'utf-8')
        kwargs['include_headers_line'] = True
        spider.exporter = CsvItemExporter(spider.file, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.item_scraped, signals.item_scraped)
        return spider

    def spider_opened(self, spider):
        spider.exporter.start_exporting()

    def spider_closed(self, spider):
        spider.exporter.finish_exporting()
        spider.file.close()

    def item_scraped(self, item, response, spider):
        spider.exporter.export_item(item)

    def parse(self, response):
        
        menu = re.findall(r'class=\"menu-item-texto\" href=\"(.*?)\"', response.body,flags=re.S)

        for url in menu:
            request = Request(url, callback=self.fetch_base_url)
            yield request
    
    def fetch_base_url(self, response):
        
        url = re.findall(r'\.load\(\'(.*?)\'', response.body,flags=re.S)
        if (url):
            re.sub(r'PS=48', r'PS=100', url[0])
            url = response.request.url + url[0]
        
            request = Request(url + "1", callback=self.parse_products_list_page)
            request.meta['page_number'] = 1
            request.meta['base_url'] = url
            yield request
        
            
    def parse_products_list_page(self, response):
        
        products = re.findall(r'itemprop=\"url\" href=\"(.*?)\"', response.body,flags=re.S)

        if products != None and len(products) > 0:
            request = Request(response.meta['base_url'] + str(response.meta['page_number'] + 1), callback=self.parse_products_list_page)
            request.meta['page_number'] = int(response.meta['page_number']) + 1
            request.meta['base_url'] = response.meta['base_url']
            yield request
        
            for product_url in products:
                requestProduct = Request(product_url, callback=self.parse_product_page)
                yield requestProduct 
        
    def parse_product_page(self, response):
        
        product_info = re.findall(r'vtex\.events\.addData\((.*?)\)\;', response.body,flags=re.S)
        ms_number = re.findall(r'\"value-field NumeroRegistroMS\">(.*?)<\/td>', response.body,flags=re.S)
        
        product_object = json.loads(product_info[0])
        
        item = PagueMenosSpiderItem()

        item['name'] = product_object['productName']
        item['brandId'] = product_object['productBrandId']
        item['brand'] = product_object['productBrandName']
        item['categoryId'] = product_object['productDepartmentId']
        item['category'] = product_object['productDepartmentName']
        item['subcategoryId'] = product_object['productCategoryId']           
        item['subcategory'] = product_object['productCategoryName']
        item['oldPrice'] = product_object['productListPriceFrom']
        item['newPrice'] = product_object['productPriceFrom']
        item['pagueMenosId'] = product_object['productId']
        if (ms_number != None and len(ms_number) > 0):
            item['MSNumber'] = ms_number[0]
        
        yield item
        

class PagueMenosSpiderItem(Item):
    field_list = ['name','brandId','brand','categoryId','category','subcategoryId','subcategory','oldPrice','newPrice', 'pagueMenosId', 'MSNumber']
    fields = {f:Field() for f in field_list}