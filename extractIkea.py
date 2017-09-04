#!/usr/bin/python

__author__ = 'aminah.nuraini92@gmail.com'

import sys
import requests
import datetime
import json
import pandas as pd

def req_with_retry(url):
	for i in range(1,6):
		try:
			r = requests.get(url)
			result_json = json.loads(r.content)
			if result_json.get('message') == 'A general internal error has occurred.': continue
			return result_json['extractorData']['data'][0]['group']
		except Exception as e: pass
	print("Failed to scrape extraction from {}\nReason: {}".format(url, e))

if __name__ == '__main__':
	api_key = sys.argv[1]

	base_extract_url = "https://extraction.import.io/query/extractor/{}?_apikey={}&url={}"

	search_extract_id = "3cf6d6df-0d64-4e81-9145-dbfdb7881c8d"
	base_search_extract_url = "http%3A%2F%2Fwww.ikea.com%2Fus%2Fen%2Fsearch%2F%3Fquery%3Dchair%26pageNumber%3D{}"

	detail_extract_id = "247a2989-3659-4690-8792-f4a2f6dc5c43"

	columns = ["Product name" , "Product price"  , "Article number" , "Product Image" ]
	df1 = pd.DataFrame(columns=columns)

	# To avoid duplicates, collect the search results first before scraping the detail
	search_results = set()
	search_data = {}

	for i in range(1,11):
		page_search_results = req_with_retry(
			base_extract_url.format(
				search_extract_id,
				api_key,
				base_search_extract_url.format(i)
				)
			)
		if page_search_results: 
			for page_search_result in page_search_results:
				item_url = page_search_result['Product name'][0]['href']
				search_results.add(item_url)
				search_data[item_url] = {
					'Product name':page_search_result['Product name'][0]['text'],					
					'Product price':page_search_result['Product price'][0]['text']
					}
	print len(search_results)

	for item_url in search_results:			
		detail_result = req_with_retry(
    		base_extract_url.format(
    			detail_extract_id,
    			api_key,
    			item_url
    			)
    		)
		if detail_result:
			df2 = pd.DataFrame(
				{ "Product name" : [search_data[item_url]['Product name'].encode('UTF-8')], 
				"Product price" : [search_data[item_url]['Product price'].encode('UTF-8')], 
				"Article number" : [detail_result[0]['Article number'][0]['text'].encode('UTF-8')], 
				"Product Image" : [detail_result[0]['Product Image'][0]['src'].encode('UTF-8')] }
				)
			print(df2)
	        df1 = df1.append(df2)

	now = datetime.datetime.now()
	datetime_stamp = now.strftime("%Y%m%d%H%M")

	filename = "ikea-chair_{}.csv".format(datetime_stamp)

	df1.to_csv(filename,index=False)
