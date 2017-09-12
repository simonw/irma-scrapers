#https://eureka.ykyuen.info/2015/04/02/python-read-and-parse-a-json-via-url/
import urllib2
import json

#response = urllib2.urlopen("https://github.com/simonw/irma-scraped-data/blob/master/irma-shelters.json")
#data = json.loads(response)
#print data

# https://gist.github.com/sirleech/2660189
req = urllib2.Request("https://github.com/simonw/irma-scraped-data/blob/master/irma-shelters.json")
opener = urllib2.build_opener()
f = opener.open(req)
json = json.loads(f.read())
print json

