from urllib import urlopen
import re
import simplejson
import quotedao


IDT_LIST_URL = 'http://cotacoes.economia.uol.com.br/ws/asset/stock/list?size=10000'

def geturl(url):
    print 'looking url ' + (url)
    content = urlopen(url).read()
    print 'load successful'
    return content

def main():
    idtstr = geturl(IDT_LIST_URL)
    idtjson = simplejson.loads(idtstr)
    if quoteDao.insertCompanies(idtjson['data']):
        print "%d companies fetched"
    else: 
        print "no companies fetched"
if (__name__=='__main__'):
    main()
 