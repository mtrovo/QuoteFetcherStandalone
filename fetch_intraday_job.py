﻿from urllib import urlopen
import simplejson
import quotedao


ERRORS= {   "FSW-0001":"Este período não é válido.",
            "FSW-0002":"Este período não é válido; insira uma data inicial menor que a data final.",
            "FSW-0003":"A data inicial não é válida; insira uma data menor que a data de hoje.",
            "FSW-0004":"A data final não é válida; insira uma data menor ou igual a data atual.",
            "FSW-0101":"Parâmetro size menor que 1","FSW-0102":"Parâmetro page menor que 1",
            "FSW-0103":"Parâmetro fields inválido","FSW-0104":"Parâmetro idt menor que 1",
            "FSW-0201":"Parâmetro inválido",
            "FSW-0202":"Campo IDT inválido",
            "FSW-0401":"Não há informação disponível para esta ação/índice.",
            "FSW-0402":"Campo target inválido",
            "FSW-0404":"URL não encontrada",
            "FSW-0500":"Internal Server Error",
            "FSW-0400":"Bad Request"
        }

WS_URL = 'http://cotacoes.economia.uol.com.br/ws/asset/%s/intraday?size=500&page=1'


def geturl(url):
    print 'looking url ' + (url)
    content = urlopen(url).read()
    print 'load successful'
    return content

def main():
    
   wcompanies = quotedao.findall_watched_companies()
   for wc in wcompanies:
      company = wc['company_code']
      idt = wc['idt_num']
      data = geturl(WS_URL % idt)
      idtjson = simplejson.loads(data)
      
      err = idtjson.get('error')
      
      if err:
         logging.error('Unable to fetch data for campany %s: %s: %s' % (company.code, err, ERRORS[err]))
      else: 
         rows_num = quotedao.insert_quotes(company, idtjson['data'])
         if rows_num:
            print '%d quotes saved for company %s' % (rows_num, company)
         else: print 'no quotes saved for company %s' % (company)

if (__name__=='__main__'):
    main()

