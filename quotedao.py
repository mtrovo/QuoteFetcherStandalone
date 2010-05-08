import MySQLdb
import ConfigParser
from os import path

__config = ConfigParser.RawConfigParser()
__config.read(path.join(path.dirname(__file__), 'quotefetcher.conf'))

# INSERT COMPANY STATEMENT
__INSERT_COMPANY = """INSERT INTO Company (idt_num, company_code, desc_name, complete_name, abv_name)
VALUES (%s, %s, %s, %s, %s)"""

# INSERT QUOTE STATEMENT 
__INSERT_QUOTE = """INSERT INTO Quote (dat_timestamp,
   price,
   low,
   high,
   var,
   var_pct,
   vol, company_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

# FIND ALL WATCHED COMPANIES AS COMPANY
__FINDALL_WCOMPANIES = """SELECT A.* 
    FROM Company A, WatchedCompany B
    WHERE A.company_code = B.company_code"""

    

__db = MySQLdb.connect( host=__config.get('Database', 'host'),
                        user=__config.get('Database', 'user'),
                        passwd=__config.get('Database', 'passwd'),
                        db=__config.get('Database', 'db'))

def __transform_company(comp):
    ret = [comp['idt'], comp['code'], comp['name'], comp['companyName'], comp['companyAbvName']]
    #print repr(ret)
    return ret

# {"date":1273156380000,"price":30.44,"low":30.07,"high":30.78,"var":0.23,"varpct":0.76,"vol":5233913.0}    
def __transform_quote(q):
    ret = [q["date"], q["price"], q["low"], q["high"], q["var"], q["varpct"], q["vol"]]
    return ret

def __fetchoneDict(cursor):
    row = cursor.fetchone()
    if row is None: return None
    cols = [ d[0] for d in cursor.description ]
    return dict(zip(cols, row))
    
def __fetchallDict(cursor):
    rows = cursor.fetchall()
    if rows is None: return None
    cols = [ d[0] for d in cursor.description ]
    
    return [dict(zip(cols, row)) for row in rows]
    
def select_company(code):
    c = __db.cursor()
    
    c.execute("SELECT * FROM Company WHERE company_code = %s", (code))
    el = c.fetchone()

    c.close()
    return el

def select_quote(company_code, timestamp):
    c = __db.cursor()
    
    c.execute("SELECT * FROM Quote WHERE company_code = %s AND dat_timestamp = %s", (company_code, timestamp))
    el = c.fetchone()

    c.close()
    return el
    
    

def insert_companies(comps):
    allnew = [comp for comp in comps if not select_company(comp['code'])]
    mapped = map(__transform_company, allnew)
    
    c = __db.cursor()
    try:
        c.executemany(__INSERT_COMPANY, mapped)
        __db.commit()
        c.close()
        return len(mapped)
    except:
        __db.rollback()
        c.close()
        raise

def __concat_in_place(a, b):
    a.extend(b)
    return a

def insert_quotes(company_code, qs):
    allnew = [q for q in qs if not select_quote(company_code, q['date'])]
    mapped = map(__transform_quote, allnew)
    for el in mapped: 
        el.append(company_code)
    
    if not mapped or len(mapped) == 0: return 0
    
    c = __db.cursor()
    try:
        c.executemany(__INSERT_QUOTE, mapped)
        __db.commit()
        c.close()
        return len(mapped)
    except:
        __db.rollback()
        c.close()
        raise
    
def create_schema():
    schema = fopen('schema.sql').read()
    c = __db.cursor()
    try:
        c.execute(schema)
        c.close()
        return True
    except:
        __db.rollback()
        c.close()
        raise
        
def findall_watched_companies():
    c = __db.cursor()
    try:
        c.execute(__FINDALL_WCOMPANIES)
        rows = __fetchallDict(c)
        c.close()
    except:
        __db.rollback()
        c.close()
        raise
    return rows
