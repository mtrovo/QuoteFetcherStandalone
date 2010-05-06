import MySQLdb
import ConfigParser

__config = ConfigParser.RawConfigParser()
__config.read('quotefetcher.conf')

__INSERT_COMPANY = """INSERT INTO Company (idt_num, company_code, desc_name, complete_name, abv_name)
VALUES (%s, %s, %s, %s, %s)"""
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

def select_company(code):
    c = __db.cursor()
    
    c.execute("SELECT * FROM Company WHERE company_code = %s", (code))
    return c.fetchone()
    
    c.close()

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
        rows = c.fetchall()
        c.close()
    except:
        __db.rollback()
        c.close()
        raise
    return rows