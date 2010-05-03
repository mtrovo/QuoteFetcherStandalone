import MySQLdb

__INSERT_COMPANY = """INSERT INTO Company (idt_num, company_code, desc_name, complete_name, abv_name)
VALUES (%d, %s, %s, %s, %s)"""
__FINDALL_WCOMPANIES = """SELECT A.* 
    FROM Company A, WatchedCompany B
    WHERE A.company_code = B.company_code"""

__db = MySQLdb.connect(host="mysql.mtrovo.dreamhosters.com",user="mtrovo",
                  passwd="123098",db="mtrovo_quotefetcher")

def __transform_company(comp):
    return [comp['idt'], comp['code'], comp['name'], comp['companyName'], comp['companyAbvName']]

def select_company(code):
    c = __db.cursor()
    
    c.execute("SELECT * FROM Company WHERE company_code = %s" % code)
    return c.fetchone()
    
    c.close()

def insert_companies(comps):
    allnew = [comp for comp in comps if not selectCompany(comp.['code'])]
    mapped = map(comps, transform_company)
    
    c = __db.cursor()
    try:
        c.executemany(__INSERT_COMPANY, mapped)
        db.commit()
        c.close()
        return len(mapped)
    except:
        db.rollback()
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
        db.rollback()
        c.close()
        raise
        
def findall_watched_companies():
    c = __db.cursor()
    try:
        c.execute(__FINDALL_WCOMPANIES)
        rows = c.fetchall()
        c.close()