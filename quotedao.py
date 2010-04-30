import MySQLdb

__INSERT_COMPANY = """INSERT INTO Company (idt_num, company_code, desc_name, complete_name, abv_name)
VALUES (%d, %s, %s, %s, %s)"""
__db = MySQLdb.connect(host="mysql.mtrovo.dreamhosters.com",user="mtrovo",
                  passwd="123098",db="mtrovo_quotefetcher")

def __transform_company(comp):
    return [comp['idt'], comp['code'], comp['name'], comp['companyName'], comp['companyAbvName']]

def selectCompany(code):
    c = __db.cursor()
    
    c.execute("SELECT * FROM Company WHERE company_code = %s" % code)
    return c.fetchone()
    
    c.close()

def insertCompanies(comps):
    allnew = [comp for comp in comps if not selectCompany(comp.['code'])]
    mapped = map(comps, transform_company)
    
    c = __db.cursor()
    try:
        c.executemany(__INSERT_COMPANY, mapped)
        db.commit()
        return len(mapped)
    except:
        db.rollback()
        raise
    c.close()
