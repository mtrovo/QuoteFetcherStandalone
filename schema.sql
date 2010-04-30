create table `Company` (
    company_code VARCHAR(10),
    idt_num SMALLINT(5) not null,
    abv_name VARCHAR(60),
    desc_name VARCHAR(60),
    complete_name VARCHAR(60),
    primary key (company_code)
);
        
create table `Quote`(
   company_code VARCHAR(10),
   dat_timestamp datetime,
   price NUMERIC(5,2),
   low NUMERIC(5,2),
   high NUMERIC(5,2),
   var NUMERIC(5,2),
   var_pct NUMERIC(5,2),
   vol NUMERIC(15,2),
   primary key (company_code, dat_timestamp),
   constraint fk_company
   foreign key fk_company (company_code)
   references Company(company_code)
);
    
create table `WatchedCompany`(
   company_code VARCHAR(10),
   primary key (company_code),   
   constraint fk_company
   foreign key fk_company (company_code)
   references Company(company_code)
);
