/*
===============================================================================
PROJETO: Data Warehouse - Integração CRM & ERP
AUTOR: Agnaldo Gonzaga
DESCRIÇÃO: Implementação da Camada BRONZE (Raw Layer)
===============================================================================
CONTEXTO:
    A Camada Bronze representa a 'Single Source of Truth' (Fonte Única da Verdade)
    dos dados brutos. Aqui, os dados são ingeridos "como estão" na origem, 
    garantindo a linhagem completa do dado.

BOAS PRÁTICAS APLICADAS:
    - Isolamento por Schema (bronze)
    - Persistência da estrutura original (Raw Data)
    - Scripts Idempotentes (DROP IF EXISTS) para automação CI/CD
    - Nomenclatura técnica preservada para facilitar o mapeamento com o ERP/CRM

MAPA DE DADOS (DOMÍNIOS):
    - CRM: Clientes (crm_cust_info), Vendas (crm_sales_details), Produtos (crm_prd_info)
    - ERP: Localização (erp_loc_a101), Demografia (erp_cust_az12), Categorias (erp_px_cat_g1v2)
===============================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(

cst_id INT,
cst_key NVARCHAR (50),
cst_firstname NVARCHAR (50),
cst_lastname NVARCHAR (50),
cst_material_status NVARCHAR (50),
cst_gndr NVARCHAR (50),
cst_create_date DATE 

);


IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(

sls_ord_num NVARCHAR (50),
sls_prd_key NVARCHAR (50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR (50),
prd_cost INT,
prd_line NVARCHAR (50),
prd_start_dt DATETIME,
prd_end_dt DATETIME

);


IF OBJECT_ID ('bronze.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;



CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
		
);



IF OBJECT_ID ('bronze.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
		
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;


CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR (50),
	subcat NVARCHAR (50),
	maintenance NVARCHAR (50)
	

);

