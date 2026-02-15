/* ========================================================================
   PROJETO: Data Warehouse - Integração CRM & ERP
   AUTOR: [Agnaldo Gonzaga]
   PERFIL: Engenharia de Dados | Análise de Dados
   BANCO DE DADOS: SQL Server
   LINGUAGEM: T-SQL (DDL)

   ------------------------------------------------------------------------
   CONTEXTO DO PROJETO
   ------------------------------------------------------------------------
   Este projeto simula um cenário real de integração de dados entre 
   sistemas CRM e ERP, aplicando conceitos de Data Warehouse e 
   arquitetura Medallion (Bronze, Silver e Gold).

   Este script implementa a Camada Bronze (Raw Layer), responsável pela:
   - Ingestão de dados brutos
   - Estruturação inicial das tabelas
   - Padronização de nomenclaturas
   - Preparação para transformação posterior

   ------------------------------------------------------------------------
   BOAS PRÁTICAS APLICADAS
   ------------------------------------------------------------------------
    Organização por schema (bronze)
    Padronização de nomenclatura
    Separação por domínio (CRM / ERP)
    Uso de DROP IF EXISTS para controle de versionamento
    Estrutura preparada para evolução para modelo dimensional

   ------------------------------------------------------------------------
   TABELAS CRIADAS
   ------------------------------------------------------------------------
   CRM:
   - crm_cust_info        → Informações de clientes
   - crm_sales_details    → Detalhes de vendas
   - crm_prd_info         → Informações de produtos

   ERP:
   - erp_loc_a101         → Localização de clientes
   - erp_cust_az12        → Dados demográficos de clientes
   - erp_px_cat_g1v2      → Categorias e subcategorias de produtos

   ======================================================================== */



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

