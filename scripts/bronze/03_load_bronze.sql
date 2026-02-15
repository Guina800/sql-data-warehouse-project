/*
===============================================================================
PROJETO: Data Warehouse - Integração CRM & ERP
AUTOR: Agnaldo Gonzaga
DESCRIÇÃO: Stored Procedure para Carga da Camada BRONZE
===============================================================================
OBJETIVO:
    Este script automatiza o processo de ETL (Extract, Load) da camada Bronze.
    Ele extrai dados de arquivos CSV (arquivos fonte) e os carrega nas tabelas
    do SQL Server, garantindo que o estado atual da camada reflita os dados brutos.

FLUXO DE EXECUÇÃO:
    1. Truncate: Limpeza das tabelas bronze existentes (Full Load).
    2. Bulk Insert: Carga massiva de arquivos CSV localizados no repositório.
    3. Logging: Monitoramento do tempo de execução por tabela e total do lote.
    4. Error Handling: Tratamento de erros estruturado com TRY...CATCH.

PARÂMETROS:
    Nenhum. A procedure utiliza caminhos de diretório pré-definidos.

DEPENDÊNCIAS:
    - Arquivos CSV no diretório: ..\datasets\source_crm\ e ..\datasets\source_erp\
    - Tabelas criadas no schema 'bronze'.
===============================================================================
*/
EXEC bronze.load_bronze;


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '======================================================';
        PRINT 'Iniciando carga da camada Bronze';
        PRINT '======================================================';

        PRINT '------------------------------------------------------';
        PRINT 'Carregamento tabelas CRM';
        PRINT '------------------------------------------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncar Tabela: crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserindo Dados de: crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Duração do Carregamento: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -----------------------';

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncar Tabela: crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserindo Dados de: crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Duração do Carregamento: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -----------------------';

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncar Tabela: crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserindo Dados de: crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Duração do Carregamento: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -----------------------';

        PRINT '------------------------------------------------------';
        PRINT 'Carregamento tabelas ERP';
        PRINT '------------------------------------------------------';

        -- erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncar Tabela: erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserindo Dados de: erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Duração do Carregamento: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -----------------------';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncar Tabela: erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserindo Dados de: erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Duração do Carregamento: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -----------------------';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncar Tabela: erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserindo Dados de: erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Duração do Carregamento: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -----------------------';

        SET @batch_end_time = GETDATE();
        PRINT '======================================================';
        PRINT 'Carga da camada Bronze concluída com sucesso!';
        PRINT 'Duração total de carregamento: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' segundos';
        PRINT '======================================================';
    END TRY
    BEGIN CATCH
        PRINT 'Erro na carga da camada Bronze: ' + ERROR_MESSAGE();
        PRINT 'Erro ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Erro ' + CAST(ERROR_STATE() AS NVARCHAR);
        THROW;
    END CATCH;
END;
GO

