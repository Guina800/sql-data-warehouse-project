/* ============================================================
   PROJETO: Data Warehouse – Arquitetura Medallion
   CAMADA: Bronze
   OBJETIVO:
       Realizar carga FULL dos dados brutos provenientes 
       dos sistemas CRM e ERP para a camada Bronze.

   ESTRATÉGIA:
       - TRUNCATE TABLE → Remove dados existentes antes da carga (Full Reload)
       - BULK INSERT → Carrega os arquivos CSV diretamente nas tabelas
       - Medição de tempo de execução para cada tabela e para o batch completo
       - Mensagens PRINT exibem progresso e duração de cada etapa

 OBSERVAÇÕES IMPORTANTES:
       - Arquivos CSV devem estar em diretório com permissão de leitura para o serviço SQL Server.
       - Diretórios utilizados no script:
           CRM: 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_crm\'
           ERP: 'D:\Area de trabalho\PJ DTWH\sql-data-warehouse-project\datasets\source_erp\'
       - Procedure bronze.load_bronze deve ser criada antes de ser executada.
       - Em caso de erro, o CATCH captura a mensagem, código e estado do erro, e interrompe a execução.
       - O script pode ser reutilizado para execução periódica da carga FULL na camada Bronze


============================================================ */
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

