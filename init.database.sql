/*
=========================================
Criando o Banco de Dados e Esquemas
=========================================
Objetivo do Script:
Este script cria um novo banco de dados chamado 'DataWarehouse' após verificar se ele já existe. 
Se o banco de dados existir, ele será excluído (dropped) e recriado. 
Além disso, o script configura três esquemas dentro do banco de dados: 'bronze', 'silver' e 'gold'.

--------------------------------------------------------------------------------
 AVISO IMPORTANTE (WARNING)
--------------------------------------------------------------------------------
A execução deste script irá EXCLUIR (DROP) todo o banco de dados 'DataWarehouse' 
se ele já existir. Todos os dados serão permanentemente apagados. 
Certifique-se de ter backups antes de prosseguir.

*/

USE master;
GO

-- Drop e recriando o 'DataWarehouse' banco de dados
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse;
END;
GO

-- Criando o 'DataWarehouse' banco de dados
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

-- Criando Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA prata;
GO

CREATE SCHEMA ouro;
GO



