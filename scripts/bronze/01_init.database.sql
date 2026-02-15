/*
===============================================================================
PROJETO: Data Warehouse - Integração CRM & ERP
AUTOR: Agnaldo Gonzaga
DESCRIÇÃO: Setup Inicial do Ambiente de Dados
===============================================================================
OBJETIVO:
    Este script é o ponto de partida do projeto. Ele realiza a limpeza de 
    instâncias anteriores e prepara a fundação do Data Warehouse.

AÇÕES REALIZADAS:
    1. Verifica a existência e remove o banco 'DataWarehouse'.
    2. Cria o banco de dados 'DataWarehouse'.
    3. Define as camadas da Medallion Architecture (Bronze, Silver, Gold).

AVISO:
    A execução deste script é DESTRUTIVA. Ele apaga todos os dados existentes
    no banco 'DataWarehouse' antes de recriá-lo.
===============================================================================
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



