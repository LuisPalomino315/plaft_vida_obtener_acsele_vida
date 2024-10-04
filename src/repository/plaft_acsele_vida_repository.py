from ..utils.database_executes import execute_query_to_df,bulk_insert_from_df_pipe,bulk_insert_from_df_coma,execute_query_no_results,execute_query_with_results
from ..utils.logger import logger
import pandas as pd

def limpiar_temporal(tabla):
    logger.info('delete_temp_ - inicio')
    execute_query_no_results("TRUNCATE TABLE " + tabla + ";",'pg')
    logger.info(f'limpiar_temporal {tabla} - fin')
    return True

def procesar_mig_ubigeo():
    logger.info('procesar_mig_ubigeo - inicio')
    retorno = False;
    try:
        logger.info('procesar_mig_ubigeo - obtener dataframe')
        query = """
        SELECT PROP.SIMBOLO, TRANS.TTL_NAME AS CODIGO, TRANS.TTL_VALUE AS DESCRIPCION
        FROM INTERSEGURO_GU.CDTR_TRANSFORMERTRANSLATION TRANS
        INNER JOIN INTERSEGURO.TRANSFORMADORFILA TF
        ON TF.TRANSFORMADORFILAID = cast(NULLIF(TRANS.TRF_ID,'') as DECIMAL(19,0)) 
        AND TRANS.TTL_LANGUAGE = '(d)'
        INNER JOIN INTERSEGURO.PROPERTY PROP
        ON PROP.PROPERTYID = TF.PROPERTYID
        AND PROP.SIMBOLO IN('CodDepartamento','CodProvincia','CodDistrito');
        """
        df = execute_query_to_df(query, 'pg')
        logger.info(f"procesar_mig_ubigeo - => Datos: {len(df)}")
        logger.info('procesar_mig_ubigeo - insert bulk inicio')
        columns = ('simbolo', 'codigo', 'descripcion')
        bulk_insert_from_df_pipe(df, 'mig_ubigeo','interseguror',columns,'pg')
        logger.info('procesar_mig_ubigeo - insert bulk fin')
        retorno = True;
    except Exception as e:
        logger.error(f"Error en procesar_mig_ubigeo: {str(e)}")
        retorno = False;
    logger.info('procesar_mig_ubigeo - fin')
    return retorno

def procesar_tbl_uni_vida(fecha):
    logger.info('procesar_tbl_uni_vida - inicio')
    retorno = False;
    try:
        logger.info('procesar_tbl_uni_vida - obtener dataframe')
        query = f"""
        SELECT * FROM(
        SELECT
        ROW_NUMBER() OVER(PARTITION BY  PREP.STATIC  ORDER BY CTX.TIME_STAMP desc,CTX.ID DESC) AS NRO,
        CTX.ID,
        CTX.ITEM,
        ST.DESCRIPTION AS ESTADO
        FROM   INTERSEGURO.PREPOLICY PREP
        INNER JOIN INTERSEGURO.POLICYDCO PDCO ON PREP.PK = PDCO.DCOID
        INNER JOIN INTERSEGURO.CONTEXTOPERATION CTX  ON CTX.ID = PDCO.OPERATIONPK
        INNER JOIN INTERSEGURO.STATE ST ON ST.STATEID = PDCO.STATEID
        INNER JOIN INTERSEGURO.AGREGATEDPOLICY AP ON CTX.ITEM = AP.AGREGATEDPOLICYID
        INNER JOIN INTERSEGURO.PRODUCT PRO ON PRO.PRODUCTID = AP.PRODUCTID
        INNER JOIN INTERSEGURO.PRODUCTPROPERTY PRE ON PRE.PRO_ID = PRO.PRODUCTID
        WHERE PRE.PRP_TYPE = 2 AND CTX.STATUS = 2 AND CTX.TIME_STAMP <  to_timestamp('' || '{fecha}' || '','DD/MM/YYYY')
        ) AS TABAL WHERE NRO = 1 AND ESTADO IN('Vigente','Saldada','Prorrogado')
        """
        df = execute_query_to_df(query, 'pg')
        logger.info(f"procesar_tbl_uni_vida - => Datos: {len(df)}")
        logger.info('procesar_tbl_uni_vida - insert bulk inicio')
        columns = ('nro', 'id', 'item', 'estado')
        bulk_insert_from_df_pipe(df, 'tbl_uni_vida','interseguror',columns,'pg')
        logger.info('procesar_tbl_uni_vida - insert bulk fin')
        retorno = True;
    except Exception as e:
        logger.error(f"Error en procesar_tbl_uni_vida: {str(e)}")
        retorno = False;
    logger.info('procesar_tbl_uni_vida - fin')
    return retorno