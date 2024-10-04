from ..repository.plaft_acsele_vida_repository import (
    limpiar_temporal,
    procesar_mig_ubigeo,
    procesar_tbl_uni_vida
)
import pandas as pd
from ..utils.logger import logger


def plaft_obtener_acsele_vida_service():
    logger.info("plaft_obtener_acsele_vida - inicio")

    logger.info("plaft_obtener_acsele_vida - Limpiando tablas temporales")

    tablas = [
        "TBL_UNI_VIDA",
        "TMP_OP_VIDA",
        "TMP_OP_VIDA_MONTO",
        "MIG_ASEGURADO_VNO",
        "MIG_CONTRATANTE_VNO",
        "MIG_CONTRATANTE_VNO_J",
        "TBL_PERSONAS_VIDA",
        "TMP_UNI_BENEF",
        "MIG_BENEF_SIN",
        "RPT_FINAL_VIDA",
        "MIG_UBIGEO"
    ]
    for tabla in tablas:
        limpiar_temporal('interseguror.' + tabla)


    logger.info("plaft_obtener_acsele_vida - Insertando tablas temporales")
    #response = procesar_mig_ubigeo()
    response = procesar_tbl_uni_vida('25/09/2023');
    #logger.info("actualizacion_envio_acsele - Cruzando polizas Acsele x SME")
    # merged_df = pd.merge(
    #     dfAlloy,
    #     dfSme[["idproducto", "idpoliza", "idoperacion", "evento", "idenviosme"]],
    #     on=["idproducto", "idpoliza", "idoperacion", "evento"],
    #     how="inner",
    # )
    # merged_df = merged_df.rename(columns={"idenviosme_y": "idenviosme"})
    # merged_df = merged_df.drop("idenviosme_x", axis=1)
    # merged_df = merged_df.drop_duplicates(
    #     subset=["idproducto", "evento", "idpoliza", "idoperacion"]
    # )

    # logger.info("actualizacion_envio_acsele - Limpiando tablas temporales")
    # limpiar_temporal("interseguror.impmas_temp_envio")

    # logger.info("actualizacion_envio_acsele - Insertando polizas en temporal")
    # insertar_polizas_temporal(merged_df)

    # logger.info(
    #     "actualizacion_envio_acsele - Actualizando estado sme de las polizas Acsele"
    # )
    # updates = update_impmas_desde_temp()

    # response = {
    #     "polizas alloy": {"count": len(dfAlloy.to_dict(orient="records"))},
    #     "polizas sme": {"count": len(dfSme.to_dict(orient="records"))},
    #     "polizas mergeadas": {"count": len(merged_df.to_dict(orient="records"))},
    #     "polizas actualizadas": {"count": updates},
    # }
    # logger.info("actualizacion_envio_acsele - fin")
    return response