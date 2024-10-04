from src.service.plaft_acsele_vida_service import plaft_obtener_acsele_vida_service

def main():
    resultado = plaft_obtener_acsele_vida_service()
    print("Resultado del proceso:", resultado)

if __name__ == "__main__":
    main()