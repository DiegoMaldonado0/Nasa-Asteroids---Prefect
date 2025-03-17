import requests
import csv
from datetime import datetime, timedelta
from prefect import flow, task

API_KEY = "QCPKyEEU7U0D0he2zMFWvRQwsD2Cxf0kl3JVVaau"

@task
def obtener_asteroides():
    """Obtiene información de asteroides cercanos a la Tierra en la última semana."""
    fecha_actual = datetime.today().strftime('%Y-%m-%d')
    fecha_inicio = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={fecha_inicio}&end_date={fecha_actual}&api_key={API_KEY}"
    respuesta = requests.get(url)
    
    if respuesta.status_code == 200:
        datos = respuesta.json()
        asteroides = []
        
        for fecha, lista_asteroides in datos["near_earth_objects"].items():
            for asteroide in lista_asteroides:
                asteroides.append({
                    "nombre": asteroide["name"],
                    "fecha": fecha,
                    "diametro_min_km": asteroide["estimated_diameter"]["kilometers"]["estimated_diameter_min"],
                    "diametro_max_km": asteroide["estimated_diameter"]["kilometers"]["estimated_diameter_max"],
                    "velocidad_km_h": asteroide["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"],
                    "distancia_tierra_km": asteroide["close_approach_data"][0]["miss_distance"]["kilometers"]
                })
        return asteroides
    else:
        print("❌ Error al obtener datos.")
        return []

@task
def guardar_en_csv(asteroides):
    """Guarda los datos de los asteroides en un archivo CSV."""
    if not asteroides:
        print("⚠️ No hay datos para guardar.")
        return

    with open("asteroides.csv", mode="w", newline="", encoding="utf-8") as archivo:
        campos = ["nombre", "fecha", "diametro_min_km", "diametro_max_km", "velocidad_km_h", "distancia_tierra_km"]
        escritor = csv.DictWriter(archivo, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(asteroides)
    
    print("✅ Datos guardados en 'asteroides.csv'")

@flow(name="Flujo de Asteroides de la NASA")
def flujo_asteroides():
    """Flujo principal que obtiene y guarda asteroides cercanos."""
    asteroides = obtener_asteroides()
    guardar_en_csv(asteroides)

if __name__ == "__main__":
    flujo_asteroides()
