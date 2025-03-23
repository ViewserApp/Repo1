import os
import requests
import sys
from supabase import create_client, Client
from datetime import datetime, UTC
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# URL base de la API Jolpica F1
API_BASE_URL = "http://api.jolpi.ca/ergast/f1/2025"

def fetch_races():
    """Obtener el calendario de carreras de 2025, incluyendo datos de sprint"""
    try:
        response = requests.get(f"{API_BASE_URL}/races.json")
        if response.status_code == 429:
            print("Error 429: Too Many Requests al obtener carreras.")
            sys.exit(429)
        response.raise_for_status()
        data = response.json()["MRData"]["RaceTable"]["Races"]
    except requests.RequestException as e:
        print(f"Error al obtener carreras: {e}")
        return []
    
    races = []
    for race in data:
        race_date = datetime.strptime(race["date"], "%Y-%m-%d")
        fp1_date = race.get("FirstPractice", {}).get("date")
        fp1_time = race.get("FirstPractice", {}).get("time", "00:00:00Z")
        fp1 = f"{fp1_date}T{fp1_time}" if fp1_date else None
        
        fp2_date = race.get("SecondPractice", {}).get("date")
        fp2_time = race.get("SecondPractice", {}).get("time", "00:00:00Z")
        fp2 = f"{fp2_date}T{fp2_time}" if fp2_date else None
        
        fp3_date = race.get("ThirdPractice", {}).get("date")
        fp3_time = race.get("ThirdPractice", {}).get("time", "00:00:00Z")
        fp3 = f"{fp3_date}T{fp3_time}" if fp3_date else None
        
        qual_date = race.get("Qualifying", {}).get("date")
        qual_time = race.get("Qualifying", {}).get("time", "00:00:00Z")
        qual = f"{qual_date}T{qual_time}" if qual_date else None
        
        sprint_qual_date = race.get("Sprint", {}).get("date")
        sprint_qual_time = race.get("Sprint", {}).get("time", "00:00:00Z")
        sprint_qual = f"{sprint_qual_date}T{sprint_qual_time}" if sprint_qual_date else None
        
        sprint_race_date = race.get("Sprint", {}).get("date")
        sprint_race_time = race.get("Sprint", {}).get("time", "00:00:00Z")
        sprint_race = f"{sprint_race_date}T{sprint_race_time}" if sprint_race_date else None
        
        race_name = race.get("raceName") or "Unknown Race"
        season_year = 2025

        if not race_name:
            print(f"Advertencia: raceName no encontrado para una carrera: {race}")
            continue

        races.append({
            "race_name": race_name,
            "circuit_name": race["Circuit"]["circuitName"],
            "circuit_location": race["Circuit"]["Location"]["locality"],
            "circuit_country": race["Circuit"]["Location"]["country"],
            "race_date": race_date.isoformat(),
            "fp1_time": fp1,
            "fp2_time": fp2,
            "fp3_time": fp3,
            "qualifying_time": qual,
            "race_time": f"{race['date']}T{race['time']}",
            "sprint_qualifying_time": sprint_qual,
            "sprint_race_time": sprint_race,
            "season_year": season_year,
            "updated_at": datetime.now(UTC).isoformat()
        })
    return races

def fetch_sprint_results(round_number):
    """Obtener resultados de la carrera sprint para una ronda específica"""
    try:
        response = requests.get(f"{API_BASE_URL}/{round_number}/sprint.json")
        if response.status_code == 429:
            print("Error 429: Too Many Requests al obtener resultados de sprint.")
            sys.exit(429)
        if response.status_code != 200 or not response.json()["MRData"]["RaceTable"]["Races"]:
            print(f"No hay datos de sprint para la ronda {round_number}.")
            return []
        race_data = response.json()["MRData"]["RaceTable"]["Races"][0]
        race_name = race_data["raceName"]
        sprint_results_data = race_data.get("SprintResults", [])
    except requests.RequestException as e:
        print(f"Error al obtener resultados de sprint para ronda {round_number}: {e}")
        return []
    
    sprint_results = []
    for result in sprint_results_data:
        driver_code = result["Driver"]["code"]
        driver_id_response = supabase.table("drivers").select("id").eq("driver_code", driver_code).execute()
        if not driver_id_response.data:
            print(f"Piloto con código {driver_code} no encontrado en la tabla drivers.")
            continue
        driver_id = driver_id_response.data[0]["id"]
        
        race_id_response = supabase.table("calendar").select("id").eq("race_name", race_name).eq("season_year", 2025).execute()
        if not race_id_response.data:
            print(f"Carrera {race_name} no encontrada en la tabla calendar.")
            continue
        race_id = race_id_response.data[0]["id"]

        position = int(result.get("position", 0))
        if position <= 0:
            print(f"Posición inválida ({position}) para el piloto {driver_code} en la carrera sprint {race_name}. Saltando.")
            continue

        sprint_results.append({
            "driver_id": driver_id,
            "race_id": race_id,
            "position": position,
            "points": int(float(result.get("points", 0))),
            "team_id": supabase.table("teams").select("id").eq("team_name", result["Constructor"]["name"]).execute().data[0]["id"]
        })
    return sprint_results

def fetch_qualifying_results(round_number):
    """Obtener resultados de clasificación para una ronda específica"""
    try:
        response = requests.get(f"{API_BASE_URL}/{round_number}/qualifying.json")
        if response.status_code == 429:
            print("Error 429: Too Many Requests al obtener resultados de clasificación.")
            sys.exit(429)
        if response.status_code != 200 or not response.json()["MRData"]["RaceTable"]["Races"]:
            print(f"No hay datos de clasificación para la ronda {round_number}.")
            return []
        race_data = response.json()["MRData"]["RaceTable"]["Races"][0]
        race_name = race_data["raceName"]
        qualifying_results_data = race_data.get("QualifyingResults", [])
    except requests.RequestException as e:
        print(f"Error al obtener resultados de clasificación para ronda {round_number}: {e}")
        return []
    
    qualifying_results = []
    for result in qualifying_results_data:
        driver_code = result["Driver"]["code"]
        driver_id_response = supabase.table("drivers").select("id").eq("driver_code", driver_code).execute()
        if not driver_id_response.data:
            print(f"Piloto con código {driver_code} no encontrado en la tabla drivers.")
            continue
        driver_id = driver_id_response.data[0]["id"]
        
        race_id_response = supabase.table("calendar").select("id").eq("race_name", race_name).eq("season_year", 2025).execute()
        if not race_id_response.data:
            print(f"Carrera {race_name} no encontrada en la tabla calendar.")
            continue
        race_id = race_id_response.data[0]["id"]

        position = int(result.get("position", 0))
        if position <= 0:
            continue

        qualifying_results.append({
            "driver_id": driver_id,
            "race_id": race_id,
            "position": position
        })
    return qualifying_results

def fetch_drivers():
    """Obtener lista de pilotos"""
    try:
        response = requests.get(f"{API_BASE_URL}/drivers.json")
        if response.status_code == 429:
            print("Error 429: Too Many Requests al obtener pilotos.")
            sys.exit(429)
        response.raise_for_status()
        data = response.json()["MRData"]["DriverTable"]["Drivers"]
    except requests.RequestException as e:
        print(f"Error al obtener pilotos: {e}")
        return []
    return [{
        "driver_code": driver["code"],
        "first_name": driver["givenName"],
        "last_name": driver["familyName"],
        "nationality": driver["nationality"],
        "updated_at": datetime.now(UTC).isoformat()
    } for driver in data]

def fetch_teams():
    """Obtener lista de equipos"""
    try:
        response = requests.get(f"{API_BASE_URL}/constructors.json")
        if response.status_code == 429:
            print("Error 429: Too Many Requests al obtener equipos.")
            sys.exit(429)
        response.raise_for_status()
        data = response.json()["MRData"]["ConstructorTable"]["Constructors"]
    except requests.RequestException as e:
        print(f"Error al obtener equipos: {e}")
        return []
    return [{
        "team_name": constructor["name"],
        "nationality": constructor["nationality"],
        "updated_at": datetime.now(UTC).isoformat()
    } for constructor in data]

def fetch_standings():
    """Obtener clasificaciones de pilotos y equipos"""
    try:
        driver_response = requests.get(f"{API_BASE_URL}/driverStandings.json")
        if driver_response.status_code == 429:
            print("Error 429: Too Many Requests al obtener clasificaciones de pilotos.")
            sys.exit(429)
        driver_response.raise_for_status()
        team_response = requests.get(f"{API_BASE_URL}/constructorStandings.json")
        if team_response.status_code == 429:
            print("Error 429: Too Many Requests al obtener clasificaciones de equipos.")
            sys.exit(429)
        team_response.raise_for_status()
        
        driver_data = driver_response.json()["MRData"]["StandingsTable"]["StandingsLists"]
        team_data = team_response.json()["MRData"]["StandingsTable"]["StandingsLists"]
        print(f"Datos de driverStandings: {driver_data}")
        print(f"Datos de constructorStandings: {team_data}")

        driver_standings_list = driver_data[0]["DriverStandings"] if driver_data else []
        team_standings_list = team_data[0]["ConstructorStandings"] if team_data else []
    except requests.RequestException as e:
        print(f"Error al obtener clasificaciones: {e}")
        return [], []
    
    driver_standings = []
    for standing in driver_standings_list:
        if standing.get("positionText") == "-":
            print(f"Piloto {standing['Driver']['code']} no tiene posición definida. Saltando.")
            continue

        driver_code = standing["Driver"]["code"]
        driver_id_response = supabase.table("drivers").select("id").eq("driver_code", driver_code).execute()
        if not driver_id_response.data:
            print(f"Piloto con código {driver_code} no encontrado en la tabla drivers.")
            continue
        driver_id = driver_id_response.data[0]["id"]

        position = int(standing.get("position", 0))
        if position <= 0:
            print(f"Posición inválida ({position}) para el piloto {driver_code}. Saltando.")
            continue

        driver_standings.append({
            "driver_id": driver_id,
            "race_id": None,
            "position": position,
            "total_points": int(float(standing.get("points", 0))),
            "season_year": 2025,
            "updated_at": datetime.now(UTC).isoformat()
        })
    
    team_standings = []
    for standing in team_standings_list:
        if standing.get("positionText") == "-":
            print(f"Equipo {standing['Constructor']['name']} no tiene posición definida. Saltando.")
            continue

        team_name = standing["Constructor"]["name"]
        team_id_response = supabase.table("teams").select("id").eq("team_name", team_name).execute()
        if not team_id_response.data:
            print(f"Equipo {team_name} no encontrado en la tabla teams.")
            continue
        team_id = team_id_response.data[0]["id"]

        position = int(standing.get("position", 0))
        if position <= 0:
            print(f"Posición inválida ({position}) para el equipo {team_name}. Saltando.")
            continue

        team_standings.append({
            "team_id": team_id,
            "race_id": None,
            "position": position,
            "total_points": int(float(standing.get("points", 0))),
            "season_year": 2025,
            "updated_at": datetime.now(UTC).isoformat()
        })
    
    return driver_standings, team_standings

def fetch_race_results(round_number):
    """Obtener resultados de la carrera principal para una ronda específica"""
    try:
        response = requests.get(f"{API_BASE_URL}/{round_number}/results.json")
        if response.status_code == 429:
            print("Error 429: Too Many Requests al obtener resultados de carrera.")
            sys.exit(429)
        if response.status_code != 200 or not response.json()["MRData"]["RaceTable"]["Races"]:
            print(f"No hay datos de resultados para la ronda {round_number}.")
            return []
        race_data = response.json()["MRData"]["RaceTable"]["Races"][0]
        race_name = race_data["raceName"]
        results_data = race_data.get("Results", [])
    except requests.RequestException as e:
        print(f"Error al obtener resultados para ronda {round_number}: {e}")
        return []
    
    results = []
    for result in results_data:
        driver_code = result["Driver"]["code"]
        driver_id_response = supabase.table("drivers").select("id").eq("driver_code", driver_code).execute()
        if not driver_id_response.data:
            print(f"Piloto con código {driver_code} no encontrado en la tabla drivers.")
            continue
        driver_id = driver_id_response.data[0]["id"]
        
        race_id_response = supabase.table("calendar").select("id").eq("race_name", race_name).eq("season_year", 2025).execute()
        if not race_id_response.data:
            print(f"Carrera {race_name} no encontrada en la tabla calendar.")
            continue
        race_id = race_id_response.data[0]["id"]

        team_name = result["Constructor"]["name"]
        team_id_response = supabase.table("teams").select("id").eq("team_name", team_name).execute()
        if not team_id_response.data:
            print(f"Equipo {team_name} no encontrado en la tabla teams.")
            continue
        team_id = team_id_response.data[0]["id"]

        position = int(result.get("position", 0))
        if position <= 0:
            continue  # Saltar si no hay posición válida (por ejemplo, DNF)

        results.append({
            "driver_id": driver_id,
            "race_id": race_id,
            "team_id": team_id,
            "position": position,
            "points": int(float(result.get("points", 0))),
            "fastest_lap": result.get("FastestLap", {}).get("rank") == "1"
        })
    return results

def update_statistics(races):
    """Actualizar estadísticas de pilotos y equipos basadas en resultados de carreras, sprint y clasificación"""
    driver_stats = {}
    team_stats = {}

    # Procesar resultados de carreras principales
    for i, race in enumerate(races, 1):
        race_results = fetch_race_results(i)
        for result in race_results:
            driver_id = result["driver_id"]
            team_id = result["team_id"]
            position = result["position"]
            
            # Inicializar estadísticas del piloto si no existen
            if driver_id not in driver_stats:
                driver_stats[driver_id] = {
                    "race_wins": 0, "sprint_wins": 0, "podiums": 0, "poles": 0, 
                    "total_points": 0, "fastest_laps": 0
                }
            # Actualizar estadísticas del piloto
            driver_stats[driver_id]["total_points"] += result["points"]
            if position == 1:
                driver_stats[driver_id]["race_wins"] += 1
            if position <= 3:  # Solo carreras normales cuentan como podios
                driver_stats[driver_id]["podiums"] += 1
            if result["fastest_lap"]:
                driver_stats[driver_id]["fastest_laps"] += 1

            # Inicializar estadísticas del equipo si no existen
            if team_id not in team_stats:
                team_stats[team_id] = {
                    "race_wins": 0, "sprint_wins": 0, "podiums": 0, 
                    "total_points": 0, "fastest_laps": 0
                }
            # Actualizar estadísticas del equipo
            team_stats[team_id]["total_points"] += result["points"]
            if position == 1:
                team_stats[team_id]["race_wins"] += 1
            if position <= 3:  # Solo carreras normales cuentan como podios
                team_stats[team_id]["podiums"] += 1
            if result["fastest_lap"]:
                team_stats[team_id]["fastest_laps"] += 1

        # Procesar resultados de sprint
        sprint_results = fetch_sprint_results(i)
        for result in sprint_results:
            driver_id = result["driver_id"]
            team_id = result["team_id"]
            position = result["position"]
            
            if driver_id not in driver_stats:
                driver_stats[driver_id] = {
                    "race_wins": 0, "sprint_wins": 0, "podiums": 0, "poles": 0, 
                    "total_points": 0, "fastest_laps": 0
                }
            driver_stats[driver_id]["total_points"] += result["points"]
            if position == 1:
                driver_stats[driver_id]["sprint_wins"] += 1
            # No se cuentan podios en sprints

            if team_id not in team_stats:
                team_stats[team_id] = {
                    "race_wins": 0, "sprint_wins": 0, "podiums": 0, 
                    "total_points": 0, "fastest_laps": 0
                }
            team_stats[team_id]["total_points"] += result["points"]
            if position == 1:
                team_stats[team_id]["sprint_wins"] += 1
            # No se cuentan podios en sprints

        # Procesar resultados de clasificación
        qualifying_results = fetch_qualifying_results(i)
        for result in qualifying_results:
            driver_id = result["driver_id"]
            position = result["position"]
            
            if driver_id not in driver_stats:
                driver_stats[driver_id] = {
                    "race_wins": 0, "sprint_wins": 0, "podiums": 0, "poles": 0, 
                    "total_points": 0, "fastest_laps": 0
                }
            if position == 1:
                driver_stats[driver_id]["poles"] += 1

    # Actualizar driver_statistics en Supabase (solo generales)
    driver_standings, _ = fetch_standings()
    for driver_id, stats in driver_stats.items():
        try:
            existing = supabase.table("driver_statistics")\
                .select("id")\
                .eq("driver_id", driver_id)\
                .eq("season_year", 2025)\
                .is_("race_id", "null")\
                .execute()
            
            data = {
                "driver_id": driver_id,
                "race_id": None,
                "season_year": 2025,
                "race_wins": stats["race_wins"],
                "sprint_wins": stats["sprint_wins"],
                "podiums": stats["podiums"],
                "poles": stats["poles"],
                "total_points": stats["total_points"],
                "fastest_laps": stats["fastest_laps"],
                "position": next((s["position"] for s in driver_standings if s["driver_id"] == driver_id), 0),
                "updated_at": datetime.now(UTC).isoformat()
            }
            if existing.data:
                supabase.table("driver_statistics")\
                    .update(data)\
                    .eq("driver_id", driver_id)\
                    .eq("season_year", 2025)\
                    .is_("race_id", "null")\
                    .execute()
            else:
                supabase.table("driver_statistics").insert(data).execute()
        except Exception as e:
            print(f"Error al actualizar estadísticas del piloto {driver_id}: {e}")

    # Actualizar team_statistics en Supabase (solo generales)
    _, team_standings = fetch_standings()
    for team_id, stats in team_stats.items():
        try:
            existing = supabase.table("team_statistics")\
                .select("id")\
                .eq("team_id", team_id)\
                .eq("season_year", 2025)\
                .is_("race_id", "null")\
                .execute()
            
            data = {
                "team_id": team_id,
                "race_id": None,
                "season_year": 2025,
                "race_wins": stats["race_wins"],
                "sprint_wins": stats["sprint_wins"],
                "podiums": stats["podiums"],
                "total_points": stats["total_points"],
                "fastest_laps": stats["fastest_laps"],
                "position": next((s["position"] for s in team_standings if s["team_id"] == team_id), 0),
                "updated_at": datetime.now(UTC).isoformat()
            }
            if existing.data:
                supabase.table("team_statistics")\
                    .update(data)\
                    .eq("team_id", team_id)\
                    .eq("season_year", 2025)\
                    .is_("race_id", "null")\
                    .execute()
            else:
                supabase.table("team_statistics").insert(data).execute()
        except Exception as e:
            print(f"Error al actualizar estadísticas del equipo {team_id}: {e}")

def update_database():
    """Actualizar las tablas en Supabase"""
    try:
        # Actualizar calendario
        races = fetch_races()
        for race in races:
            try:
                existing = supabase.table("calendar")\
                    .select("id")\
                    .eq("race_name", race["race_name"])\
                    .eq("season_year", race["season_year"])\
                    .execute()
                
                if existing.data:
                    supabase.table("calendar")\
                        .update(race)\
                        .eq("race_name", race["race_name"])\
                        .eq("season_year", race["season_year"])\
                        .execute()
                else:
                    supabase.table("calendar").insert(race).execute()
            except Exception as e:
                print(f"Error al procesar carrera {race['race_name']}: {e}")
                raise

        print("Tabla calendar actualizada.")

        # Actualizar pilotos
        drivers = fetch_drivers()
        for driver in drivers:
            try:
                existing = supabase.table("drivers")\
                    .select("id")\
                    .eq("driver_code", driver["driver_code"])\
                    .execute()
                
                if existing.data:
                    supabase.table("drivers")\
                        .update(driver)\
                        .eq("driver_code", driver["driver_code"])\
                        .execute()
                else:
                    supabase.table("drivers").insert(driver).execute()
            except Exception as e:
                print(f"Error al procesar piloto {driver['driver_code']}: {e}")
                raise

        print("Tabla drivers actualizada.")

        # Actualizar equipos
        teams = fetch_teams()
        for team in teams:
            try:
                existing = supabase.table("teams")\
                    .select("id")\
                    .eq("team_name", team["team_name"])\
                    .execute()
                
                if existing.data:
                    supabase.table("teams")\
                        .update(team)\
                        .eq("team_name", team["team_name"])\
                        .execute()
                else:
                    supabase.table("teams").insert(team).execute()
            except Exception as e:
                print(f"Error al procesar equipo {team['team_name']}: {e}")
                raise

        print("Tabla teams actualizada.")

        # Actualizar estadísticas
        update_statistics(races)
        print("Tablas driver_statistics y team_statistics actualizadas.")

    except Exception as e:
        print(f"Error al actualizar la base de datos: {e}")
        raise

if __name__ == "__main__":
    update_database()
    print("Base de datos actualizada exitosamente.")