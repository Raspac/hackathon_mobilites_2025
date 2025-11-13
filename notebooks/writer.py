

def write_geoparquet(gdf, file_path):
    try:
        gdf.to_parquet(file_path)
        print(f"✅ GeoParquet écrit avec succès dans '{file_path}'")
    except Exception as e:
        raise ValueError(f"Erreur lors de l’écriture du GeoParquet : {e}")
