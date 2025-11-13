
import pandas as pd
import boto3
import geopandas as gpd


def loader_local_csv(file_path, sep=";"):
    return pd.read_csv(file_path, sep=sep)


def s3(access_key, secret_key, session_token):
    region = "fr-central"
    endpoint_url = "minio.data-platform-self-service.net"

    return boto3.client(
        "s3",
        endpoint_url="https://" + endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region,
    )


def loader_s3_csv(s3, file_path, sep=";"):

    bucket = "dlb-hackathon"
    response = s3.get_object(Bucket=bucket, Key=file_path)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        return pd.read_csv(response.get("Body"), sep=sep)

    else:
        raise "Erreur lors de la connection avec s3"


def loader_geoparquet(file_path: str) -> gpd.GeoDataFrame:

    try:
        gdf = gpd.read_parquet(file_path)
        if not isinstance(gdf, gpd.GeoDataFrame):
            raise ValueError("Le fichier n'a pas pu être lu comme GeoDataFrame.")
        return gdf
    except Exception as e:
        raise ValueError(f"Erreur lors de la lecture du fichier GeoParquet : {e}")
