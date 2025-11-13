import geopandas as gpd
from shapely.geometry import Point


def clean_name(name):
    name = name.strip()           # supprime espaces au début/fin
    name = name.replace(" ", "")  # supprime les espaces internes
    name = name.replace("-", "")  # supprime les tirets
    name = name.lower()           # met en minuscules

    return name


def transform_geopandas(df, geo_point_2d):

    df[["latitude", "longitude"]] = df[geo_point_2d].str.split(",", expand=True)

    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)

    df["geometry"] = df.apply(lambda row: Point(row["longitude"], row["latitude"]), axis=1)
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
