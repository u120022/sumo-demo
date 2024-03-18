import geopandas as gpd
import sqlalchemy

gdf = gpd.read_file("N03-20240101.shp", engine="pyogrio", encoding="utf-8")

gdf.rename_geometry("geom", inplace=True)
gdf.to_crs(6668, inplace=True)

# # insert to db
engine = sqlalchemy.create_engine("postgresql://postgres:0@localhost:5432/postgres")
gdf.to_postgis("city", engine)
