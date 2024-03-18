import geopandas as gpd
import sqlalchemy

gdf = gpd.read_file("Mesh4_POP_00.shp", engine="pyogrio")

gdf.rename_geometry("geom", inplace=True)
gdf.to_crs(6668, inplace=True)

# # insert to db
engine = sqlalchemy.create_engine("postgresql://postgres:0@localhost:5432/postgres")
gdf.to_postgis("population", engine)
