import geopandas as gpd
import sqlalchemy
import shapely.geometry

# gdf = gpd.read_file("Mesh4_POP_00.shp", engine="pyogrio")
gdf = gpd.read_file("Mesh4_POP_16.shp", engine="pyogrio")

gdf.rename_geometry("geom", inplace=True)
gdf.to_crs(6668, inplace=True)

x_min, y_min = 137.011029079, 36.646053135
x_max, y_max = 137.180130220, 36.793910577
region = shapely.geometry.Polygon([(x_min, y_min), (x_max, y_min), (x_max, y_max), (x_min, y_max)])

gdf = gdf[gdf.intersects(region)]
print(gdf)

# # insert to db
engine = sqlalchemy.create_engine("postgresql://postgres:0@localhost:5432/postgres")
gdf.to_postgis("population", engine)
