import geopandas as gpd
import sqlalchemy
import shapely.geometry

df0 = gpd.pd.read_csv("tblT000842H5437.csv")
df1 = gpd.pd.read_csv("tblT000842H5537.csv")

gdf0 = gpd.read_file("MESH05437.shp", engine="pyogrio")
gdf1 = gpd.read_file("MESH05537.shp", engine="pyogrio")

df = gpd.pd.concat([df0, df1])
gdf = gpd.pd.concat([gdf0, gdf1])

df = df.astype({ "KEY_CODE": "int32", "全産業事業所数": "int32", "全産業従業者数": "int32" })
gdf = gdf.astype({ "KEY_CODE": "int32",  })

gdf = gdf.merge(df, on="KEY_CODE")
gdf.rename_geometry("geom", inplace=True)
gdf.to_crs(6668, inplace=True)

x_min, y_min = 137.011029079, 36.646053135
x_max, y_max = 137.180130220, 36.793910577
region = shapely.geometry.Polygon([(x_min, y_min), (x_max, y_min), (x_max, y_max), (x_min, y_max)])

gdf = gdf[gdf.intersects(region)]
print(gdf)

# # insert to db
engine = sqlalchemy.create_engine("postgresql://postgres:0@localhost:5432/postgres")
gdf.to_postgis("employee", engine)
