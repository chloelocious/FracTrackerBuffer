import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable

shapefile_path = "/Users/chloelocious/Documents/GitHub/FracTrackerBuffer/Processed_Results/joined_36.shp"

gdf = gpd.read_file(shapefile_path)

print("\n Population Statistics:\n", gdf[["TotalPop", "Pop_Percen"]].describe())

print("\n First 20 rows of Population Fields:\n", gdf[["TotalPop", "Clp_SqKm", "Blk_SqKm", "Pop_Percen"]].head(20))

fig, ax = plt.subplots(figsize=(14, 10))
divider = make_axes_locatable(ax)
cax = divider.append_axes("right", size="5%", pad=0.1)

gdf.plot(column="Pop_Percen", cmap="coolwarm", 
         legend=True, edgecolor="gray", alpha=0.7,
         linewidth=0.2, ax=ax, cax=cax)

ax.set_title("Improved Population % in 1km Buffer Zone (Capped at 100%)", fontsize=16)
ax.set_xlabel("Longitude", fontsize=12)
ax.set_ylabel("Latitude", fontsize=12)
ax.set_xticks([])
ax.set_yticks([])
ax.set_frame_on(False)
plt.show()
