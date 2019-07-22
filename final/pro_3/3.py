import gmplot
import webbrowser
import pandas as pd
from sodapy import Socrata
import geocoder
from scipy.spatial import distance

client = Socrata("data.melbourne.vic.gov.au", None)

results = client.get("dtpv-d4pf", limit=2000)

results_df = pd.DataFrame.from_records(results)

# latitude list, longitude list
lats, lons, status, lats2, lons2, = [], [], [], [], []
coords = []
temp, temp2 = [], []

for index, row in results_df.iterrows():
    status.append(row['status'])
    lats.append(float(row['lat']))
    lons.append(float(row['']))

# find the available parking space
for i in range(0, len(status)):
    if status[i] == "Unoccupied":
        lats2.append(lats[i])
        lons2.append(lons[i])

# my location
myloc = geocoder.ip('me')
cur, cur2 = myloc.lat, myloc.lng

# place map Melbourne
gmap = gmplot.GoogleMapPlotter(cur, cur2, 13)

# Scatter Drawing. parking spot
gmap.scatter(lats2, lons2, '#FF0000', size=20, marker=False)

# marker about current location
gmap.marker(cur, cur2, '#FFFF00', title="my location")

# Make coordinator list
for i in range(0, len(lats2)):
    temp.append(lats2[i])
    temp2.append(lons2[i])
    coords.append([temp[i], temp2[i]])
# print(coords)

loc = [[cur, cur2], ]

# calculate euclidean distance
e_dst = distance.cdist(loc, coords, "euclidean")
e_dst = e_dst.tolist()
idx, m_val, i = 0, 1000000, 0
for dst in e_dst[0]:
    if dst < m_val:
        m_val = dst
        idx = i
    i += 1

# marking the nearest parking spot
gmap.marker(coords[idx][0], coords[idx][1], '#FF00FF', title="The nearest parking spot")

# Draw
gmap.draw("parking_map.html")
# Run Browser
webbrowser.open_new("parking_map.html")