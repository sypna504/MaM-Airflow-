import pandas as pd

df = pd.DataFrame([
    {"file_name": "a.mp4", "status": "new", "last_size": 100},
    {"file_name": "b.mp4", "status": "ready", "last_size": 250},
    {"file_name": "bdasasf.mp4", "status": "ready", "last_size": 250},
    {"file_name": "c.mp4", "status": "done", "last_size": 300},
])
li = []
values = df.loc[df["status"] == "ready", "file_name"]
for v in values:
    li.append(v)
print(li)