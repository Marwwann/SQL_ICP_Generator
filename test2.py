import pandas as pd

df = pd.read_csv('D:/weewe.csv')

list1 = df["reporting_name"].values.flatten().tolist()

print(list1)