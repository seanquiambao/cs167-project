import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("RangeReportResult.csv/part-00000-...csv")
plt.scatter(df["X"], df["Y"])
plt.xlabel("X")
plt.ylabel("Y")
plt.title("Crimes in Specified Region and Time Range")
plt.show()
