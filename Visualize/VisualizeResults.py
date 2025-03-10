import pandas as pd
import matplotlib.pyplot as plt
import glob

csv_files = glob.glob("RangeReportResult.csv/part-*.csv")
df_list = [pd.read_csv(f) for f in csv_files]
df = pd.concat(df_list, ignore_index=True)

plt.scatter(df["X"], df["Y"])
plt.xlabel("X")
plt.ylabel("Y")
plt.title("Crimes in Specified Region and Time Range")
plt.show()
