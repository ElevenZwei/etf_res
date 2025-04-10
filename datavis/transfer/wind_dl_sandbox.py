import datetime
import pandas as pd 

from WindPy import w
import wind_dl

# opt_names = wind_dl.dl_opt_names("510500.SH", "2025-04-02")
# opt_names.to_csv("opt_names.csv", index=False)

df = wind_dl.wind2df(w.tdays("2025-01-20", "2025-02-10", ""))
print(df)