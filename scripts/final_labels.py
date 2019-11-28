import pandas as pd

src_tie_csv = "./data/sources_3.csv"
src_csv = "/home/manoelribeiro/PycharmProjects/radicalization_data_collection/data/youtube/sources.csv"
df_tie_break = pd.read_csv("./data/sources_3.csv")
df_src = pd.read_csv(src_csv)

print("---> Agreement between original & NNet  <---")
df_tie_break["agreed_auto"] = df_tie_break["Annotator 2"].isnull()
print(df_tie_break.groupby("Category").sum()["agreed_auto"] / df_tie_break.groupby("Category").count()["agreed_auto"])


print("---> Agreement between annotators 1 & 2 <---")
df_tie_break = df_tie_break[~ df_tie_break["Annotator 2"].isnull()]
df_tie_break["agreed_anno"] = df_tie_break["Annotator 1"] == df_tie_break["Annotator 2"]
print(df_tie_break.groupby("Category").sum()["agreed_anno"] / df_tie_break.groupby("Category").count()["agreed_anno"])

df_src.set_index('Id', inplace=True)
df_src["Altered"] = False

for idx, v in df_tie_break.iterrows():
    v = dict(v)
    df_src.loc[v["Id"], "Category"] = v["Category Final"]
    df_src.loc[v["Id"], "Altered"] = True

df_src.sort_values("Category").to_csv("./data/sources_final.csv")
