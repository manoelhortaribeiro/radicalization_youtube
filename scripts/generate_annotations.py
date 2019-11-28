import pandas as pd
import argparse


parser = argparse.ArgumentParser(description="""Generate file for us to manually label.""")

parser.add_argument("--src", dest="src", type=str, default="./data/captions_seedsFalse_p100.csv",
                    help="Seeds to predict created by `generate_captions.py`")

parser.add_argument("--src_csv", dest="src_csv", type=str,
                    default="/home/manoelribeiro/PycharmProjects/" +
                            "radicalization_data_collection/data/youtube/sources.csv",
                    help=".csv with rows `Name`, `Category`, `Data Collection step`, `Id`.")

parser.add_argument("--prediction", dest="pred", type=str, default="./data/predictions.csv",
                    help="Where to save the output files.")

parser.add_argument("--dst", dest="dst", type=str, default="./data/sources_2.csv",
                    help="Where to save the output files.")

args = parser.parse_args()

df = pd.read_csv(args.src, usecols=["category", "channel_id", "chunk", "video_id"])

df["labels_text"] = pd.read_csv(args.pred)["predictions"]

df = df.groupby("channel_id").agg(lambda x: x.value_counts().index[0])

df_src = pd.read_csv(args.src_csv)
df_src["Category Caption"] = "None"
df_src["Category Final"] = "None"

df_src = df_src[(df_src['Data Collection step'] != "control") & (df_src['Data Collection step'] != "1")]

for idx, tmp_src in df_src.iterrows():
    tmp_src_dict = dict(tmp_src)
    channel_id = tmp_src_dict["Id"]
    try:
        tmp = dict(df.loc[channel_id])
        df_src.loc[idx, "Category Caption"] = tmp["labels_text"]
        if tmp["labels_text"] == tmp["category"]:
            df_src.loc[idx, "Category Final"] = tmp["labels_text"]

    except KeyError:
        print(channel_id)

df_src.to_csv(args.dst, index=False)
