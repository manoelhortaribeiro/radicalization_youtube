from pandas.errors import EmptyDataError
import pandas as pd
import argparse
import random


parser = argparse.ArgumentParser(description="""This script generates the captions for training the neural network.""")

parser.add_argument("--src", dest="src", type=str,
                    default="/home/manoelribeiro/PycharmProjects/" +
                            "radicalization_data_collection/data/youtube/cp/",
                    help="Source folder created by `loader_captions.py`.")

parser.add_argument("--src_csv", dest="src_csv", type=str,
                    default="/home/manoelribeiro/PycharmProjects/" +
                            "radicalization_data_collection/data/youtube/sources.csv",
                    help=".csv with rows `Name`, `Category`, `Data Collection step`, `Id`.")

parser.add_argument("--dst", dest="dst", type=str, default="./data/",
                    help="Where to save the output files.")

parser.add_argument("--name", dest="name", type=str, default="seeds_captions",
                    help="Name of the output file.")

parser.add_argument("--p", dest="p", type=int, default=10,
                    help="Percentage of captions to get.")

parser.add_argument("--max_length", dest="max_length", type=int, default=1500,
                    help="Maximum number of words per chunk.")

parser.add_argument("--min_length", dest="min_length", type=int, default=250,
                    help="Minimum number of words per chunk.")

parser.add_argument("--max_chunks", dest="max_chunks", type=int, default=10,
                    help="Maximum number of chunks per video.")

parser.add_argument("--all", dest="seeds", action="store_false",
                    help="If present, runs script for all captions, otherwise, runs only for seeds.")

args = parser.parse_args()


df_src = pd.read_csv(args.src_csv)

df_src = df_src[df_src["Data Collection step"] != "control"]

# Choose seeds
if args.seeds:
    df_src = df_src[df_src["Data Collection step"] == "1"]
else:
    df_src = df_src[df_src["Data Collection step"] != "1"]

df_list = []


for channel_id, category in zip(df_src["Id"], df_src["Category"]):
    try:
        # iterate through videos of each of the channels
        for v in pd.read_csv(args.src + channel_id + ".csv").iterrows():

            # if p != 100, randomly skip lines
            if random.random() > args.p/100:
                continue

            dict_v = dict(v[1])
            dict_v["channel_id"] = channel_id
            dict_v["category"] = category

            # ignore captions of size < args.min_length
            if type(dict_v["captions"]) != str or len(dict_v["captions"].split(" ")) < args.min_length:
                continue

            # iterate through chunks of size args.max_length
            for idx, nbr in enumerate(list(range(0, len(dict_v["captions"].split()), args.max_length))):

                if idx > args.max_chunks:
                    break
                else:
                    captions_chunk = " ".join(dict_v["captions"].split()[nbr:nbr + args.max_length])

                    # ignore trailing chunks with size < args.min_length
                    if len(captions_chunk) < args.min_length:
                        continue
                    else:
                        dict_chunk = dict(**dict_v)
                        dict_chunk["captions"] = captions_chunk
                        dict_chunk["chunk"] = idx
                        df_list.append(dict_chunk)

    except EmptyDataError:
        print(channel_id)

pd.DataFrame(df_list).to_csv(args.dst + "captions_seeds{}_p{}.csv".format(args.seeds, args.p), index=False)
