from pandas.errors import EmptyDataError
from sqlitedict import SqliteDict
import pandas as pd
import datetime
import argparse
import swifter


def get_author_ids(comments_raw):
    author_ids = []
    comment_ids = []
    timestamps = []
    texts = []
    likes = []
    isreplies = []

    if type(comments_raw) != str:
        return author_ids, comment_ids, timestamps, texts, likes, isreplies
    else:
        comments_video = eval(comments_raw.replace("\0", ""))

    for comment in comments_video:
        try:
            author_ids.append(comment["authorLink"])
            comment_ids.append(comment["id"])
            timestamps.append(comment["timestamp"])
            texts.append(comment["text"])
            likes.append(comment["likes"])
            isreplies.append(False)
        except KeyError:
            pass

        if comment["hasReplies"] is False:
            continue

        try:
            for reply in comment["replies"]:
                try:
                    author_ids.append(reply["authorLink"])
                    comment_ids.append(comment["id"])
                    timestamps.append(reply["timestamp"])
                    texts.append(comment["text"])
                    likes.append(comment["likes"])
                    isreplies.append(True)
                except KeyError:
                    pass
        except KeyError:
            print(comment)
            pass

    return author_ids, comment_ids, timestamps, texts, likes, isreplies


parser = argparse.ArgumentParser(description="""This script generates two sqlite files that help throughout the analysis
                                                of the comments!""")

parser.add_argument("--src", dest="src", type=str, default="/dlabdata1/youtube_radicalization/cm/",
                    help="Folder with comment files (.gz'ed).")

parser.add_argument("--src_csv", dest="src_csv", type=str,
                    default="/dlabdata1/youtube_radicalization/sources_final_trimmed.csv",
                    help=".csv with rows `Name`, `Category`, `Data Collection step`, `Id`.")

parser.add_argument("--dst", dest="dst", type=str, default="/dlabdata1/youtube_radicalization/helpers2/",
                    help="Where to save the output files.")


forbidden_cats = ['NONE']
args = parser.parse_args()

print(args)

df_src = pd.read_csv(args.src_csv)


author_dict = dict()
channel_dict = dict()

for channel_id, category in list(zip(df_src["Id"], df_src["Category"]))[::-1]:

    if category in forbidden_cats:
        continue

    print("--------------------------------------------\n" * 1 + channel_id + ":" + category + "\n")

    now = datetime.datetime.now()
    count = 0
    try:
        chunk = pd.read_csv(args.src + channel_id + ".csv.gz", compression='gzip')
        print(count)
        count += 1

        for author_list, ids_list, timestamp_list, text_list, likes_list, isreplies_list in \
                list(chunk['comments'].swifter.apply(get_author_ids).values):
            for author, id_c, timestamp, text, likes, isreply in \
                    zip(author_list, ids_list, timestamp_list, text_list, likes_list, isreplies_list):


                dict_val = {"timestamp": timestamp, "channel_id": channel_id, "category": category,
                            "id": id_c}
                val = author_dict.get(author, [])
                val.append(dict_val)
                author_dict[author] = val


                dict_val = {"user_id": author, "timestamp": timestamp, "category": category,
                            "id": id_c}
                val = channel_dict.get(channel_id, [])
                val.append(dict_val)
                channel_dict[channel_id] = val

    except EmptyDataError:
        print("EmptyDataError:", channel_id)
        pass

    except FileNotFoundError:
        print("FileNotFoundError:", channel_id)
        pass

    print(datetime.datetime.now() - now)

import pickle
with open(args.dst + "/authors_dict.pickle", "wb") as dst_f:
    pickle.dump(author_dict, dst_f)

with open(args.dst + "/channel_dict.pickle", "wb") as dst_f:
    pickle.dump(channel_dict, dst_f)

