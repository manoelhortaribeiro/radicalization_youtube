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

parser.add_argument("--src", dest="src", type=str, default="./data/cm/",
                    help="Folder with comment files (.gz'ed).")

parser.add_argument("--src_csv", dest="src_csv", type=str, default="./data/sources_final.csv",
                    help=".csv with rows `Name`, `Category`, `Data Collection step`, `Id`.")

parser.add_argument("--dst", dest="dst", type=str, default="./data/helpers/",
                    help="Where to save the output files.")

parser.add_argument("--author_dict", dest="author_dict", action="store_false",
                    help="If present, runs author_dict, otherwise, runs for channel_dict")

forbidden_cats = ["PUA", "MGTOW", "NONE", "Incel", "MRA"]

args = parser.parse_args()

dst = args.dst + "authors_dict.sqlite" if args.author_dict else args.dst + "channel_dict.sqlite"
table_name = "authors" if args.author_dict else "channels"
df_src = pd.read_csv(args.src_csv)

dict_db = SqliteDict(dst, tablename=table_name, journal_mode="OFF")

dict_text = SqliteDict(args.dst + "text_dict.sqlite", tablename="text", journal_mode="OFF")

author_dict = dict()

for channel_id, category in list(zip(df_src["Id"], df_src["Category"]))[::-1]:

    if category in forbidden_cats:
        continue

    print("--------------------------------------------\n" * 3 + channel_id + ":" + category + "\n")
    channel_dict = dict()

    now = datetime.datetime.now()
    count = 0
    try:
        for chunk in pd.read_csv(args.src + channel_id + ".csv.gz", chunksize=250, compression='gzip'):
            print(count)
            count += 1

            for author_list, ids_list, timestamp_list, text_list, likes_list, isreplies_list in \
                    list(chunk['comments'].swifter.apply(get_author_ids).values):
                for author, id_c, timestamp, text, likes, isreply in \
                        zip(author_list, ids_list, timestamp_list, text_list, likes_list, isreplies_list):

                    if args.author_dict:  # -- updates authors_dict

                        dict_val = {"timestamp": timestamp, "channel_id": channel_id, "category": category,
                                    "id": id_c}
                        val = author_dict.get(author, [])
                        val.append(dict_val)
                        author_dict[author] = val

                    else:  # -- updates channel_dict

                        dict_val = {"user_id": author, "timestamp": timestamp, "category": category,
                                    "id": id_c}
                        val = channel_dict.get(channel_id, [])
                        val.append(dict_val)
                        channel_dict[channel_id] = val

                        dict_text[id_c] = {"likes": likes, "text": text, "is_reply": isreply, "timestamp": timestamp,
                                           "channel_id": channel_id, "category": category}

            if args.author_dict:
                for key, item in author_dict.items():
                    val = dict_db.get(key, [])
                    val += item
                    dict_db[key] = val
                dict_db.commit()
                author_dict = {}
            else:
                dict_db[channel_id] = channel_dict[channel_id]
                dict_db.commit()
                dict_text.commit()

    except EmptyDataError:
        print("EmptyDataError:", channel_id)
        pass

    except FileNotFoundError:
        print("FileNotFoundError:", channel_id)
        pass

    print(datetime.datetime.now() - now)

for key, item in author_dict.items():
    dict_db[key] = item
dict_text.commit()
dict_text.close()
dict_db.commit()
dict_db.close()
