#!/usr/bin/env python3

import os
import pandas as pd
from urllib.parse import quote

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

def main():

    excel_file = "topic_keywords.xlsx"
    language_dict_file = "language_dict.csv"
    division_language_mapping_file = "division_language_mapping.csv"

    TIMEFRAME = "2004-01-01 2025-09-01"
    TIMEFRAME_PARAM = "2004-01-01%202025-09-01"
    ANCHOR = "%2Fm%2F02nf_"  # email topic
    HL_PARAM = "en-US"      
    
    # language dict(iso2 -> full language name)
    lang_df = pd.read_csv(language_dict_file, dtype=str)
    valid_languages = set(lang_df["alpha_two"].str.strip().tolist())

    # division-language mapping
    division_lang_map = pd.read_csv(division_language_mapping_file, dtype=str)


    # load all topics from the excel file
    xls = pd.ExcelFile(excel_file)
    topics_data = {}
    for sheet_name in xls.sheet_names:
        df_topic = xls.parse(sheet_name=sheet_name, dtype=str)
        col_dict = {}
        for col in df_topic.columns:
            vals = [v.strip() for v in df_topic[col].dropna() if v.strip()]
            clean_vals = [v for v in vals if v != "[Error: invalid destination language]"] # languages not supported (couldn't be translated)
            col_dict[col.strip()] = clean_vals
        topics_data[sheet_name] = col_dict

    # for each division row, we get the list of iso2 languages from 'lang' column
    for _, row in division_lang_map.iterrows():
        division_iso = row["iso_two"].strip()
        division_name = row["division_name"].strip()
        if not division_iso:
            continue
        if pd.isna(row["lang"]):
            langs_for_division = []
        else:
            langs_for_division = [l.strip() for l in row["lang"].split(",")]

        if not langs_for_division:
            continue

        for lang_iso in langs_for_division:
            if lang_iso not in valid_languages:
                # fall back to english
                lang_iso = "en"

            # create directory for (division, language)
            lang_full = lang_df.loc[lang_df["alpha_two"] == lang_iso, "lang_name"].values[0]
            out_dir = f"data/{division_name}_{lang_full}/queries"
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            for topic_name, lang_dict in topics_data.items():
                # If language iso2 not in the topic's columns or no valid keywords, fallback to en
                if lang_iso not in lang_dict or not lang_dict[lang_iso]:
                    fallback_keywords = lang_dict.get("en", [])
                    keywords = fallback_keywords
                else:
                    keywords = lang_dict[lang_iso]

                if not keywords:
                    continue

                out_csv = os.path.join(out_dir, f"{division_iso}_{lang_iso}_{topic_name}_queries.csv")

                rows = []
                # chunk keywords in groups of 4 + anchor (email)
                for kw_group in chunk_list(keywords, 4):
                    quoted_keywords = ",".join([quote(k) for k in kw_group])
                    query_url = (
                        f"https://trends.google.com/trends/explore?"
                        f"date={TIMEFRAME_PARAM}&geo={division_iso}&q={quoted_keywords},{ANCHOR}&hl={HL_PARAM}"
                    )
                    rows.append([
                        division_iso,
                        topic_name,
                        "; ".join(kw_group), 
                        query_url
                    ])

                if rows:
                    df_out = pd.DataFrame(rows, columns=["division_iso_two","topic","keywords[list]","query url"])
                    df_out.to_csv(out_csv, index=False, encoding="utf-8")

if __name__ == "__main__":
    main()