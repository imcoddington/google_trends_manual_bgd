#!/usr/bin/env python3

import os
import glob
import pandas as pd


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=lambda col: col if col == "Month" else col.split(":")[0].strip())

def clean_col_name(col: str) -> str:
    return col if col == "Month" else col.split(":")[0].strip()


def merge_raw_data(csv_files):
    """
    merge all raw files by outer-joining on 'Month' without ratio linking.
    """
    merged_df = None

    for i, fpath in enumerate(csv_files):
        df = pd.read_csv(fpath, skiprows=2, encoding="utf-8")

        df.replace("<1", 0, inplace=True)
        df = rename_columns(df) 
        keyword_cols = [c for c in df.columns if c != "Month"]
        df[keyword_cols] = df[keyword_cols].apply(pd.to_numeric, errors="coerce")

        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(
                merged_df, df,
                on="Month", how="outer",
                suffixes=("", f"_file{i}")
            )
    return merged_df


def merge_ratio_linked_data(csv_files):
    """
    merge all raw files in by outer-joining on 'Month'and applying ratio-based linking using an anchor keyword.
    """

    all_keyword_sets = []
    for fpath in csv_files:
        df_header = pd.read_csv(fpath, header=None, skiprows=1, nrows=1, encoding="utf-8")
        
        columns_in_file = [clean_col_name(col) for col in df_header.values.flatten().tolist()]
        if columns_in_file[0] == "Month":
            all_keyword_sets.append(set(columns_in_file[1:]))
        else:
            all_keyword_sets.append(set(columns_in_file))

    common_keywords = set.intersection(*all_keyword_sets)
    if len(common_keywords) == 0:
        print("No common keywords found across all files., skipping this topic.")
        return None, None

    anchor_keyword = list(common_keywords)[0]

    combined_df = None
    for i, fpath in enumerate(csv_files):
        df = pd.read_csv(fpath, skiprows=2, encoding="utf-8")
        df.replace("<1", 0, inplace=True)
        df = rename_columns(df)  
        keyword_cols = [c for c in df.columns if c != "Month"]
        df[keyword_cols] = df[keyword_cols].apply(pd.to_numeric, errors="coerce")

        if anchor_keyword not in df.columns:
            print(f"Anchor keyword '{anchor_keyword}' not found in file: {fpath}. Skipping this topic.")
            return None, anchor_keyword

        if combined_df is None:
            combined_df = df.copy()
        else:
            temp_merge = pd.merge(
                combined_df[["Month", anchor_keyword]],
                df[["Month", anchor_keyword]],
                on="Month",
                how="inner",
                suffixes=("_combined", "_new")
            )

            if temp_merge.empty:
                print(f"Merge resulted in empty data for file: {fpath}. Skipping this topic.")
                scale_factor = 1
            else:
                avg_combined = temp_merge[f"{anchor_keyword}_combined"].mean()
                avg_new = temp_merge[f"{anchor_keyword}_new"].mean()

                if avg_new == 0:
                    scale_factor = 1
                else:
                    scale_factor = avg_combined / avg_new

            # scale the entire new df by that factor
            for col in keyword_cols:
                df[col] = df[col] * scale_factor

            # merge with previous combined_df
            combined_df = pd.merge(
                combined_df, df,
                on="Month", how="outer",
                suffixes=("", "_new")
            )

            for col in list(combined_df.columns):
                if col.endswith("_new"):  
                    base_col = col.replace("_new", "")
                    if base_col in combined_df.columns:
                        combined_df[base_col] = (
                            combined_df[base_col].fillna(0)
                            + combined_df[col].fillna(0)
                        )
                        combined_df.drop(columns=[col], inplace=True)
                    else:
                        combined_df.rename(columns={col: base_col}, inplace=True)

    return combined_df, anchor_keyword


def main():
    root_data_dir = "data"
    if not os.path.isdir(root_data_dir):
        print(f"Data directory '{root_data_dir}' not found. Exiting.")
        return

    # traverse all country_lang dirs inside data/
    country_lang_dirs = [
        d for d in os.listdir(root_data_dir)
        if os.path.isdir(os.path.join(root_data_dir, d))
    ]
    
    print(f"Found {len(country_lang_dirs)} country_language directories.")

    for cl_dir in country_lang_dirs:
        cl_path = os.path.join(root_data_dir, cl_dir)
        
        topic_dirs = [
            d for d in os.listdir(cl_path)
            if os.path.isdir(os.path.join(cl_path, d))
        ]

        if not topic_dirs:
            print(f"No topic subdirectories found in {cl_dir}. Skipping.")
            continue

        for topic_subdir in topic_dirs:
            # Full path of the topic directory
            topic_path = os.path.join(cl_path, topic_subdir)
            
            # Gather all multiTimeline CSV files
            csv_files = glob.glob(os.path.join(topic_path, "multiTimeline*.csv"))
            if not csv_files:
                print(f"Topic '{topic_subdir}' in {cl_dir} has no multiTimeline CSVs. Skipping.")
                continue

            print(f"Found {len(csv_files)} files for country_language='{cl_dir}', topic='{topic_subdir}'.")

            # 1) Merge raw (no ratio linking)
            merged_raw_df = merge_raw_data(csv_files)
            if merged_raw_df is not None and not merged_raw_df.empty:
                raw_outfile = os.path.join(
                    cl_path,
                    f"{topic_subdir}_{cl_dir}_raw.csv"  # e.g. "Environment_US_en_raw.csv"
                )
                merged_raw_df.to_csv(raw_outfile, index=False, encoding="utf-8")
                print(f"Saved raw merged data to {raw_outfile}")
            else:
                print(f"Raw merge for topic '{topic_subdir}' in {cl_dir} resulted in empty data. Skipping raw output.")

            # 2) Merge with ratio linking
            merged_adjusted_df, anchor_keyword = merge_ratio_linked_data(csv_files)
            if merged_adjusted_df is None:
                # This means either no anchor or the anchor wasn't found in all files
                print(
                    f"Ratio linking failed for topic '{topic_subdir}' in {cl_dir}. "
                    f"Possible missing common keyword across all files."
                )
                continue

            if not merged_adjusted_df.empty:
                adjusted_outfile = os.path.join(
                    cl_path,
                    f"{topic_subdir}_{cl_dir}_adjusted.csv"
                )
                merged_adjusted_df.to_csv(adjusted_outfile, index=False, encoding="utf-8")
                print(f"Saved ratio-linked merged data to {adjusted_outfile}. Anchor: '{anchor_keyword}'")
            else:
                print(f"Adjusted merge for topic '{topic_subdir}' in {cl_dir} is empty. Skipping adjusted output.")


if __name__ == "__main__":
    main()