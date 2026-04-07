import pandas as pd

def parse_list_column(cell_value: str) -> list[str]:
    if pd.isna(cell_value):
        return []

    text = str(cell_value).strip()

    for sep in [";", "|", "/"]:
        text = text.replace(sep, ",")

    return [item.strip() for item in text.split(",") if item.strip()]

def get_unique_organs(df: pd.DataFrame) -> list[str]:
    organ_set = set()

    for val in df["organs"]:
        for organ in parse_list_column(val):
            organ_set.add(organ.lower())  # normalize

    return sorted(organ_set)

def filter_by_organ(df: pd.DataFrame, chosen_organ: str) -> pd.DataFrame:
    def contains_organ(cell):
        organs = [o.lower() for o in parse_list_column(cell)]
        return chosen_organ.lower() in organs

    return df[df["organs"].apply(contains_organ)].copy()


def summarize_results(df: pd.DataFrame):
    if df.empty:
        print("\nNo matches found.")
        return

    missions = sorted(df["mission"].dropna().unique())
    mice = sorted(df["mouse_id"].dropna().unique())
    assays = sorted(
        set(
            a
            for val in df["assay_names"].dropna()
            for a in parse_list_column(val)
        )
    )

    print("\nRESULTS")

    print("\nMissions:")
    for m in missions:
        print(f"  - {m}")

    print("\nMice:")
    for m in mice:
        print(f"  - {m}")

    print("\nAssays:")
    for a in assays:
        print(f"  - {a}")

    print(f"\nTotal rows: {len(df)}")

def summarize_by_mission(df: pd.DataFrame):
    print("\nBY MISSION")

    for mission, group in df.groupby("mission"):
        mice = sorted(group["mouse_id"].unique())

        assays = sorted(
            set(
                a
                for val in group["assay_names"].dropna()
                for a in parse_list_column(val)
            )
        )

        print(f"\nMission: {mission}")
        print(f"  Mice ({len(mice)}): {', '.join(mice)}")
        print(f"  Assays: {', '.join(assays)}")

def main():
    input_csv = "mouse_ranking.csv"

    df = pd.read_csv(input_csv)

    required = ["mouse_id", "mission", "organs", "assay_names"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
        
    organs = get_unique_organs(df)

    print("\nAvailable organs:\n")
    for i, organ in enumerate(organs, 1):
        print(f"{i}. {organ}")

    chosen = input("\nEnter an organ: ").strip().lower()

    filtered = filter_by_organ(df, chosen)

    summarize_results(filtered)
    summarize_by_mission(filtered)

    save = input("\nSave results? (y/n): ").strip().lower()
    if save == "y":
        filename = f"{chosen}_filtered.csv"
        filtered.to_csv(filename, index=False)
        print(f"Saved to {filename}")


if __name__ == "__main__":
    main()
