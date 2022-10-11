import pyspark.pandas as ps
import argparse

def get_codes(args):
    df_codes = ps.read_csv(f"{args.input_dir}/offense_codes.csv")
    df_codes["crime_type"] = df_codes["NAME"].apply(lambda x: x.split(" - ")[0])
    df_codes = df_codes.groupby("CODE", as_index=False).aggregate({
        "crime_type" : "first"
    })
    return df_codes
    
def get_crime(args):
    df_crime = ps.read_csv(f"{args.input_dir}/crime.csv")
    df_crime = df_crime.drop_duplicates()
    return df_crime

def calc(df_crime, df_codes):
    # merge crime with codes
    df_merged_codes = ps.merge(
        df_crime,
        df_codes,
        how="left",
        left_on="OFFENSE_CODE",
        right_on="CODE"
    )
    
    # calc crimes_total, lat, lng
    df_grouped = df_merged_codes.groupby(["DISTRICT"], as_index=False).agg({
        "INCIDENT_NUMBER": "count",
        "Lat": "mean",
        "Long": "mean"
    }).rename(columns={"DISTRICT":"district", "INCIDENT_NUMBER":"crimes_total", "Lat":"lat", "Long":"lng"})

    # calc frequent_crime_types
    df_distr_type = df_merged_codes.groupby(["DISTRICT", "crime_type"])["INCIDENT_NUMBER"].count().reset_index()
    df_top_crimes = df_distr_type.groupby(["DISTRICT"]).apply(
        lambda x: x.nlargest(3,["INCIDENT_NUMBER"])["crime_type"].str.cat(sep=", ")
    ).reset_index().rename(
        columns={"DISTRICT":"district", 0:"frequent_crime_types"}
    )

    # calc crimes_monthly
    df_distr_month = df_merged_codes.groupby(["DISTRICT", "YEAR", "MONTH"])["INCIDENT_NUMBER"].count().reset_index()[["DISTRICT", "INCIDENT_NUMBER"]]
    df_med_month = df_distr_month.groupby(["DISTRICT"]).median().reset_index().rename(columns={"DISTRICT":"district", "INCIDENT_NUMBER":"crimes_monthly"})

    # merge data
    df_merged = ps.merge(
        df_grouped,
        df_top_crimes,
        on="district",
    )
    df_full = ps.merge(
        df_merged,
        df_med_month,
        on="district",
    )
    
    return df_full

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="data")
    parser.add_argument("--output_dir", default="data/result")
    args = parser.parse_args()
    
    df_codes = get_codes(args)
    df_crime = get_crime(args)
    
    result = calc(df_crime, df_codes)
    
    result.to_parquet(f"{args.output_dir}", index=False)
    