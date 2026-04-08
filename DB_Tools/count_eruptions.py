"VEMORA"
"Author: Alejandra Guerrero"
"Date: 2024-06-01"
import json
import pandas as pd
import matplotlib.pyplot as plt
from owslib.wfs import WebFeatureService

url = "https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows?service=WFS"

# ---------------------------------------------------------
# Download ALL Holocene eruptions
# ---------------------------------------------------------
def get_all_eruptions():
    wfs = WebFeatureService(url, version='1.1.0')
    response = wfs.getfeature(
        typename='GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions',
        outputFormat='json'
    )
    eruptions = json.load(response)
    return eruptions["features"]

# ---------------------------------------------------------
# Download ALL volcanoes
# ---------------------------------------------------------
def get_all_volcanoes():
    wfs = WebFeatureService(url, version='1.1.0')
    response = wfs.getfeature(
        typename='GVP-VOTW:Smithsonian_VOTW_Holocene_Volcanoes',
        outputFormat='json'
    )
    volcanoes = json.load(response)
    return volcanoes["features"]

# ---------------------------------------------------------
# Build DataFrame
# ---------------------------------------------------------
def build_dataframe():
    eruptions = get_all_eruptions()
    volcanoes = get_all_volcanoes()

    # Volcano ID → Name dictionary
    volcano_dict = {
        v["properties"]["Volcano_Number"]: v["properties"]["Volcano_Name"]
        for v in volcanoes
    }

    rows = []
    for e in eruptions:
        props = e["properties"]
        vid = props["Volcano_Number"]

        # Skip eruptions without year
        if props["StartDateYear"] is None:
            continue

        # Skip eruptions without numeric VEI
        vei = props["ExplosivityIndexMax"]
        if vei is None:
            continue
        if isinstance(vei, str):
            try:
                vei = float(vei)
            except:
                continue

        rows.append({
            "volcano_id": vid,
            "volcano_name": volcano_dict.get(vid, "Unknown"),
            "year": props["StartDateYear"],
            "VEI": vei
        })

    df = pd.DataFrame(rows)

    summary = df.groupby(["volcano_id", "volcano_name"]).agg(
        eruptions_count=("year", "count"),
        max_vei=("VEI", "max")
    ).reset_index()

    return summary

# ---------------------------------------------------------
# Plot
# ---------------------------------------------------------
def plot_eruptions(summary):
    summary_sorted = summary.sort_values("eruptions_count", ascending=False)

    # Count volcanoes with at least 20 eruptions (with numeric VEI)
    n20 = (summary_sorted["eruptions_count"] >= 20).sum()
    print(f"Volcanoes with at least 20 eruptions (with numeric VEI): {n20}")

    plt.figure(figsize=(14, 8))
    plt.bar(summary_sorted["volcano_name"], summary_sorted["eruptions_count"], color="firebrick")

    # Horizontal line at 20 eruptions
    plt.axhline(20, color="black", linestyle="--", linewidth=2, label="20 eruptions")

    plt.xticks(rotation=90, fontsize=3)  # smaller labels
    plt.ylabel("Number of eruptions")
    plt.xlabel("Volcano")
    plt.title("Holocene eruptions per volcano (numeric VEI only)")
    plt.legend()
    plt.tight_layout()
    plt.show()
def plot_top_volcanoes(summary):
    top = summary[summary["eruptions_count"] >= 20].sort_values("eruptions_count", ascending=False)

    plt.figure(figsize=(10, 6))
    plt.bar(top["volcano_name"], top["eruptions_count"], color="darkred")

    plt.xticks(rotation=90, fontsize=5.5)
    plt.ylabel("Number of eruptions")
    plt.xlabel("Volcano")
    plt.title("Volcanoes with ≥20 Holocene eruptions (numeric VEI only)")
    plt.tight_layout()
    plt.show()
# ---------------------------------------------------------
# Run
# ---------------------------------------------------------
summary = build_dataframe()
plot_eruptions(summary)
plot_top_volcanoes(summary)