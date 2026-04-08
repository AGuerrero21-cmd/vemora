#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 13:08:21 2022

@author: aleja
"""
import csv
import SQLite_connection as mdb
#Reads the Smithsonian DB of eruptions and returns year, and magnitude arrays


# ----------------------------
# Helpers
# ----------------------------

def to_int(x):
    if x is None or x.strip() == "":
        return None
    return int(x)

def to_float(x):
    """Convert comma-decimal string to float."""
    if x is None or x.strip() == "":
        return None
    return float(x.replace(",", "."))

def parse_volume(line):
    tags = [
        "PYROCLAST_VOLUME_(KM3)",
        "LAVA_VOLUME_(KM3)",     
        "TOTAL_VOLUME_(p+l)"
    ]
    return [to_float(line[tag]) or 0.0 for tag in tags]

def normalize_row(row):
    """Normalize all decimal commas in a row before use."""
    return {k: (v.replace(",", ".") if isinstance(v, str) else v) for k, v in row.items()}

# ----------------------------
# Main Function
# ----------------------------

def read_events(filename, volcano_id):

    try:
        with open(filename, mode="r", encoding="utf-8") as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for line in csv_reader:
                # Normalize decimal formatting
                line = normalize_row(line)

                year_bce  = line["DATE_BCE"]
                year_gvp  = line["SMITHSONIAN_DATE"]
                month     = line["MONTH"]

                print(csv_reader.line_num, line)
                print("Date GVP:", year_gvp, "Date Biblio:", year_bce)

                # -----------------------------------------
                # Check if Smithsonian / Bibliographic date mismatch
                # -----------------------------------------
                if year_gvp and year_bce != year_gvp:
                    eruption_id = mdb.query_eruption_ym(volcano_id, year_bce, month)
                    print("Correcting event year →", year_bce)
                    mdb.update_year(eruption_id, year_bce, line["ERROR"])
                    print("Date updated")

                # -----------------------------------------
                # Check whether eruption event already exists
                # -----------------------------------------
                eruption = mdb.query_eruption_ym(
                    volcano_id,
                    to_int(year_bce),
                    to_int(month) or 0
                )

                # -----------------------------------------
                # INSERT new event
                # -----------------------------------------
                if eruption is None:

                    entry = {
                        "_id": volcano_id + line["DEF_YEAR"] + month,
                        "volcano": volcano_id,
                        "year": to_int(line["DEF_YEAR"]),
                        "error": to_int(line["ERROR"]),
                        "month": to_int(month),
                        "VEI": to_int(line["DEF_VEI"]),
                        "volume": parse_volume(line),
                        "mass": None,
                        "magnitude_mastin": to_float(line["MAGNITUDE"]),
                        "energy": None,
                        "biblio": [line[t] for t in ["BIBLIO1", "BIBLIO2", "BIBLIO3", "BIBLIO4"] if line[t]],
                        "column_height": to_int(line["COLUMN_HEIGHT"]),
                        "mer":to_float(line["MER"])
                    }

                    print("NEW EVENT ADDED")
                    print(entry)
                    mdb.add_eruption(entry)
                    continue

                # -----------------------------------------
                # UPDATE existing event
                # -----------------------------------------
                print("Existing eruption:", eruption)

                # volumes
                if any(line[t] for t in ["PYROCLAST_VOLUME_(KM3)", "LAVA_VOLUME_(KM3)", "TOTAL_VOLUME_(p+l)"]):
                    mdb.update_eruption(eruption, "volume", parse_volume(line))

                # VEI
                if line["VEI"]:
                    mdb.update_eruption(eruption, "VEI", to_int(line["VEI"]))

                # bibliography
                if line["BIBLIO1"]:
                    biblio = mdb.query_eruption_ym_biblio(eruption) or []
                    for t in ["BIBLIO1", "BIBLIO2", "BIBLIO3", "BIBLIO4"]:
                        if line[t] and line[t] not in biblio:
                            biblio.append(line[t])
                    mdb.update_eruption(eruption, "biblio", biblio)

                # magnitude, density, etc.
                numeric_fields = {
                    "density": "DENSITY",
                    "temperature": "TEMPERATURE"
                }
                for field, col in numeric_fields.items():
                    if line[col]:
                        mdb.update_eruption(eruption, field, to_float(line[col]))

                # column height
                if line["COLUMN_HEIGHT"]:
                    mdb.update_eruption(eruption, "column_height", to_int(line["COLUMN_HEIGHT"]))
                #MER
                if line["MER"]:
                    mdb.update_eruption(eruption, "mer", to_float(line["MER   "]))
    except Exception as e:
        print("ERROR:", e)
        print("Line:", e.__traceback__.tb_lineno)
        print("Frame:", e.__traceback__.tb_frame)

def read_rock_type(volcano):
    filename="resources/volcano_list.csv"
    try:
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=";")
            for row in csv_reader:
                if str(row["Volcano_Number"]) == volcano:
                    mdb.update_rock_type(row,volcano)
                    break
    except Exception as e:
            print(e)