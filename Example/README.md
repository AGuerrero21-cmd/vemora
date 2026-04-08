# Smithsonian GVP Eruption Data

This project contains eruption data from the Smithsonian Global Volcanism Program (GVP), formatted for structured storage and analysis. The data includes eruption years, volumes of materials ejected, eruption heights, and bibliographic references.

## Table Overview

### Columns
The table contains the following columns:

| Column Name                   | Description                                                                 |
|-------------------------------|-----------------------------------------------------------------------------|
| BP_DATE                       | Years before present (0 = Common Era).                                      |
| RANGE                         | Year uncertainty range (0 = no range).                                      |
| YEAR                          | Eruption year (CE).                                                         |
| MONTH                         | Month of the eruption.                                                      |
| ERROR                         | Year uncertainty (0 = no error).                                            |
| DATE_BCE                      | Eruption year in BCE (blank for CE dates).                                  |
| SMITHSONIAN_DATE              | Smithsonian-calculated eruption date (if provided).                         |
| DEF_YEAR                      | Defined year of eruption (matches YEAR).                                    |
| VEI                           | Volcanic Explosivity Index (VEI), measuring eruption magnitude.             |
| SMITH_VEI                     | Smithsonian-calculated VEI.                                                 |
| DEF_VEI                       | Defined VEI based on external sources.                                      |
| MAGNITUDE                     | Eruption magnitude (if available).                                          |
| PYROCLAST_VOLUME_(KM3)        | Volume of pyroclastic material ejected, in cubic kilometers.                |
| LAVA_VOLUME_(KM3)             | Volume of lava erupted, in cubic kilometers.                                |
| TOTAL_VOLUME_(p+l)            | Total volume (pyroclastic + lava), in cubic kilometers.                     |
| MASS_FROM_MAGNITUDE_(KG)      | Erupted material mass, in kilograms, derived from eruption magnitude.       |
| COLUMN_HEIGHT                 | Height of the eruption column, in meters.                                   |
| DENSITY                       | Density of erupted material (kg/m3).                                        |
| TEMPERATURE                   | Temperature of the eruption products (C).                                   |
| C                             | Specific heat  J·kg−1·K−1                                                   |
| BIBLIO1 to BILIO4             | Bibliographic reference(s) for the eruption.                                |
