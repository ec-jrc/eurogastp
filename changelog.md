![Documentation](https://img.shields.io/badge/python-3.8-blue.svg) [![License](https://img.shields.io/badge/License-EUPL--1.2-blue.svg)](https://opensource.org/licenses/EUPL-1.2)

# eurogastp - Change Log


## v0.1.2 (2023-04-14)

- fix topology at Velke Kapusany (UA-SK)
- add missing node Northern Ireland (NI) to list of non-EU nodes
- added beta version of topology file (v4) to the Repo (attempt to corrected NODE flow calculation)
- replace frame.append (deprecated) by pandas.concat
- updated Example Notebook 1 to use topology v3


## v0.1.1 (2022-11-18)

- update topology (new NL consumption points since 1/7/2022)
- add edge Tarifa (ES-MA) to topology
- changed ENTSOG TP download behavior (download only network point data for which at least one indicator in topology is different from zero)
- fix unit of gasInStorage in download_gie_agsi()


## v0.1.0 (2022-10-28)

- First version tag
- switched to new topology v3
- updated topology around Dornum (NO-DE)
- changed EIC file to CSV format
- added some minor functionality


## 2022-09-13

- Initial commit
