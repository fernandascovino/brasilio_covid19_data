# brasilio_covid19_data 🦠
Source code used to get Paraná (PR) and Rio de Janeiro (RJ) state's epidemiological report data by day for Brasil.IO.

Checkout the data on: https://brasil.io/dataset/covid19/caso_full/

## Run local to get data

```bash
python3 main.py --DD [DD] --MM [MM] --AA [AAAA] [UF]
```

> Check usage of flags (in pt-br) with: `python3 main.py --help`

🗂 It will save a file with the data on `ouputs/[uf]/[uf]_[aaaa]_[mm]_[dd].xlsx`
