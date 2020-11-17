# brasilio_covid19_data
Source code used to get Paraná (PR) and Rio de Janeiro (RJ) state's epidemiological report data daily.

## How to use it? (update for RJ, PR in progress)

Just run on your terminal:

```
$ python3 main.py --DD [DD] --MM [MM] RJ
```

> Check usage of flags (in pt-br) with: `python3 main.py --help`

It will generate a file on `ouputs/[uf]/[uf]_[mm]_[dd]`

#### How to run for PR

```
$ python3 main.py DD_MM PR
```