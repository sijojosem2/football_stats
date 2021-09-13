import os

API_KEY=os.environ.get("FOOTBALL_API_KEY")

""" The dataset metadata should be declared in the list
    "dataset_name"  : << used to designate the dataset and name the csv >>
    "request"       : << url and headers to be provided here, any additional parameters, body also should be given >>
    "write_to_csv"  : << Flag enables or disables csv creation from the pandas dataframe>> 
    "pd_dataframe"  : <<pandas dataframe parameters, for JSON responses, provide the column that needs to be extracted in 'record_path' 
                        for non JSON responses provide either the column delimiter (variable) or colspecs(fixed length) >>
    "target_table"  : <<target table parameters given here>>
    
    the api key is exported in the env """




input = {
    "desctiption": "data source list",
    "etl": [
        {
            "dataset_name": "premier_league_season",
            "request": {"url": "https://api.football-data.org/v2/competitions/PL/",
                        "headers": {"X-Auth-Token": API_KEY}, "params": {}},
            "write_to_csv": True,
            "pd_dataframe": {"norm": {"record_path": ["seasons"],"sep" : "_"},"drop_cols": ["winner_crestUrl"]},
            "target_table": {"name": "pl_seasons", "if_exists": "append", "index": False}
        },
        {
            "dataset_name": "premier_league_teams",
            "request": {"url": "https://api.football-data.org/v2/teams",
                        "headers": {"X-Auth-Token": API_KEY}, "params": {}},
            "write_to_csv": True,
            "pd_dataframe": {"norm": {"record_path": ["teams"],"sep" : "_"}, "drop_cols": []},
            "target_table": {"name": "pl_teams", "if_exists": "append", "index": False}
        },
    ]
}
