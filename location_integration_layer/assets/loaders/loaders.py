from dagster import (
    asset,
    get_dagster_logger,
    Output,
    DailyPartitionsDefinition,
    FreshnessPolicy,
    AutoMaterializePolicy,
    AssetExecutionContext,
    MetadataValue
)
import datetime
import pandas as pd
from utils.partitions import DAILY_SEASON_PARTITIONS, INCLUDED_LEAGUES, FIXTURES_PARTITIONS, DAILY_FIXTURES_PARTITIONS


logger = get_dagster_logger()

@asset(
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=0, cron_schedule="0 6 * * *"),
    auto_materialize_policy=AutoMaterializePolicy.lazy(),
    io_manager_key="duckdb_io_manager",
    compute_kind="duckdb"
)
def raw_leagues(context:AssetExecutionContext, extract_leagues: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted leagues and defines the DataFrame schema for that entity.

    Columns are defined explicitly and are assigned a matching value for each league.
    """
    list_records = []
    for league in extract_leagues["response"]:
        dict_records = {}
        dict_records.update(
            {
                "leagueid": league["league"]["id"],
                "name": league["league"]["name"],
                "type": league["league"]["type"],
                "logomedia": league["league"]["logo"],
                "country": league["country"]["name"],
                "countrycode": league["country"]["code"],
                "countryflagmedia": league["country"]["flag"],
                "RunId": context.run_id,
            }
        )

        list_records.append(dict_records.copy())
    # Create a dataframe
    leagues_df = pd.DataFrame(list_records)
    return leagues_df


@asset(
    partitions_def=DAILY_SEASON_PARTITIONS,
    metadata={"partition_expr": "DatePartition"},
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=0, cron_schedule="0 6 * * *"),
    auto_materialize_policy=AutoMaterializePolicy.lazy(),
    io_manager_key="duckdb_io_manager",
    compute_kind="duckdb"
)
def raw_fixtures(context: AssetExecutionContext, extract_fixtures: dict) -> Output:
    """
    ## `raw_fixtures` function

    This function is a Dagster asset that processes raw fixture data.

    ### Parameters:
    - `context` (`AssetExecutionContext`): The execution context of the asset.
    - `extract_fixtures` (`dict`): The raw fixture data to be processed.

    ### Returns:
    - `pd.DataFrame`: A DataFrame containing the processed fixture data.

    ### Asset Decorator:
    The `@asset` decorator specifies the following properties for this asset:
    - `partitions_def`: The partitions definition, set to `DAILY_SEASON_PARTITIONS`.
    - `metadata`: Metadata for the asset, including a `partition_expr` set to `DatePartition`.
    - `freshness_policy`: The freshness policy for the asset, set to check every day at 6 AM with no maximum lag minutes.
    - `auto_materialize_policy`: The auto materialize policy, set to `lazy`.
    - `io_manager_key`: The IO manager key, set to `duckdb_io_manager`.
    - `compute_kind`: The compute kind, set to `duckdb`.

    ### Behavior:
    The function iterates over the `extract_fixtures` dictionary and processes each fixture. The processed fixtures are then stored in a DataFrame, which is returned.

    ### Example:
    ```python
    context = ...  # An instance of AssetExecutionContext
    extract_fixtures = ...  # A dictionary containing raw fixture data
    df = raw_fixtures(context, extract_fixtures)
    print(df)  # Prints the DataFrame
    ```
    """
    list_records = []
    for fixture in extract_fixtures["response"]:
        dict_records = {}
        dict_records.update({
            "FixtureId": fixture["fixture"]["id"],
            "Referee": fixture["fixture"]["referee"],
            "DateTimeUTC": fixture["fixture"]["date"],
            "VenueId": fixture["fixture"]["venue"]["id"],
            "VenueName": fixture["fixture"]["venue"]["name"],
            "VenueCity": fixture["fixture"]["venue"]["city"],
            "LeagueId": fixture["league"]["id"],
            "LeagueName": fixture["league"]["name"],
            "LeagueCountry": fixture["league"]["country"],
            "LeagueLogoMedia": fixture["league"]["logo"],
            "LeagueFlagMedia": fixture["league"]["flag"],
            "Season": fixture["league"]["season"],
            "Round": fixture["league"]["round"],
            "HomeTeamId": fixture["teams"]["home"]["id"],
            "HomeTeamName": fixture["teams"]["home"]["name"],
            "HomeTeamLogo": fixture["teams"]["home"]["logo"],
            "HomeTeamGoals": fixture["goals"]["home"],
            "AwayTeamId": fixture["teams"]["away"]["id"],
            "AwayTeamName": fixture["teams"]["away"]["name"],
            "AwayTeamLogo": fixture["teams"]["away"]["logo"],
            "AwayTeamGoals": fixture["goals"]["away"],
            "MatchStatus": fixture["fixture"]["status"]["short"],
            "Elapsed": fixture["fixture"]["status"]["elapsed"],
            "DatePartition": context.asset_partition_key_for_output(),
            "RunId": context.run_id,
        }
        )
        list_records.append(dict_records.copy())
    # Create a dataframe
    fixtures_df = pd.DataFrame(list_records)
    return Output(fixtures_df, metadata={"Sample": MetadataValue.md(fixtures_df.head().to_markdown())})

@asset(
    io_manager_key="duckdb_io_manager",
    compute_kind='duckdb',
    auto_materialize_policy=AutoMaterializePolicy.lazy()
)
def raw_seasons(extract_leagues: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted leagues and processes the corresponding seasons that are available for each league in the league endpoint.

    Columns are defined explicitly and are assigned a matching value for each league-season pair.
    """
    list_records = []
    for each in extract_leagues["response"]:
        dict_records = {}
        league_id = each["league"]["id"]

        # Loop through seasons
        for each_season in each["seasons"]:

            dict_records.update(
                {
                    "leagueid": league_id,
                    "seasonid": each_season["year"],
                    "year": each_season["year"],
                    "startdate": each_season["start"],
                    "enddate": each_season["end"],
                    "current": each_season["current"],
                    "fixtureevents": each_season["coverage"]["fixtures"]["events"],
                    "fixturelineups": each_season["coverage"]["fixtures"]["lineups"],
                    "fixturestatistics": each_season["coverage"]["fixtures"]["statistics_fixtures"],
                    "fixtureplayerstatistics": each_season["coverage"]["fixtures"]["statistics_players"],
                    "standings": each_season["coverage"]["standings"],
                    "players": each_season["coverage"]["players"],
                    "topscorers": each_season["coverage"]["top_scorers"],
                    "topassists": each_season["coverage"]["top_assists"],
                    "topcards": each_season["coverage"]["top_cards"],
                    "injuries": each_season["coverage"]["injuries"],
                    "predictions": each_season["coverage"]["predictions"],
                    "odds": each_season["coverage"]["odds"]
                }
            )
            list_records.append(dict_records.copy())
    # Create a dataframe from the list of dicts
    seasons_df = pd.DataFrame(list_records)
    return seasons_df

@asset(
    io_manager_key="duckdb_io_manager",
    compute_kind='duckdb'
)
def raw_fixture_events(extract_fixture_events: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted fixture events and transforms it into a pandas DataFrame.

    Columns are defined explicitly and are assigned a matching value for each fixture.
    """
    list_records = []
    fixture_id = extract_fixture_events["parameters"]["fixture"]
    for each in extract_fixture_events["response"]:
        dict_records = {}
        dict_records.update(
            {
                "fixtureid": fixture_id,
                "teamid": each["team"]["id"],
                "type": each["type"],
                "detail": each["detail"],
                "comments": each["comments"],
                "minute": each["time"]["elapsed"],
                "timeextra": each["time"]["extra"],
                "mainplayerid": each["player"]["id"],
                "secondaryplayerid": each["assist"]["id"]
            }
        )

        list_records.append(dict_records.copy())
    # Create a dataframe
    fixture_events_df = pd.DataFrame(list_records)
    return fixture_events_df

@asset(
    io_manager_key="duckdb_io_manager",
    compute_kind='duckdb'
)
def raw_fixture_stats(extract_fixture_stats: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted fixture stats and transforms it into a pandas DataFrame.

    Columns are defined explicitly and are assigned a matching value for each fixture.
    """
    list_records = []
    fixture_id = extract_fixture_stats["parameters"]["fixture"]
    for each in extract_fixture_stats["response"]:
        dict_records = {}
        dict_records.update(
            {
                "FixtureId": fixture_id,
                "TeamId": each["team"]["id"]
            }
        )

        # Loop through to get all of the metrics. Dynamically created
        for each_stat in each["statistics"]:
            dict_records.update({each_stat["type"]: each_stat["value"]})
        list_records.append(dict_records.copy())
    # Create a dataframe
    fixture_stats_df = pd.DataFrame(list_records)
    return fixture_stats_df

@asset(
    io_manager_key='duckdb_io_manager',
    compute_kind='duckdb',
    auto_materialize_policy=AutoMaterializePolicy.lazy()
)
def raw_fixture_player_stats(extract_fixture_player_stats: dict) -> pd.DataFrame:
    """
    ### Converts dict to DataFrame
    Takes in the extracted fixture player stats and transforms it into a pandas DataFrame.

    Columns are defined explicitly and are assigned a matching value for each fixture.
    """
    list_records = []
    fixture_id = extract_fixture_player_stats["parameters"]["fixture"]
    for each in extract_fixture_player_stats["response"]:
        dict_records = {}
        dict_records.update({"FixtureId": fixture_id})
        dict_records.update({"TeamId": each["team"]["id"]})
        for each_player in each["players"]:
            dict_records.update({"PlayerId": each_player["player"]["id"]})
            dict_records.update(
                {"PlayerName": each_player["player"]["name"]})
            dict_records.update(
                {"PlayerPhotoMedia": each_player["player"]["photo"]}
            )
            for player_stats in each_player["statistics"]:
                dict_records.update({"Offsides": player_stats["offsides"]})
                stats_list = list(player_stats.keys())
                stats_list.remove("offsides")
                for each_stat in stats_list:
                    key_list = list(player_stats[each_stat].keys())
                    for each_key in key_list:
                        dict_records.update(
                            {
                                f"{each_stat} {each_key}": player_stats[each_stat][
                                    each_key
                                ]
                            }
                        )

            list_records.append(dict_records.copy())
    # Create a dataframe
    fixture_player_stats_df = pd.DataFrame(list_records)
    return fixture_player_stats_df