from dagster import asset, AssetIn, SourceAsset, AssetKey


source_asset = SourceAsset(key=AssetKey("some_asset"))

@asset()
def cool_asset(some_asset):
    upstream = some_asset
    return "Hello Mom"