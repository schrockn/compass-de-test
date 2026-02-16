import dagster as dg


@dg.asset
def hello_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
