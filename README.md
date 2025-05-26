Includes rewrite of [feature_pipeline_flightAPI_historical_processor.py](./src/feature_pipeline/feature_pipeline_historical/feature_pipeline_historical_flight/feature_pipeline_flightAPI_historical_processor.py) using our feature store library to evaluate its performance.

Running and checking that the result of the rewrite is the same:
```python
python -m src.feature_pipeline.feature_pipeline_historical.feature_pipeline_historical_flight.feature_pipeline_flightAPI_historical_processor

diff datasets/flight_historical_data/output.csv datasets/flight_historical_data/original_output.csv
```
