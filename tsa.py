import datetime

from constants import GCP_PROJECT_ID
from mozillaScraper import fetchData
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX

SEASONAL_PERIOD = 24
FORECAST_HOURS = 168


def fit_data(timestamps, metric_val):
    if not isinstance(timestamps[0], pd.Timestamp):
        timestamps = pd.to_datetime(timestamps)

    ts_df = pd.DataFrame({'timestamp': timestamps, 'metric_val': metric_val})
    ts_df.set_index('timestamp', inplace=True)
    ts_df.sort_index(inplace=True)

    last_timestamp = ts_df.index.max()
    # TODO: timestamp wrong here - fix timestamps in train + test df
    test_start = last_timestamp - pd.Timedelta(days=7)

    train_df = ts_df[ts_df.index < test_start]
    test_df = ts_df[ts_df.index >= test_start]

    # TODO - choose order params based on ACF/PACF or grid search
    model = SARIMAX(
        train_df['metric_val'],
        order=(1, 1, 1),
        seasonal_order=(1, 1, 1, SEASONAL_PERIOD),  # 24 for diurnal.
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    results = model.fit(disp=False)

    forecast_steps = FORECAST_HOURS
    forecast = results.get_forecast(steps=forecast_steps)

    forecast_index = pd.date_range(
        start=train_df.index[-1] + pd.Timedelta(hours=1),
        periods=forecast_steps,
        freq='H'
    )

    forecast_series = pd.Series(forecast.predicted_mean.values, index=forecast_index)

    plt.figure(figsize=(14, 6))
    plt.plot(test_df.index, test_df['metric_val'], label='Actual', color='blue')
    plt.plot(forecast_series.index, forecast_series, label='Forecast', color='red')

    plt.title("Forecast vs Actual for most recent week (Hourly)")
    plt.xlabel("Timestamp")
    plt.ylabel("Metric Value")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     description='Continually fetches Mozilla telemetry data from the Google Bigquery and writes it into kafka')
    #
    # parser.add_argument("--broker", type=str, required=True, help="The kafka broker to connect to")
    # parser.add_argument("--channel", type=str, required=True, help="Kafka channel to write the data into")
    # parser.add_argument("--topicprefix", type=str, required=True, help="Topic prefix to prepend to each Kafka message")
    # parser.add_argument("--projectid", type=str, required=True, help="The Google Cloud project ID")
    # parser.add_argument("--starttime", type=int, help="Fetch traffic data starting from the given Unix timestamp. \
    #                                                                 If not provided, defaults to 2 days before endtime.")
    # parser.add_argument("--endtime", type=int, help="Fetch traffic data up until the given Unix timestamp. \
    #                                                               If not provided, defaults to the current time.")
    # args = parser.parse_args()
    #
    # main(args)
    saved = {}
    endtime = datetime.datetime.now()
    starttime = endtime - datetime.timedelta(weeks=3)
    fetchData(GCP_PROJECT_ID, starttime=starttime, endtime=endtime, region='US', saved=saved)

    print(saved)
    timestamps = saved.keys()
    proportion_timeout = []
    for keys, metric_array in saved.items():
        proportion_timeout.append(metric_array[0][1])
    fit_data(timestamps, proportion_timeout)
    pass
