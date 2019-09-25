#!/usr/bin/env python3
import math
import json

import plotly.graph_objects as go
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', "--input", type=str, help="Directory to read extracted documents from",
                        default="../results/metrics")
    parser.add_argument('-m', "--metric", type=str, help="Metric to build graphic for",
                        default="rprecision")

    args = parser.parse_args()

    metric = args.metric

    fig = go.Figure()
    x = [i / 10.0 for i in range(11)]
    y = [0 for _ in range(11)]
    sum = 0.0
    cnt = 0.0
    with open(args.input, 'r') as f:
        statsj = json.load(f)["metrics"]
        for stats in statsj.values():
            cnt += 1
            sum += stats[metric]
            y[int(stats[metric] * 10)] += 1

    fig.add_trace(go.Scatter(
        x=x, y=y,
        name='log frequency',
    ))
    fig.update_layout(
        title=go.layout.Title(
            text="Качество ранжирования по метрике {}, (среднее: {})".format(metric, sum / cnt),
            xref="paper",
            x=0
        ),
        xaxis=go.layout.XAxis(
            title=go.layout.xaxis.Title(
                text="Количество запросов",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
        ),
        yaxis=go.layout.YAxis(
            title=go.layout.yaxis.Title(
                text=metric,
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
        )
    )
    print("Average {}: {}".format(metric, sum / cnt))
    fig.show()


if __name__ == "__main__":
    main()
