#!/usr/bin/env python3
import math

import plotly.graph_objects as go
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', "--input", type=str, help="Directory to read extracted documents from",
                        default="top_frequent_words.txt")

    args = parser.parse_args()
    fig = go.Figure()
    x = []
    y = []
    rank = 0
    with open(args.input, 'r') as f:
        for line in f.readlines():
            rank += 1
            freq = int(line.split()[1])
            x.append(rank)
            y.append(math.log(freq))
    fig.add_trace(go.Scatter(
        x=x, y=y,
        name='log frequency',
    ))
    fig.update_layout(
        title=go.layout.Title(
            text="Частота слов",
            xref="paper",
            x=0
        ),
        xaxis=go.layout.XAxis(
            title=go.layout.xaxis.Title(
                text="Ранк слова",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
        ),
        yaxis=go.layout.YAxis(
            title=go.layout.yaxis.Title(
                text="log(Частота слова)",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
        )
    )
    fig.show()

if __name__ == "__main__":
    main()
