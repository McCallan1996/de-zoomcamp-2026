import marimo

__generated_with = "0.20.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import altair as alt

    conn = duckdb.connect("taxi_pipeline.duckdb")
    return alt, conn, mo


@app.cell
def _(mo):
    mo.md("""
    # NYC Yellow Taxi — dlt Pipeline Analysis

    Data loaded from the [Data Engineering Zoomcamp API](https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api)
    via a dlt REST pipeline into DuckDB.
    """)
    return


@app.cell
def _(conn, mo):
    total_rows = conn.execute("SELECT COUNT(*) FROM nyc_taxi.taxi_rows").fetchone()[0]
    earliest, latest = conn.execute(
        "SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) FROM nyc_taxi.taxi_rows"
    ).fetchone()
    total_tips = conn.execute("SELECT ROUND(SUM(tip_amt), 2) FROM nyc_taxi.taxi_rows").fetchone()[0]
    credit_pct = conn.execute(
        "SELECT ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) FROM nyc_taxi.taxi_rows WHERE payment_type = 'Credit'"
    ).fetchone()[0]

    mo.hstack([
        mo.stat(label="Total Trips", value=f"{total_rows:,}"),
        mo.stat(label="Date Range", value=f"{earliest.date()} → {latest.date()}"),
        mo.stat(label="Credit Card %", value=f"{credit_pct}%"),
        mo.stat(label="Total Tips", value=f"${total_tips:,.2f}"),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ## Q1 — What is the dataset date range?
    """)
    return


@app.cell
def _(conn, mo):
    date_data = conn.execute("""
        SELECT
            STRFTIME(trip_pickup_date_time, '%Y-%m') AS month,
            COUNT(*) AS trips
        FROM nyc_taxi.taxi_rows
        GROUP BY month
        ORDER BY month
    """).fetchall()

    mo.ui.table(
        [{"Month": r[0], "Trips": r[1]} for r in date_data],
        label="Trips per month"
    )
    return


@app.cell
def _(alt, conn):
    import warnings
    rows = conn.execute("""
        SELECT
            STRFTIME(trip_pickup_date_time, '%Y-%m-%d') AS day,
            COUNT(*) AS trips
        FROM nyc_taxi.taxi_rows
        GROUP BY day
        ORDER BY day
    """).fetchall()

    data = [{"date": r[0], "trips": r[1]} for r in rows]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        chart = (
            alt.Chart(alt.Data(values=data))
            .mark_bar(color="#4C72B0")
            .encode(
                x=alt.X("date:O", title="Date", axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("trips:Q", title="Number of Trips"),
                tooltip=["date:O", "trips:Q"],
            )
            .properties(title="Daily Trip Volume (June–July 2009)", width=620, height=260)
        )
    chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## Q2 — What proportion of trips used Credit Card?
    """)
    return


@app.cell
def _(conn, mo):
    payment_rows = conn.execute("""
        SELECT
            payment_type,
            COUNT(*) AS trips,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
        FROM nyc_taxi.taxi_rows
        GROUP BY payment_type
        ORDER BY trips DESC
    """).fetchall()

    mo.ui.table(
        [{"Payment Type": r[0], "Trips": r[1], "Percentage": f"{r[2]}%"} for r in payment_rows],
        label="Payment type breakdown"
    )
    return (payment_rows,)


@app.cell
def _(alt, payment_rows):
    import warnings as _w

    pay_data = [{"type": r[0], "trips": r[1], "pct": float(r[2])} for r in payment_rows]

    with _w.catch_warnings():
        _w.simplefilter("ignore")
        pay_chart = (
            alt.Chart(alt.Data(values=pay_data))
            .mark_arc(innerRadius=60)
            .encode(
                theta=alt.Theta("trips:Q"),
                color=alt.Color("type:N", legend=alt.Legend(title="Payment Type")),
                tooltip=["type:N", "trips:Q", "pct:Q"],
            )
            .properties(title="Payment Type Distribution", width=380, height=280)
        )
    pay_chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## Q3 — What is the total tip amount?
    """)
    return


@app.cell
def _(conn, mo):
    tip_stats = conn.execute("""
        SELECT
            ROUND(SUM(tip_amt), 2)   AS total_tips,
            ROUND(AVG(tip_amt), 2)   AS avg_tip,
            ROUND(MAX(tip_amt), 2)   AS max_tip,
            COUNT(*) FILTER (WHERE tip_amt > 0) AS tippers
        FROM nyc_taxi.taxi_rows
    """).fetchone()

    mo.hstack([
        mo.stat(label="Total Tips", value=f"${tip_stats[0]:,.2f}"),
        mo.stat(label="Average Tip", value=f"${tip_stats[1]:,.2f}"),
        mo.stat(label="Max Single Tip", value=f"${tip_stats[2]:,.2f}"),
        mo.stat(label="Trips with Tip", value=f"{tip_stats[3]:,}"),
    ])
    return


@app.cell
def _(alt, conn):
    import warnings as _w2

    tip_rows = conn.execute("""
        SELECT
            CASE
                WHEN tip_amt = 0      THEN '0'
                WHEN tip_amt < 2      THEN '0–2'
                WHEN tip_amt < 5      THEN '2–5'
                WHEN tip_amt < 10     THEN '5–10'
                WHEN tip_amt < 20     THEN '10–20'
                ELSE '20+'
            END AS bucket,
            COUNT(*) AS trips,
            CASE
                WHEN tip_amt = 0      THEN 0
                WHEN tip_amt < 2      THEN 1
                WHEN tip_amt < 5      THEN 2
                WHEN tip_amt < 10     THEN 3
                WHEN tip_amt < 20     THEN 4
                ELSE 5
            END AS sort_order
        FROM nyc_taxi.taxi_rows
        GROUP BY bucket, sort_order
        ORDER BY sort_order
    """).fetchall()

    tip_data = [{"bucket": r[0], "trips": r[1]} for r in tip_rows]

    with _w2.catch_warnings():
        _w2.simplefilter("ignore")
        tip_chart = (
            alt.Chart(alt.Data(values=tip_data))
            .mark_bar(color="#2ca02c")
            .encode(
                x=alt.X("bucket:O", title="Tip Amount ($)", sort=None),
                y=alt.Y("trips:Q", title="Number of Trips"),
                tooltip=["bucket:O", "trips:Q"],
            )
            .properties(title="Tip Amount Distribution", width=480, height=260)
        )
    tip_chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## Homework Answers

    | # | Question | Answer |
    |---|----------|--------|
    | Q1 | Dataset date range | **June–July 2009** |
    | Q2 | Credit card payment proportion | **26.66%** |
    | Q3 | Total tips generated | **$6,063.41** |
    """)
    return


if __name__ == "__main__":
    app.run()
